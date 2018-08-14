
#include <random>

#include "allscale/runtime/com/network.h"
#include "allscale/runtime/com/hierarchy.h"

#include "allscale/runtime/log/logger.h"
#include "allscale/runtime/work/scheduler.h"
#include "allscale/runtime/work/task.h"
#include "allscale/runtime/work/worker.h"

#include "allscale/runtime/data/data_item_manager.h"
#include "allscale/runtime/data/data_item_region.h"
#include "allscale/runtime/data/data_item_index.h"

#include "allscale/runtime/work/schedule_policy.h"

namespace allscale {
namespace runtime {
namespace work {

	namespace {

		/**
		 * Computes the ceiling of log2(x).
		 */
		int ceilLog2(int x) {
			// TODO: move this to utility header
			// TODO: look for built-in operator
			int r = 0;
			int s = 1;
			while(s<x) {
				s<<=1;
				r++;
			}
			return r;
		}

		/**
		 * Determines the cut-off level for task splitting
		 */
		int getCutOffLevel() {
			// get the network
			auto& net = com::Network::getNetwork();
			auto numNodes = net.numNodes();

			// the cut-off level for "forced" distribution
			return ceilLog2(numNodes) + 3;
		}

	}

	/**
	 * Determines whether the given task should be split.
	 */
	bool shouldSplit(const TaskPtr& t) {

		// should only be called for tasks that are actually splitable
		assert_true(t->isSplitable());

		// decide on the cut-off level
		return t->getId().getDepth() < getCutOffLevel();
	}

	// a simple scheduler for the development phase -- just statically distributing tasks without considering data dependencies
	namespace simple {

		/**
		 * Requests the target location for the given task.
		 *
		 * @param localRank the local rank this function is processed on
		 * @param task the task to be scheduled
		 */
		com::rank_t getScheduleTarget(com::rank_t localRank, const TaskPtr& t) {

			// -- An initial, simple scheduling function --

			// should only be contacted for portable tasks
			assert_true(t->canBeDistributed());

			// for very fine-grained tasks => process locally
			if (t->getId().getDepth() < getCutOffLevel()) return localRank;

			// for all others, determine a rank by distributing task equally among nodes

			// get the network
			auto& net = com::Network::getNetwork();
			auto numNodes = net.numNodes();

			int pow2 = getCutOffLevel();
			int max = 1 << pow2;

			auto path = t->getId().getPath();

			// inverse the path
			int inv = 0;
			auto it = path.begin();
			for(int i=0; i<pow2; ++it,i++) {
				inv <<= 1;
				if (*it) inv = inv | 1;
			}

			// this should give me a value between 0 and max
			assert_le(0,inv);
			assert_lt(inv,max);

			// assign to target node
			return com::rank_t(numNodes * double(inv)/double(max));
		}


		// a simple scheduler statically assigning tasks to nodes
		void schedule(TaskPtr&& task) {

			// ask scheduler where to schedule task
			auto localRank = com::Node::getLocalRank();
			auto targetRank = (task->canBeDistributed()) ? getScheduleTarget(localRank,task) : localRank;

			// test whether this is local
			if (localRank == targetRank) {
				// => run local
				Worker::getLocalWorker().schedule(std::move(task));
			} else {
				// => send to remote location
				// TODO: realize this as a remote method call
				com::Network::getNetwork().runOn(targetRank,[&](com::Node& node){
					node.getService<Worker>().schedule(std::move(task));
				});
			}
		}

	} // end namespace simple


	namespace data_aware {

		/**
		 * A service running on each virtual node.
		 */
		class ScheduleService {

			// the network being a part of
			com::HierarchicalOverlayNetwork network;

			// the address of this scheduler service
			com::HierarchyAddress myAddr;

			// the address of the root node
			com::HierarchyAddress rootAddr;

			// a flag indicating whether this node is the root node
			bool isRoot;

			// the depth of this node in the tree, counted from the root
			std::size_t depth;

			// the scheduling policy to follow
			SchedulingPolicy policy;

		public:

			ScheduleService(com::Network& net, const com::HierarchyAddress& addr)
				: network(net),
				  myAddr(addr),
				  rootAddr(network.getRootAddress()),
				  isRoot(myAddr == rootAddr),
				  depth(rootAddr.getLayer()-myAddr.getLayer()),
				  policy(SchedulingPolicy::createUniform(net.numNodes())) {}

			/**
			 * The following two methods are the central element of the scheduling process.
			 *
			 * They attempt to achieve two goals: get tasks where the data is, and spread tasks evenly where possible.
			 *
			 */


			// requests this scheduler instance to schedule this task.
			bool schedule(TaskReference task) {

				// Phase 1: locate virtual node allowed to perform the scheduling
				DLOG << "Start Scheduling " << task->getId() << " on " << myAddr << " ... \n";
				assert_true(task->isReady());

				// check whether current node is allowed to make autonomous scheduling decisions
				auto& diis = network.getLocalService<data::DataItemIndexService>(myAddr.getLayer());
				auto path = task->getId().getPath();
				if (!isRoot
						// test that this virtual node is allowed to interfear with the scheduling of this task
						&& policy.isInvolved(myAddr,path)
						// test that this virtual node has control over all required data
						&& diis.covers(task->getProcessRequirements().getWriteRequirements())
					) {
					// we can schedule it right here!
					DLOG << "Short-cutting " << task->getId() << " on " << myAddr << "\n";
					return scheduleDown(std::move(task),{});
				}

				// propagate to parent
				if (!isRoot) {
					// forward call to parent node
					return network.getRemoteProcedure(myAddr.getParent(),&ScheduleService::schedule)(std::move(task));
				}

				// Phase 2: propagate task down the hierarchy

				// compute unallocated data item regions
				auto missing = diis.getMissingRegions(task->getProcessRequirements().getWriteRequirements());

				// pass those along with the scheduling process
				return scheduleDown(std::move(task),missing);

			}

			bool scheduleDown(TaskReference task, const data::DataItemRegions& allowance) {

				// make sure this is processed on the right node
				assert_eq(myAddr.getRank(),com::Node::getLocalRank());
				assert_true(policy.isInvolved(myAddr,task->getId().getPath()));

				// obtain access to the co-located data item index service
				auto& diis = network.getLocalService<data::DataItemIndexService>(myAddr.getLayer());

				// on leaf level, schedule locally
				if (myAddr.isLeaf()) {
					// add granted allowances
					diis.addAllowanceLocal(allowance);

					// now all requirements should be satisfied now
					assert_pred1(diis.covers,task->getProcessRequirements().getWriteRequirements())
						<< "Allowance: " << allowance << "\n"
						<< "Available: " << diis.getAvailableData() << "\n"
						<< "Required:  " << task->getProcessRequirements();

					// run locally
					return scheduleLocal(std::move(task));
				}

				// schedule locally if decided to do so
				auto id = task->getId();

				// TODO: check whether left or right node covers all write requirements

				// ask the scheduling policy what to do with this task
				auto d = policy.decide(myAddr,id.getPath());
				assert_ne(d,Decision::Done);

				// if it should stay, process it here
				if (d == Decision::Stay) {

					// add granted allowances
					diis.addAllowanceLocal(allowance);

					// now all requirements should be satisfied now
					assert_pred1(diis.covers,task->getProcessRequirements().getWriteRequirements())
						<< "Allowance: " << allowance << "\n"
						<< "Available: " << diis.getAvailableData() << "\n"
						<< "Required:  " << task->getProcessRequirements();

					// run locally
					return scheduleLocal(std::move(task));
				}

				bool targetLeft = (d == Decision::Left);

				// get address of next virtual node to be involved
				com::HierarchyAddress next = (targetLeft)
						? myAddr.getLeftChild()
						: myAddr.getRightChild();

				// reconsider if right is to far right
				if (next.getRank() >= network.numNodes()) {
					next = myAddr.getLeftChild();
				}


				// compute regions
				auto reqs = task->getProcessRequirements();
				auto subAllowances = (targetLeft)
						? diis.addAllowanceLeft(allowance,reqs.getWriteRequirements())
						: diis.addAllowanceRight(allowance,reqs.getWriteRequirements());


				// record handout of missing region
				DLOG << "Dispatching " << id << " on " << myAddr << " to " << next << " with " << subAllowances << " ... \n";


				// forward task
				return network.getRemoteProcedure(next,&ScheduleService::scheduleDown)(std::move(task),subAllowances);
			}


			// process the task
			bool scheduleLocal(TaskPtr&& task) {

				// make sure this is as it has been intended by the policy
				assert_eq(myAddr,policy.getTarget(task->getId().getPath()))
					<< "Task: " << task->getId() << "\n"
					<< "Policy:\n" << policy;

				DLOG << "Scheduling " << task->getId() << " on Node " << com::Node::getLocalRank() << " ... \n";

				// assign to local worker
				com::Node::getLocalService<Worker>().schedule(std::move(task));

				// success
				return true;
			}

		};


		// schedules a task based on its write-set requirements, spreads evenly in case of multiple options
		void schedule(TaskPtr&& task) {

			auto& service = com::HierarchicalOverlayNetwork::getLocalService<ScheduleService>();

			// special case: non-distributable task
			if (!task->canBeDistributed()) {
				service.scheduleLocal(std::move(task));
				return;
			}

			// forward to task scheduling service
			service.schedule(std::move(task));

		}

	} // end namespace data_aware


	namespace random {

		/**
		 * A service running on each virtual node.
		 */
		class ScheduleService {

			// the network being a part of
			com::HierarchicalOverlayNetwork network;

			// the address of this scheduler service
			com::HierarchyAddress myAddr;

			// the address of the root node
			com::HierarchyAddress rootAddr;

			// a flag indicating whether this node is the root node
			bool isRoot;

			// the depth of this node in the tree, counted from the root
			std::size_t depth;

			// a random device - because only random is truly fair
			std::uniform_real_distribution<> policy;

			// the source for random values
			std::random_device generator;

		public:

			ScheduleService(com::Network& net, const com::HierarchyAddress& addr)
				: network(net),
				  myAddr(addr),
				  rootAddr(network.getRootAddress()),
				  isRoot(myAddr == rootAddr),
				  depth(rootAddr.getLayer()-myAddr.getLayer()) {}

			/**
			 * The following two methods are the central element of the scheduling process.
			 *
			 * They attempt to achieve two goals: manage allowances and distribute tasks randomly
			 *
			 */

			// requests this scheduler instance to schedule this task.
			bool schedule(TaskReference task) {

				// Phase 1: locate virtual node allowed to perform the scheduling
				DLOG << "Start Scheduling " << task->getId() << " on " << myAddr << " ... \n";
				assert_true(task->isReady());

				// check whether current node is allowed to make autonomous scheduling decisions
				auto& diis = network.getLocalService<data::DataItemIndexService>(myAddr.getLayer());
				auto path = task->getId().getPath();
				if (!isRoot
						// decide randomly whether this one is allowed to schedule it
						&& path.getLength() > depth && policy(generator) < 0.2			// 20% probability for fine enough tasks
						// test that this virtual node has control over all required data
						&& diis.covers(task->getProcessRequirements().getWriteRequirements())
					) {
					// we can schedule it right here!
					DLOG << "Short-cutting " << task->getId() << " on " << myAddr << "\n";
					return scheduleDown(std::move(task),{});
				}

				// if there are unallocated allowances still to process, do so
				auto unallocated = diis.getManagedUnallocatedRegion(task->getProcessRequirements().getWriteRequirements());
				if (!unallocated.empty()) {
					// take unallocated share and pass along scheduling process
					return scheduleDown(std::move(task),unallocated);
				}

				// propagate to parent
				if (!isRoot) {
					// forward call to parent node
					return network.getRemoteProcedure(myAddr.getParent(),&ScheduleService::schedule)(std::move(task));
				}

				// Phase 2: propagate task down the hierarchy
				assert_true(isRoot);

				// compute unallocated data item regions
				auto missing = diis.getMissingRegions(task->getProcessRequirements().getWriteRequirements());

				// pass those along with the scheduling process
				return scheduleDown(std::move(task),missing);

			}

			bool scheduleDown(TaskReference task, const data::DataItemRegions& allowance) {

				// make sure this is processed on the right node
				assert_eq(myAddr.getRank(),com::Node::getLocalRank());

				// obtain access to the co-located data item index service
				auto& diis = network.getLocalService<data::DataItemIndexService>(myAddr.getLayer());

				// on leaf level, schedule locally
				if (myAddr.isLeaf()) {
					diis.addAllowanceLocal(allowance);
					return scheduleLocal(std::move(task));
				}

				// schedule locally if decided to do so
				auto id = task->getId();

				// TODO: check whether left or right node covers all write requirements

				// ask the scheduling policy what to do with this task
				auto r = policy(generator);
				auto d = (r < 0.33) ? Decision::Left  :
						 (r < 0.66) ? Decision::Right :
								      (id.getDepth() < getCutOffLevel())
									  	  ? Decision::Stay
										  : (( r < 0.83 ) ? Decision::Left : Decision::Right) ;

				// if it should stay, process it here
				if (d == Decision::Stay) {
					assert_lt(id.getDepth(),getCutOffLevel());
					diis.addAllowanceLocal(allowance);
					return scheduleLocal(std::move(task));
				}

				bool targetLeft = (d == Decision::Left);

				// get address of next virtual node to be involved
				com::HierarchyAddress next = (targetLeft)
						? myAddr.getLeftChild()
						: myAddr.getRightChild();

				// reconsider if right is to far right
				if (next.getRank() >= network.numNodes()) {
					next = myAddr.getLeftChild();
				}

				// compute regions
				auto reqs = task->getProcessRequirements().getWriteRequirements();
				auto subAllowances = (targetLeft)
						? diis.addAllowanceLeft(allowance,reqs)
						: diis.addAllowanceRight(allowance,reqs);


				// record handout of missing region
				DLOG << "Dispatching " << id << " on " << myAddr << " to " << next << " with " << subAllowances << " ... \n";

				// forward task
				return network.getRemoteProcedure(next,&ScheduleService::scheduleDown)(std::move(task),subAllowances);
			}


			// process the task
			bool scheduleLocal(TaskPtr&& task) {

				DLOG << "Scheduling " << task->getId() << " on Node " << com::Node::getLocalRank() << " ... \n";

				// assign to local worker
				com::Node::getLocalService<Worker>().schedule(std::move(task));

				// success
				return true;
			}

		};


		// schedules a task based on its write-set requirements, spreads evenly in case of multiple options
		void schedule(TaskPtr&& task) {

			auto& service = com::HierarchicalOverlayNetwork::getLocalService<ScheduleService>();

			// special case: non-distributable task
			if (!task->canBeDistributed()) {
				service.scheduleLocal(std::move(task));
				return;
			}

			// forward to task scheduling service
			service.schedule(std::move(task));

		}

	} // end namespace random


	// the main entry point for scheduling
	void schedule(TaskPtr&& task) {
		//simple::schedule(std::move(task));
		data_aware::schedule(std::move(task));
//		random::schedule(std::move(task));
	}

	void installSchedulerService(com::Network& network) {
		com::HierarchicalOverlayNetwork hierarchy(network);
		hierarchy.installServiceOnNodes<data_aware::ScheduleService>();
		hierarchy.installServiceOnNodes<random::ScheduleService>();
		hierarchy.installServiceOnNodes<data::DataItemIndexService>();
	}


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
