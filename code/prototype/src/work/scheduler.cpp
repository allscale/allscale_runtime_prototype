
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
		 * Determines the cut-off level for task splitting for a network of the given size.
		 */
		int getCutOffLevel(int numNodes) {
			// the cut-off level for "forced" distribution
			return ceilLog2(numNodes) + 3;
		}

		/**
		 * Determines the cut-off level for task splitting
		 */
		int getCutOffLevel() {
			// get the network
			auto& net = com::Network::getNetwork();
			// use its size
			return getCutOffLevel(net.numNodes());
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


	namespace detail {

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

			// the active scheduling policy
			ExchangeableSchedulingPolicy policy;

		public:

			ScheduleService(com::Network& net, const com::HierarchyAddress& addr)
				: ScheduleService(net,addr,DecisionTreeSchedulingPolicy::createUniform(net.numNodes())) {}

			ScheduleService(com::Network& net, const com::HierarchyAddress& addr, const SchedulingPolicy& policy)
				: network(net),
				  myAddr(addr),
				  rootAddr(network.getRootAddress()),
				  isRoot(myAddr == rootAddr),
				  depth(rootAddr.getLayer()-myAddr.getLayer()),
				  policy(policy) {}

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
						&& policy.isInvolved(myAddr,path)
						// test that this virtual node has control over all required data
						&& diis.covers(task->getProcessRequirements().getWriteRequirements())
					) {
					// we can schedule it right here!
					DLOG << "Short-cutting " << task->getId() << " on " << myAddr << "\n";
					return scheduleDown(std::move(task),{});
				}

				// TODO: replace this with a schedule-up phase forwarding allowances

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
				auto d = policy.decide(myAddr,id.getPath());
				assert_ne(d,Decision::Done);

				// if it should stay, process it here
				if (task->isSplitable() && d == Decision::Stay) {	// non-splitable task must not stay on inner level
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
					targetLeft = true;
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

				// make sure this is as it has been intended by the policy
				if (task->isSplitable()) {
					assert_true(policy.checkTarget(myAddr,task->getId().getPath()))
						<< "Task: " << task->getId() << "\n"
						<< "Policy:\n" << policy;
				}

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

	} // end namespace detail




	// the main entry point for scheduling
	void schedule(TaskPtr&& task) {
		detail::schedule(std::move(task));
	}

	void installSchedulerService(com::Network& network) {
		com::HierarchicalOverlayNetwork hierarchy(network);

		// get the scheduling policy to be utilized
		std::string option = "uniform";
		if (auto user = std::getenv("ART_SCHEDULER")) {
			option = user;
		}

		// instantiate the scheduling policy
		std::unique_ptr<SchedulingPolicy> policy;
		if (option == std::string("random")) {
			policy = std::make_unique<RandomSchedulingPolicy>(getCutOffLevel(network.numNodes()));
		} else {
			if (option != std::string("uniform")) {
				std::cout << "Unsupported user-defined scheduling policy: " << option << "\n";
				std::cout << "Using default: uniform\n";
			}
			policy = std::make_unique<DecisionTreeSchedulingPolicy>(DecisionTreeSchedulingPolicy::createUniform(network.numNodes()));
		}
		assert_true(policy);

		// start up scheduling service
		hierarchy.installServiceOnNodes<detail::ScheduleService>(*policy);
		hierarchy.installServiceOnNodes<data::DataItemIndexService>();
	}

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
