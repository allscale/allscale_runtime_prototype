
#include <random>
#include <algorithm>

#include "allscale/utils/printer/vectors.h"

#include "allscale/runtime/utils/timer.h"
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

			// the lock for scheduling operations (to avoid policies being exchanged while scheduling)
			mutable std::recursive_mutex policy_lock;

			using guard = std::lock_guard<std::recursive_mutex>;

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

			// updates the utilized policy
			ExchangeableSchedulingPolicy getPolicy() const {
				guard g(policy_lock);
				// access
				return policy;
			}

			// updates the utilized policy
			void setPolicy(const ExchangeableSchedulingPolicy& p) {
				guard g(policy_lock);
				policy = p;
			}

			// requests this scheduler instance to schedule this task.
			void schedule(TaskReference task) {

				// Phase 1: locate virtual node allowed to perform the scheduling
				DLOG << "Start Scheduling " << task->getId() << " on " << myAddr << " ... \n";
				assert_true(task->isReady());

				// check whether current node is allowed to make autonomous scheduling decisions
				auto& diis = network.getLocalService<data::DataItemIndexService>(myAddr.getLayer());
				auto path = task->getId().getPath();
				if (!isRoot) {

					// check whether this virtual node is involved in the scheduling
					bool involved = false;
					{
						guard g(policy_lock);
						involved = policy.isInvolved(myAddr,path);
					}

					// and that this virtual node has control over all required data
					if (involved && diis.covers(task->getProcessRequirements().getWriteRequirements())) {
						// we can schedule it right here!
						DLOG << "Short-cutting " << task->getId() << " on " << myAddr << "\n";
						scheduleDown(std::move(task),{});
						return;
					}
				}

				// TODO: replace this with a schedule-up phase forwarding allowances

				// if there are unallocated allowances still to process, do so
				auto unallocated = diis.getManagedUnallocatedRegion(task->getProcessRequirements().getWriteRequirements());
				if (!unallocated.empty()) {
					// take unallocated share and pass along scheduling process
					scheduleDown(std::move(task),unallocated);
					return;
				}

				// propagate to parent
				if (!isRoot) {
					// forward call to parent node
					network.getRemoteProcedure(myAddr.getParent(),&ScheduleService::schedule)(std::move(task));
					return;
				}

				// Phase 2: propagate task down the hierarchy
				assert_true(isRoot);

				// compute unallocated data item regions
				auto missing = diis.getMissingRegions(task->getProcessRequirements().getWriteRequirements());

				// pass those along with the scheduling process
				scheduleDown(std::move(task),missing);

			}

			void scheduleDown(TaskReference task, const data::DataItemRegions& allowance) {

				// make sure this is processed on the right node
				assert_eq(myAddr.getRank(),com::Node::getLocalRank());

				// obtain access to the co-located data item index service
				auto& diis = network.getLocalService<data::DataItemIndexService>(myAddr.getLayer());

				// on leaf level, schedule locally
				if (myAddr.isLeaf()) {
					diis.addAllowanceLocal(allowance);
					scheduleLocal(std::move(task));
					return;
				}

				// schedule locally if decided to do so
				auto id = task->getId();

				// ask the scheduling policy what to do with this task
				auto d = Decision::Done;
				{
					guard g(policy_lock);
					d = policy.decide(myAddr,id.getPath());
				}

				// over-rule done decisions for inner nodes
				if (d == Decision::Done) {
					d = Decision::Left;
				}

				// if it should stay, process it here
				if (task->isSplitable() && d == Decision::Stay) {	// non-splitable task must not stay on inner level
//					assert_lt(id.getDepth(),getCutOffLevel());
					diis.addAllowanceLocal(allowance);
					scheduleLocal(std::move(task));
					return;
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
				network.getRemoteProcedure(next,&ScheduleService::scheduleDown)(std::move(task),subAllowances);
			}


			// process the task
			void scheduleLocal(TaskPtr&& task) {

				// make sure this is as it has been intended by the policy
//				if (task->isSplitable()) {
//					assert_true(policy.checkTarget(myAddr,task->getId().getPath()))
//						<< "Task: " << task->getId() << "\n"
//						<< "Policy:\n" << policy;
//				}

				DLOG << "Scheduling " << task->getId() << " on Node " << com::Node::getLocalRank() << " ... \n";

				// assign to local worker
				com::Node::getLocalService<Worker>().schedule(std::move(task));
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


		class InterNodeLoadBalancer {

			using clock = std::chrono::high_resolution_clock;
			using time_point = clock::time_point;

			// the network being part of
			com::Network& network;

			// the node being installed on
			com::Node& node;

			// -- efficiency measurement --

			time_point lastSampleTime;

			std::chrono::nanoseconds lastProcessTime;

			com::rank_t lastNumNodes;

		public:

			InterNodeLoadBalancer(com::Node& local)
				: network(com::Network::getNetwork()),
				  node(local),
				  lastSampleTime(now()),
				  lastProcessTime(0),
				  lastNumNodes(network.numNodes()) {

				// only on rank 0 the balancer shall be started
				if (node.getRank() != 0) return;

				// start periodic balancing
				node.getLocalService<utils::PeriodicExecutorService>().runPeriodically(
						[&]{ run(); return true; },
						std::chrono::seconds(15)
				);
			}

			// to be called by remote to collect this nodes efficiency
			float getEfficiency() {
				// take a sample
				auto curTime = now();
				auto curProcess = node.getService<work::Worker>().getProcessTime();

				// compute efficiency
				auto interval = std::chrono::duration_cast<std::chrono::duration<float>>(curTime - lastSampleTime);
				auto res = (curProcess - lastProcessTime)/ interval;

				// keep track of data
				lastSampleTime = curTime;
				lastProcessTime = curProcess;

				// done
				return res;
			}

			void updatePolicy(const ExchangeableSchedulingPolicy& policy) {
				// get local scheduler service
				auto& service = node.getService<com::HierarchyService<ScheduleService>>();
				// update policies
				service.forAll([&](auto& cur) { cur.setPolicy(policy); });
			}

		private:

			com::rank_t adjustNumNodes(const std::vector<float>& load, com::rank_t currentNumNodes) {

				// TODO: improve node adjustment

				// Step 1: stabalize load distribution

				// if load distribution has reached a even enough range
				float min = load.front();
				float max = load.front();
				for(com::rank_t i=0; i<currentNumNodes; i++) {
					min = std::min(load[i],min);
					max = std::max(load[i],max);
				}
				float diff = max - min;


				// Step 2: adjust number of nodes

				float avg = std::accumulate(load.begin(),load.end(),0.0f) / currentNumNodes;
				auto res = currentNumNodes;

				// determine new number of nodes
				if (avg < 0.5) {
					res = std::max<com::rank_t>(res/2,1);
				} else if (diff < 0.1 && avg < 0.6) {
					res = std::max<com::rank_t>(res-1,1);
				} else if (avg > 0.9) {
					res = std::min<com::rank_t>(res+1,network.numNodes());
				}

				std::cout << "Average load " << avg << ", load difference: " << diff;
				if (res != currentNumNodes) {
					std::cout << " - switching from " << currentNumNodes << " to " << res << " nodes for next interval ..\n";
				} else {
					std::cout << " - using " << res << " for next interval ..\n";
				}

				return res;
			}

			void balance() {

				// collect the load of all nodes
				com::rank_t numNodes = network.numNodes();
				std::vector<float> load(numNodes,0.0f);
				for(com::rank_t i=0; i<lastNumNodes; i++) {
					load[i] = network.getRemoteProcedure(i,&InterNodeLoadBalancer::getEfficiency)();
				}

				// adjust number of involved nodes
				lastNumNodes = adjustNumNodes(load,lastNumNodes);

				// compute number of nodes to be used
				std::vector<bool> mask(numNodes);
				for(com::rank_t i=0; i<numNodes; i++) {
					mask[i] = i < lastNumNodes;
				}


				// get the local scheduler
				auto& scheduleService = node.getService<com::HierarchyService<ScheduleService>>().get(0);

				// get current policy
				auto policy = scheduleService.getPolicy();
				assert_true(policy.isa<DecisionTreeSchedulingPolicy>());

				// re-balance load
				auto curPolicy = policy.getPolicy<DecisionTreeSchedulingPolicy>();
				auto newPolicy = DecisionTreeSchedulingPolicy::createReBalanced(curPolicy,load,mask);

				// check whether something has changed
				if (curPolicy == newPolicy) return;

				// distribute new policy
				for(com::rank_t i=0; i<numNodes; i++) {
					network.getRemoteProcedure(i,&InterNodeLoadBalancer::updatePolicy)(newPolicy);
				}
			}

			void run() {
				node.run([&](com::Node&){
					balance();
				});
			}

			static time_point now() {
				return std::chrono::high_resolution_clock::now();
			}

		};

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

		// check validity
		if (option != "uniform" && option != "random" && option != "dynamic") {
			std::cout << "Unsupported user-defined scheduling policy: " << option << "\n";
			std::cout << "Using default: uniform\n";
			option = "uniform";
		}

		// instantiate the scheduling policy
		std::unique_ptr<SchedulingPolicy> policy;
		if (option == "random") {
			policy = std::make_unique<RandomSchedulingPolicy>(getCutOffLevel(network.numNodes()));
		} else {
			// the rest is based on a decision tree scheduler
			policy = std::make_unique<DecisionTreeSchedulingPolicy>(DecisionTreeSchedulingPolicy::createUniform(network.numNodes()));
		}
		assert_true(policy);


		// start up scheduling service
		hierarchy.installServiceOnNodes<detail::ScheduleService>(*policy);
		hierarchy.installServiceOnNodes<data::DataItemIndexService>();

		if (option == "dynamic") {
			network.installServiceOnNodes<detail::InterNodeLoadBalancer>();
		}
	}

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
