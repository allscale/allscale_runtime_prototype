
#include <random>
#include <algorithm>
#include <iomanip>

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

#include "allscale/runtime/hw/energy.h"
#include "allscale/runtime/hw/frequency_scaling.h"

#include "allscale/runtime/work/optimizer.h"
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

				// add allowances
				diis.addAllowanceLocal(allowance);

				// get the id of the task, as the foundation for any decision
				auto id = task->getId();

				// obtain scheduling decisions from policy
				bool involved = false;
				auto d = Decision::Done;
				{
					// lock to obtain save, consistent access to policy
					guard g(policy_lock);
					involved = policy.isInvolved(myAddr,id.getPath());
					if (involved) {
						d = policy.decide(myAddr,id.getPath());
					}
				}

				// if this not is not involved, send task to parent
				if (!involved) {
					assert_false(isRoot) << "Root should always be involved!";
					// => if not, forward to parent
					network.getRemoteProcedure(myAddr.getParent(),&ScheduleService::schedule)(std::move(task));
					return;
				}

				// on leaf level, schedule locally (ignore decision)
				if (myAddr.isLeaf()) {
					scheduleLocal(std::move(task));
					return;
				}

				// if not a leaf, the policy should not state done
				assert_ne(d,Decision::Done);

				// if it should stay, process it here
				if (d == Decision::Stay) {
					scheduleLocal(std::move(task));
					return;
				}

				// here there should only be options left and right left
				assert_true(d == Decision::Left || d == Decision::Right) << "Actual: " << d;

				bool targetLeft = (d == Decision::Left);

				// get address of next virtual node to be involved
				com::HierarchyAddress next = (targetLeft)
						? myAddr.getLeftChild()
						: myAddr.getRightChild();

//				assert_true(policy.isInvolved(next,id.getPath()))
//					<< "Node " << myAddr << ": next step " << next << " is not involved in scheduling of path " << id.getPath() << "\n"
//					<< "Current node is involved: " << policy.isInvolved(myAddr,id.getPath()) << "\n"
//					<< "Decision: " << d << " vs " << policy.decide(myAddr,id.getPath()) << "\n"
//					<< "Splittable: " << task->isSplitable() << "\n"
//					<< "Target: " << policy.getPolicy<DecisionTreeSchedulingPolicy>().getTarget(id.getPath()) << "\n";

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
			void scheduleLocal(TaskPtr&& task, const data::DataItemRegions& allowance = data::DataItemRegions()) {

				// obtain access to the co-located data item index service
				auto& diis = network.getLocalService<data::DataItemIndexService>(myAddr.getLayer());

				// add allowances
				if (!allowance.empty()) {
					// add allowances
					diis.addAllowanceLocal(allowance);
				}

//				// make sure this is as it has been intended by the policy
//				// -- for debugging only, in dynamic settings policy may have changed --
//				if (task->isSplitable()) {
//					guard g(policy_lock);
//					assert_true(policy.checkTarget(myAddr,task->getId().getPath()))
//						<< "Local: " << myAddr << "\n"
//						<< "Task: " << task->getId() << "\n"
//						<< "Policy:\n" << policy;
//				}

				// if this is a leaf or the task can be further split, hand task over to worker here
				if (myAddr.isLeaf() || task->isSplitable()) {

					DLOG << "Scheduling " << task->getId() << " on Node " << com::Node::getLocalRank() << " ... \n";

					// assign to local worker
					com::Node::getLocalService<Worker>().schedule(std::move(task));

					// done
					return;
				}

				// for everything else, pass on to left child on same node
				assert_false(task->isSplitable()) << "Local scheduling for splitable tasks not supported on non-leaf node " << myAddr;

				// compute allowances for this task
				auto reqs = task->getProcessRequirements().getWriteRequirements();
				auto subAllowances = diis.addAllowanceLeft(allowance,reqs);

				// forward task
				auto& childService = com::HierarchicalOverlayNetwork(network).getLocalService<ScheduleService>(myAddr.getLayer()-1);
				childService.scheduleLocal(std::move(task),subAllowances);

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

			// -- global tuning data --

			// the number of currently active nodes
			com::rank_t numActiveNodes;

			// the frequency currently selected for all nodes
			hw::Frequency activeFrequency;

			using config = std::pair<com::rank_t,hw::Frequency>;


			// hill climbing data
			config best;						// < best known option so far
			float best_score = 0;				// < score of best option
			std::vector<config> explore;		// < list of options to explore


			// -- local state information --

			bool active;

		public:

			InterNodeLoadBalancer(com::Node& local)
				: network(com::Network::getNetwork()),
				  node(local),
				  lastSampleTime(now()),
				  lastProcessTime(0),
				  numActiveNodes(network.numNodes()),
				  activeFrequency(hw::getFrequency(node.getRank())),
				  best({numActiveNodes,activeFrequency}),
				  best_score(0),
				  active(true) {

				// only on rank 0 the balancer shall be started
				if (node.getRank() != 0) return;

				// start periodic balancing
				node.getLocalService<utils::PeriodicExecutorService>().runPeriodically(
						[&]{ run(); return true; },
						std::chrono::seconds(5)
				);
			}

			// to be invoked locally to determine whether the local node is active
			// or in stand-by mode regarding task scheduling operations
			bool isActive() const {
				return active;
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

			void updatePolicy(const ExchangeableSchedulingPolicy& policy, com::rank_t numActiveNodes, hw::Frequency frequency) {
				// get local scheduler service
				auto& service = node.getService<com::HierarchyService<ScheduleService>>();
				// update policies
				service.forAll([&](auto& cur) { cur.setPolicy(policy); });

				// update own active state
				active = node.getRank() < numActiveNodes;

				// update CPU clock frequency
				hw::setFrequency(node.getRank(), frequency);
			}

		private:

			void tune(const std::vector<float>& load) {
				using namespace std::literals::chrono_literals;

				// This demo implements a simple hill climbing like tuner.

				// --- rate current state ---

				const int cores_per_node = 1; // for the prototype so far

				// compute current performance score
				auto maxFrequnzy = hw::getFrequencyOptions(0).back();

				std::uint64_t used_cycles = 0;
				std::uint64_t avail_cycles = 0;
				std::uint64_t max_cycles = 0;
				hw::Power used_power = 0;
				hw::Power max_power = 0;

				for(com::rank_t i=0; i<network.numNodes(); i++) {
					if (i<numActiveNodes) {
						used_cycles += load[i] * activeFrequency.toHz() * cores_per_node;
						avail_cycles += activeFrequency.toHz() * cores_per_node;
						used_power += hw::estimateEnergyUsage(activeFrequency,activeFrequency * 1s) / 1s;
					}
					max_cycles += maxFrequnzy.toHz() * cores_per_node;
					max_power += hw::estimateEnergyUsage(maxFrequnzy,maxFrequnzy * 1s) / 1s;
				}

				float speed = used_cycles / float(max_cycles);
				float efficiency = used_cycles / float(avail_cycles);
				float power = used_power / max_power;

				// compute current score
				auto obj = getActiveTuningObjectiv();
				float score = obj.getScore(speed,efficiency,power);

				std::cout << "\tSystem state:"
						<<  " spd=" << std::setprecision(2) << speed
						<< ", eff=" << std::setprecision(2) << efficiency
						<< ", pow=" << std::setprecision(2) << power
						<< " => score: " << std::setprecision(2) << score
						<< " for objective " << obj << "\n";


				// record current solution
				if (score > best_score) {
					best = { numActiveNodes, activeFrequency };
					best_score = score;
				}

				// pick next state

				if (explore.empty()) {

					// nothing left to explore => generate new points
					std::cout << "\t\tPrevious best option " << best.first << " @ " << best.second << " with score " << best_score << "\n";

					// get nearby frequencies
					auto options = hw::getFrequencyOptions(0);
					auto cur = std::find(options.begin(), options.end(), best.second);
					assert_true(cur != options.end());
					auto pos = cur - options.begin();

					std::vector<hw::Frequency> frequencies;
					if (pos != 0) frequencies.push_back(options[pos-1]);
					frequencies.push_back(options[pos]);
					if (pos+1<int(options.size())) frequencies.push_back(options[pos+1]);

					// get nearby node numbers
					std::vector<com::rank_t> numNodes;
					if (best.first > 1) numNodes.push_back(best.first-1);
					numNodes.push_back(best.first);
					if (best.first < network.numNodes()) numNodes.push_back(best.first+1);


					// create new options
					for(const auto& a : numNodes) {
						for(const auto& b : frequencies) {
							std::cout << "\t\tAdding option " << a << " @ " << b << "\n";
							explore.push_back({a,b});
						}
					}

					// reset best options
					best_score = 0;
				}

				// if there are still no options, there is nothing to do
				if (explore.empty()) return;

				// take next option and evaluate
				auto next = explore.back();
				explore.pop_back();

				numActiveNodes = next.first;
				activeFrequency = next.second;

				std::cout << "\t\tSwitching to " << numActiveNodes << " @ " << activeFrequency << "\n";
			}

			void balance() {

				// The load balancing / optimization is split into two parts:
				//  Part A: the balance function, attempting to even out resource utilization between nodes
				//			for a given number of active nodes and a CPU clock frequency
				//  Part B: in evenly balanced cases, the tune function is evaluating the current
				//          performance score and searching for better alternatives


				// -- Step 1: collect node distribution --

				// collect the load of all nodes
				com::rank_t numNodes = network.numNodes();
				std::vector<float> load(numNodes,0.0f);
				for(com::rank_t i=0; i<numActiveNodes; i++) {
					load[i] = network.getRemoteProcedure(i,&InterNodeLoadBalancer::getEfficiency)();
				}

				// compute the load variance
				float avg = std::accumulate(load.begin(),load.end(),0.0f) / numActiveNodes;

				float sum_dist = 0;
				for(com::rank_t i=0; i<numActiveNodes; i++) {
					float dist = load[i] - avg;
					sum_dist +=  dist * dist;
				}
				float var = sum_dist / (numActiveNodes - 1);


				std::cout
					<< "Average load "      << std::setprecision(2) << avg
					<< ", load variance "   << std::setprecision(2) << var
					<< ", total progress " << std::setprecision(2) << (avg*numActiveNodes)
					<< " on " << numActiveNodes << " nodes\n";


				// -- Step 2: if stable, adjust number of nodes and clock speed

				// if stable enough, allow meta-optimizer to manage load
				if (var < 0.01) {

					// adjust number of nodes and CPU frequency
					tune(load);

				}

				// -- Step 3: enforce selected number of nodes and clock speed, keep system balanced

				// compute number of nodes to be used
				std::vector<bool> mask(numNodes);
				for(com::rank_t i=0; i<numNodes; i++) {
					mask[i] = i < numActiveNodes;
				}


				// get the local scheduler
				auto& scheduleService = node.getService<com::HierarchyService<ScheduleService>>().get(0);

				// get current policy
				auto policy = scheduleService.getPolicy();
				assert_true(policy.isa<DecisionTreeSchedulingPolicy>());

				// re-balance load
				auto curPolicy = policy.getPolicy<DecisionTreeSchedulingPolicy>();
				auto newPolicy = DecisionTreeSchedulingPolicy::createReBalanced(curPolicy,load,mask);

				// distribute new policy
				for(com::rank_t i=0; i<numNodes; i++) {
					network.getRemoteProcedure(i,&InterNodeLoadBalancer::updatePolicy)(newPolicy,numActiveNodes,activeFrequency);
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


	bool isLocalNodeActive() {
		auto& node = com::Node::getLocalNode();
		if (node.hasService<detail::InterNodeLoadBalancer>()) {
			return node.getLocalService<detail::InterNodeLoadBalancer>().isActive();
		}
		return true;
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
