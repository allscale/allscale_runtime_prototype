
#include <random>
#include <algorithm>
#include <iomanip>

#include "allscale/utils/printer/vectors.h"
#include "allscale/utils/serializer/pairs.h"

#include "allscale/runtime/utils/timer.h"
#include "allscale/runtime/com/node.h"
#include "allscale/runtime/com/network.h"
#include "allscale/runtime/com/hierarchy.h"

#include "allscale/runtime/log/logger.h"
#include "allscale/runtime/work/node_mask.h"
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
#include "allscale/runtime/work/tuner.h"

#include "allscale/runtime/mon/task_stats.h"

#include "allscale/runtime/hw/model.h"

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
		int getCutOffLevel(int numNodes, int numWorkers) {
			// the cut-off level for "forced" distribution
			return ceilLog2(numNodes) * 2 + ceilLog2(numWorkers) + 3;
		}

		/**
		 * Determines the cut-off level for task splitting
		 */
		int getCutOffLevel() {
			static auto numNodes = com::Network::getNetwork().numNodes();
			static auto numWorkers = hw::getWorkerPoolConfig(com::Node::getLocalRank()).size();

			// use its size
			return getCutOffLevel(numNodes, numWorkers);
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
		 * A service running on each virtual node. This is the tactical scheduling part.
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
			mutable std::mutex policy_lock;

			using guard = std::lock_guard<std::mutex>;

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
						involved = (path.isRoot()) ? isRoot : policy.isInvolved(myAddr,path.getParentPath());
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

				// if this not is not involved ..
				if (!involved) {
					// there has been a scheduler change => process task here
					scheduleLocal(std::move(task));
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

				// if this is a leaf or the task can be further split, hand task over to worker here
				if (myAddr.isLeaf() || task->isSplitable()) {

					DLOG << "Scheduling " << task->getId() << " on Node " << com::Node::getLocalRank() << " ... \n";

					// assign to local worker
					com::Node::getLocalService<WorkerPool>().schedule(std::move(task));

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



		namespace {

			SchedulerType getInitialSchedulerType() {
				// get the scheduling policy to be utilized
				std::string option = "uniform";
				if (auto user = std::getenv("ART_SCHEDULER")) {
					option = user;
				}

				// check validity
				if (option != "uniform" && option != "random" && option != "balanced" && option != "tuned") {
					std::cout << "Unsupported user-defined scheduling policy: " << option << "\n";
					std::cout << "\tSupported options: uniform, random, balanced, or tuned\n";
					std::cout << "Using default: uniform\n";
					option = "uniform";
				}

				if (option == "uniform")  return SchedulerType::Uniform;
				if (option == "balanced") return SchedulerType::Balanced;
				if (option == "tuned")    return SchedulerType::Tuned;
				if (option == "random")   return SchedulerType::Random;

				assert_fail() << "Invalid state: " << option;
				return SchedulerType::Uniform;
			}

		}

		/**
		 * The strategic scheduler is responsible for the management of the tactical
		 * scheduling policy and the inter-node load management activities.
		 */
		class StrategicScheduler {

			using clock = std::chrono::high_resolution_clock;
			using time_point = clock::time_point;

			// the network being part of
			com::Network& network;

			// the node being installed on
			com::Node& node;

			// -- scheduler type management --

			SchedulerType type;

			// -- efficiency measurement --

			time_point lastEfficiencySampleTime;

			std::chrono::nanoseconds lastProcessTime;

			// -- task time measurement --

			time_point lastTaskTimesSampleTime;

			mon::TaskTimes lastTaskTimes;

			// -- global tuning details --

			// the currently active global configuration
			Configuration activeConfig;

			// the tuning algorithm currently utilized -- null if none
			std::unique_ptr<Tuner> tuner;

			// -- local state information --

			bool active;

			mutable std::recursive_mutex lock;

			using guard = std::lock_guard<std::recursive_mutex>;

		public:

			StrategicScheduler(com::Node& local)
				: network(com::Network::getNetwork()),
				  node(local),
				  type(getInitialSchedulerType()),
				  lastEfficiencySampleTime(now()),
				  lastProcessTime(0),
				  lastTaskTimesSampleTime(lastEfficiencySampleTime),
				  activeConfig({network.numNodes(),hw::getFrequency(node.getRank())}),
				  tuner(std::make_unique<IntervalTuner>(activeConfig,std::make_unique<SimpleCoordinateDescent>(activeConfig))),
				  active(true) {

				// enforce initial scheduler type
				updateSchedulerType(type);

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

			// obtains the currently enforced scheduling type
			SchedulerType getActiveSchedulerType() const {
				guard g(lock);
				return type;
			}

			void setActiveSchedulerType(SchedulerType type) {
				guard g(lock);
				// skip if there is no change
				if (this->type == type) return;
				updateSchedulerType(type);
			}

			void changeActiveSchedulerType(SchedulerType type) {
				// broadcast change request to all nodes
				network.broadcast(&detail::StrategicScheduler::setActiveSchedulerType)(type);
			}

			void toggleActiveState(com::rank_t rank) {
				// forward this to the locality 0
				if (node.getRank() != 0) {
					network.getRemoteProcedure(0,&StrategicScheduler::toggleActiveState)(rank);
					return;
				}

				// on the root node ...
				assert_eq(0,node.getRank());

				// .. swap state of given rank
				auto newMask = activeConfig.nodes;
				newMask.toggle(rank);

				// do not disable last node
				if (newMask.count() == 0) return;

				// update state
				activeConfig.nodes = newMask;
				auto newSetup = activeConfig;

				// re-schedule uniform schedule
				if (type == SchedulerType::Uniform) {
					// create new policy
					auto uniform = DecisionTreeSchedulingPolicy::createUniform(newMask);
					// distribute new policy
					for(com::rank_t i=0; i<newMask.totalNodes(); i++) {
						network.getRemoteProcedure(i,&StrategicScheduler::updatePolicy)(uniform,newSetup);
					}
				}
			}

			// to be called by remote to collect this nodes efficiency
			float getEfficiency() {
				// take a sample
				auto curTime = now();
				auto& pool = node.getService<work::WorkerPool>();
				auto curProcess = pool.getProcessTime();

				// compute efficiency
				auto interval = std::chrono::duration_cast<std::chrono::duration<float>>(curTime - lastEfficiencySampleTime);
				auto res = ((curProcess - lastProcessTime)/ interval) / pool.getNumWorkers();

				// keep track of data
				lastEfficiencySampleTime = curTime;
				lastProcessTime = curProcess;

				// done
				return res;
			}

			mon::TaskTimes getTaskTimes() {
				// take a sample
				auto curTime = now();
				auto curTimes = node.getService<work::WorkerPool>().getTaskTimeSummary();

				// normalize to one second
				auto interval = std::chrono::duration_cast<std::chrono::duration<float>>(curTime - lastTaskTimesSampleTime);
				auto res = (curTimes - lastTaskTimes) / interval.count();

				// keep track of data
				lastTaskTimesSampleTime = curTime;
				lastTaskTimes = curTimes;

				// done
				return res;
			}

			std::pair<float,mon::TaskTimes> getStatus() {
				return std::make_pair(getEfficiency(),getTaskTimes());
			}

			void updatePolicy(const ExchangeableSchedulingPolicy& policy, const Configuration& newConfig) {
				guard g(lock);

				// get local scheduler service
				auto& service = node.getService<com::HierarchyService<ScheduleService>>();
				// update policies
				service.forAll([&](auto& cur) { cur.setPolicy(policy); });

				// update local knowledge of active configuration
				this->activeConfig = newConfig;

				// update own active state
				active = activeConfig.nodes.isActive(node.getRank());

				// update CPU clock frequency
				hw::setFrequency(node.getRank(), activeConfig.frequency);
			}

		private:

			void updateSchedulerType(const SchedulerType& newType) {

				// get the total number of nodes
				auto numNodes = network.numNodes();
				auto defaultFrequency = hw::getFrequency(node.getRank());

				// enforce policy
				switch(newType) {
				case SchedulerType::Random : {
					RandomSchedulingPolicy random(com::HierarchicalOverlayNetwork(network).getRootAddress(), getCutOffLevel());
					activeConfig.nodes = NodeMask(numNodes);
					updatePolicy(random,activeConfig);
					break;
				}
				case SchedulerType::Uniform :
				case SchedulerType::Balanced :
				case SchedulerType::Tuned : {
					// reset configuration to initial setup
					auto uniform = DecisionTreeSchedulingPolicy::createUniform(activeConfig.nodes);
					activeConfig.frequency = defaultFrequency;
					updatePolicy(uniform,activeConfig);
					break;
				}
				}

				// update scheduler type
				type = newType;

			}

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

				// compute number of active nodes
				com::rank_t numActiveNodes = activeConfig.nodes.count();

				for(com::rank_t i=0; i<network.numNodes(); i++) {
					if (i<numActiveNodes) {
						used_cycles += load[i] * activeConfig.frequency.toHz() * cores_per_node;
						avail_cycles += activeConfig.frequency.toHz() * cores_per_node;
						used_power += hw::estimateEnergyUsage(activeConfig.frequency,activeConfig.frequency * 1s) / 1s;
					}
					max_cycles += maxFrequnzy.toHz() * cores_per_node;
					max_power += hw::estimateEnergyUsage(maxFrequnzy,maxFrequnzy * 1s) / 1s;
				}

				State s;
				s.speed = used_cycles / float(max_cycles);
				s.efficiency = used_cycles / float(avail_cycles);
				s.power = used_power / max_power;

				// compute current score
				auto obj = getActiveTuningObjectiv();
				s.score = obj.getScore(s.speed,s.efficiency,s.power);

				// print system state summary
				std::cout << s << " for objective " << obj << "\n";

				// get next configuration -- this is the actual strategic policy decision
				activeConfig = tuner->next(activeConfig,s);

				// print status update
				std::cout << "\t\tSwitching to " << activeConfig.nodes.count() << " nodes " << activeConfig.nodes << " @ " << activeConfig.frequency << "\n";
			}

			void balance() {
				guard g(lock);

				// test whether balancing should be performed at all
				if (type == SchedulerType::Random || type == SchedulerType::Uniform) return;

				// The load balancing / optimization is split into two parts:
				//  Part A: the balance function, attempting to even out resource utilization between nodes
				//			for a given number of active nodes and a CPU clock frequency
				//  Part B: in evenly balanced cases, the tune function is evaluating the current
				//          performance score and searching for better alternatives

				// -- Step 1: collect node distribution --

				// collect the load of all nodes
				com::rank_t numNodes = network.numNodes();
				std::vector<float> load(numNodes,0.0f);
				mon::TaskTimes taskTimes;
				for(com::rank_t i : activeConfig.nodes.getNodes()) {
					// TODO: collect inputs asynchrony
					auto cur = network.getRemoteProcedure(i,&StrategicScheduler::getStatus)().get();
					load[i] = cur.first;
					taskTimes += cur.second;
				}

				// compute the load variance
				auto numActiveNodes = activeConfig.nodes.count();
				float avg = std::accumulate(load.begin(),load.end(),0.0f) / numActiveNodes;

				float sum_dist = 0;
				for(com::rank_t i : activeConfig.nodes.getNodes()) {
					float dist = load[i] - avg;
					sum_dist +=  dist * dist;
				}
				float var = (numActiveNodes > 1) ? sum_dist / (numActiveNodes - 1) : 0;


				std::cout
					<< "Average load "      << std::setprecision(2) << avg
					<< ", load variance "   << std::setprecision(2) << var
					<< ", total progress " << std::setprecision(2) << (avg*numActiveNodes)
					<< " on " << numActiveNodes << " nodes\n";



				// -- Step 2: if stable, adjust number of nodes and clock speed

				// if stable enough, allow meta-optimizer to manage load
				if (type == SchedulerType::Tuned && var < 0.01) {

					// adjust number of nodes and CPU frequency
					tune(load);

				}

				// -- Step 3: enforce selected number of nodes and clock speed, keep system balanced

				// get the local scheduler
				auto& scheduleService = node.getService<com::HierarchyService<ScheduleService>>().get(0);

				// get current policy
				auto policy = scheduleService.getPolicy();
				assert_true(policy.isa<DecisionTreeSchedulingPolicy>());

				// re-balance load
				auto curPolicy = policy.getPolicy<DecisionTreeSchedulingPolicy>();
//				auto newPolicy = DecisionTreeSchedulingPolicy::createReBalanced(curPolicy,load,mask);
				auto newPolicy = DecisionTreeSchedulingPolicy::createReBalanced(curPolicy,taskTimes,activeConfig.nodes);

				// distribute new policy
				for(com::rank_t i=0; i<numNodes; i++) {
					network.getRemoteProcedure(i,&StrategicScheduler::updatePolicy)(newPolicy,activeConfig);
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
		if (!node.hasService<detail::StrategicScheduler>()) return true;
		return node.getLocalService<detail::StrategicScheduler>().isActive();
	}

	void installSchedulerService(com::Network& network) {
		com::HierarchicalOverlayNetwork hierarchy(network);

		// start up scheduling service
		hierarchy.installServiceOnNodes<detail::ScheduleService>();
		hierarchy.installServiceOnNodes<data::DataItemIndexService>();
		network.installServiceOnNodes<detail::StrategicScheduler>();
	}


	std::ostream& operator<<(std::ostream& out,const SchedulerType& type) {
		switch(type) {
		case SchedulerType::Uniform:  return out << "uniform";
		case SchedulerType::Balanced: return out << "balanced";
		case SchedulerType::Tuned:    return out << "tuned";
		case SchedulerType::Random:   return out << "random";
		}
		return out << "unknown";
	}

	SchedulerType getCurrentSchedulerType() {
		if (!com::Node::getLocalNode().hasService<detail::StrategicScheduler>()) return SchedulerType::Uniform;
		return com::Node::getLocalService<detail::StrategicScheduler>().getActiveSchedulerType();
	}

	void setCurrentSchedulerType(SchedulerType type) {
		com::Node::getLocalService<detail::StrategicScheduler>().changeActiveSchedulerType(type);
	}

	void toggleActiveState(com::rank_t rank) {
		com::Node::getLocalService<detail::StrategicScheduler>().toggleActiveState(rank);
	}

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
