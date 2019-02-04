
#include <algorithm>
#include <cmath>

#include "allscale/utils/assert.h"
#include "allscale/utils/printer/vectors.h"

#include "allscale/utils/serializer/vectors.h"

#include "allscale/runtime/com/node.h"
#include "allscale/runtime/work/schedule_policy.h"
#include "allscale/runtime/hw/model.h"

namespace allscale {
namespace runtime {
namespace work {


	std::ostream& operator<<(std::ostream& out, const SchedulingPolicy& policy) {
		policy.printTo(out);
		return out;
	}

	ExchangeableSchedulingPolicy::ExchangeableSchedulingPolicy(const ExchangeableSchedulingPolicy& other)
		: SchedulingPolicy(loadAsUniquePtr), policy(other.policy->clone()) {};

	ExchangeableSchedulingPolicy::ExchangeableSchedulingPolicy(std::unique_ptr<SchedulingPolicy>&& policy)
		: SchedulingPolicy(loadAsUniquePtr), policy(std::move(policy)) {
		assert_true(bool(this->policy));
	}

	ExchangeableSchedulingPolicy::ExchangeableSchedulingPolicy(const SchedulingPolicy& policy)
		: SchedulingPolicy(loadAsUniquePtr), policy(policy.clone()) {}


	ExchangeableSchedulingPolicy& ExchangeableSchedulingPolicy::operator=(const ExchangeableSchedulingPolicy& other) {
		if (this == &other) return *this;
		policy = other.policy->clone();
		return *this;
	}

	std::unique_ptr<SchedulingPolicy> ExchangeableSchedulingPolicy::clone() const {
		return std::make_unique<ExchangeableSchedulingPolicy>(policy->clone());
	}



	bool RandomSchedulingPolicy::isInvolved(const com::HierarchyAddress& addr, const TaskPath& path) const {

		// the root is always involved
		if (addr == root) return true;

		// check whether task has been decomposed sufficiently for the current level
		return int(path.getLength()) - 3 >= int(root.getLayer()) - int(addr.getLayer());
	}

	Decision RandomSchedulingPolicy::decide(const com::HierarchyAddress& addr, const TaskPath& path) const {
		// spread up tasks on root level, to avoid strong biases in task distribution depending on root decision
		if (path.getLength() < 3) return Decision::Stay;

		// if we are on the tasks's involvement limit layer => stay
		if (int(path.getLength() - 3 == int(root.getLayer()) - int(addr.getLayer()))) {
			return Decision::Stay;
		}

		// decide whether to send left or right randomly
		auto r = policy(generator);
		return (r < 0.5) ? Decision::Left : Decision::Right;
	}

	com::rank_t RandomSchedulingPolicy::estimateTargetLocation(const TaskPath&) const {
		// the local node is an as good estimate as any other location
		return com::Node::getLocalRank();
	}

	bool RandomSchedulingPolicy::shouldSplit(const TaskPath& path) const {
		// if the path is bellow the cut-off level, keep splitting
		return path.getLength() < cutOffLevel;
	}

	bool RandomSchedulingPolicy::shouldAcquireData(const TaskPath&) const {
		// for random scheduling, let only processed tasks acquire their data
		return false;
	}

	std::unique_ptr<SchedulingPolicy> RandomSchedulingPolicy::clone() const {
		return std::make_unique<RandomSchedulingPolicy>(root,cutOffLevel);
	}

	void RandomSchedulingPolicy::printTo(std::ostream& out) const {
		out << "RandomScheduling";
	}

	void RandomSchedulingPolicy::storeInternal(allscale::utils::ArchiveWriter& out) const {
		out.write(root);
		out.write(cutOffLevel);
	}

	std::unique_ptr<SchedulingPolicy> RandomSchedulingPolicy::loadAsUniquePtr(allscale::utils::ArchiveReader& in) {
		auto root = in.read<com::HierarchyAddress>();
		auto cut = in.read<int>();
		return std::make_unique<RandomSchedulingPolicy>(root,cut);
	}



	namespace {

		int ceilLog2(std::uint64_t x) {
			int i = 0;
			std::uint64_t c = 1;
			while (c<x) {
				c = c << 1;
				i++;
			}
			return i;
		}

		std::vector<com::rank_t> getEqualDistribution(const NodeMask& mask, int numTasks) {
			std::vector<com::rank_t> res(numTasks);

			// fill it with an even load task distribution
			auto numNodes = mask.count();
			int share = numTasks / numNodes;
			int remainder = numTasks % numNodes;
			int c = 0;
			int l = 0;
			for(com::rank_t i : mask.getNodes()) {
				for(int j=0; j<share; j++) {
					res[c++] = i;
				}
				if (l<remainder) {
					res[c++] = i;
					l++;
				}
			}
			assert_eq(c,numTasks);

			return res;
		}

		int getPosition(const TaskPath& p) {
			return (1 << p.getLength()) | p.getPath();
		}

		void fillTree(DecisionTree& tree, TaskPath p, int depth, int max_depth, std::size_t range_min, std::size_t range_max, const com::rank_t* min, const com::rank_t* max) {

			if (depth >= max_depth) return;

			// the center of the task id range
			std::size_t center = range_min + (range_max - range_min)/2;

			// process current
			auto cur_min = *min;
			auto cur_max = (min == max) ? cur_min : *(max-1);


			// the min/max forwarded to the nested call
			std::size_t nested_range_min = range_min;
			std::size_t nested_range_max = range_max;

			// make decision
			if (cur_max < center) {
				depth++;
				tree.set(p,Decision::Left);
				nested_range_max = center;
			} else if (center <= cur_min) {
				depth++;
				tree.set(p,Decision::Right);
				nested_range_min = center;
			} else {
				tree.set(p,Decision::Stay);
			}

			// check whether we are done
			if (depth >= max_depth && min + 1 >= max) return;

			// fill current node

			// cover rest
			auto mid = min + (max - min)/2;

			// fill left
			fillTree(tree,p.getLeftChildPath(),depth,max_depth,nested_range_min,nested_range_max,min,mid);

			// fill right
			fillTree(tree,p.getRightChildPath(),depth,max_depth,nested_range_min,nested_range_max,mid,max);

		}


		DecisionTree toDecisionTree(com::rank_t num_nodes, const std::vector<com::rank_t>& map) {

			// create the resulting decision tree
			auto log2 = ceilLog2(map.size());
			assert_eq((1u<<log2),map.size()) << "Input map is not of power-of-two size: " << map.size();

			// the resulting (unbalanced) tree has to be at most 2x as deep
			DecisionTree res((std::uint64_t(1)<<(2*log2)));

			// fill in decisions recursively
			fillTree(res,TaskPath::root(), 0, ceilLog2(num_nodes), 0, num_nodes, &*map.begin(),&*map.end());

			// done
			return res;
		}

	}

	DecisionTree::DecisionTree(std::uint64_t numNodes) : encoded((2*2*numNodes/8)) { // 2 bits for 2x the number of nodes
		assert_eq((std::uint64_t(1)<<ceilLog2(numNodes)),numNodes) << "Number of nodes needs to be a power of 2!";
	}

	std::ostream& operator<<(std::ostream& out, Decision d) {
		switch(d) {
		case Decision::Done:  return out << "Done";
		case Decision::Stay:  return out << "Stay";
		case Decision::Left:  return out << "Left";
		case Decision::Right: return out << "Right";
		}
		return out << "?";
	}


	// updates a decision for a given path
	void DecisionTree::set(const TaskPath& path, Decision decision) {
		int pos = getPosition(path);
		assert_lt(std::size_t(pos/4),encoded.size());
		int byte_pos = 2*pos / 8;
		int bit_pos = (2*pos) % 8;
		encoded[byte_pos] = (encoded[byte_pos] & ~(0x3 << bit_pos)) | (int(decision) << bit_pos);
	}

	// retrieves the decision for a given path
	Decision DecisionTree::get(const TaskPath& path) const {

		// check path length
		int pos = getPosition(path);
		int byte_pos = 2*pos / 8;
		int bit_pos = (2*pos) % 8;

		if (std::size_t(byte_pos) > encoded.size()) return Decision::Done;

		// extract encoded value
		return Decision((encoded[byte_pos] >> bit_pos) & 0x3);
	}

	namespace {

		void collectPaths(const TaskPath& cur, std::vector<TaskPath>& res, int depth) {
			if (depth < 0) return;
			res.push_back(cur);
			collectPaths(cur.getLeftChildPath(),res,depth-1);
			collectPaths(cur.getRightChildPath(),res,depth-1);
		}

		std::vector<TaskPath> getAllPaths(int depth) {
			std::vector<TaskPath> res;
			collectPaths(TaskPath::root(),res,depth);
			return res;
		}

	}

	DecisionTree DecisionTree::load(allscale::utils::ArchiveReader& in) {
		return in.read<std::vector<uint8_t>>();
	}

	void DecisionTree::store(allscale::utils::ArchiveWriter& out) const {
		out.write(encoded);
	}

	namespace {

		void printHelper(std::ostream& out, const DecisionTree& tree, const TaskPath& path) {
			auto d = tree.get(path);
			switch(d) {
			case Decision::Done: return;
			case Decision::Stay:
			case Decision::Left:
			case Decision::Right:
				out << path << " => " << d << "\n";
			}
			printHelper(out,tree,path.getLeftChildPath());
			printHelper(out,tree,path.getRightChildPath());
		}

	}

	std::ostream& operator<<(std::ostream& out, const DecisionTree& tree) {
		printHelper(out,tree,TaskPath::root());
		return out;

	}

	namespace {

		com::rank_t traceTarget(const DecisionTreeSchedulingPolicy& policy, TaskPath p) {
			auto res = policy.getTarget(p);
			while(!res.isLeaf()) {
				assert_lt(p.getLength(),2*policy.getGranularity());
				p = p.getLeftChildPath();
				res = policy.getTarget(p);
			}
			return res.getRank();
		}

	}

	std::vector<com::rank_t> DecisionTreeSchedulingPolicy::getTaskDistributionMapping() const {
		std::vector<com::rank_t> res;
		res.reserve((1<<granulartiy));
		for(const auto& cur : getAllPaths(granulartiy)) {
			if (cur.getLength() != granulartiy) continue;
			res.push_back(traceTarget(*this,cur));
		}
		return res;
	}

	DecisionTreeSchedulingPolicy DecisionTreeSchedulingPolicy::createUniform(const NodeMask& mask, int granularity) {
		// some sanity checks
		assert_lt(0,mask.count());

		// compute number of levels to be scheduled
		auto log2 = ceilLog2(mask.totalNodes());
		auto levels = std::max(log2,granularity);

		// create initial task to node mapping
		int numTasks = (1<<levels);
		std::vector<std::uint32_t> mapping = getEqualDistribution(mask,numTasks);

		// convert mapping in decision tree
		return { com::HierarchyAddress::getRootOfNetworkSize(mask.totalNodes()), levels, toDecisionTree((1<<log2),mapping) };
	}

	DecisionTreeSchedulingPolicy DecisionTreeSchedulingPolicy::createUniform(const NodeMask& mask) {
		return createUniform(mask,std::max(ceilLog2(mask.totalNodes())+3,5));
	}

	// create a uniform distributing policy for N nodes
	DecisionTreeSchedulingPolicy DecisionTreeSchedulingPolicy::createUniform(int N, int granularity) {
		return createUniform(NodeMask(N),granularity);
	}

	// create an uniform distributing policy for N nodes, with an auto-adjusted granularity
	DecisionTreeSchedulingPolicy DecisionTreeSchedulingPolicy::createUniform(int N) {
		return createUniform(N,std::max(ceilLog2(N)+3,5));
	}

	namespace {

		// create a balanced work distribution based on the given load distribution
		DecisionTreeSchedulingPolicy rebalanceTasks(const DecisionTreeSchedulingPolicy& p, const std::vector<float>& taskCosts, const NodeMask& mask) {

			// check input
			std::size_t mappingSize = 1<<p.getGranularity();
			assert_eq(mappingSize,taskCosts.size());

			com::rank_t numNodes = mask.totalNodes();

			// --- redistributing costs ---

			// get number of now available nodes
			int availableNodes = 0;
			for(bool x : mask.asBitMask()) if (x) availableNodes++;

			// if there is really none, make it at least one
			if (availableNodes < 1) availableNodes = 1;

			float totalCosts = std::accumulate(taskCosts.begin(),taskCosts.end(), 0.0f);
			float share = totalCosts / availableNodes;

			float curCosts = 0;
			float nextGoal = share;
			com::rank_t curNode = 0;
			while(!mask.isActive(curNode)) curNode++;
			std::vector<com::rank_t> newMapping(mappingSize);
			for(std::size_t i=0; i<mappingSize; i++) {

				// compute next costs
				auto nextCosts = curCosts + taskCosts[i];

				// if we cross a boundary
				if (curNode < (numNodes-1)) {

					// decide whether to switch to next node
					if (std::abs(curCosts-nextGoal) < std::abs(nextGoal-nextCosts)) {
						// stopping here is closer to the target
						curNode++;
						while(!mask.isActive(curNode)) curNode++;
						nextGoal += share;
					}
				}

				// else, just add current task to current node
				newMapping[i] = curNode;
				curCosts = nextCosts;
			}

//			// for development, to estimate quality:
//
//			const bool DEBUG = false;
//			if (DEBUG) {
//				// --- compute new load distribution ---
//				std::vector<float> newEstCosts(numNodes,0);
//				for(std::size_t i=0; i<mappingSize; i++) {
//					newEstCosts[newMapping[i]] += costs[mapping[i]];
//				}
//
//				// compute initial load imbalance variance
//				auto mean = [](const std::vector<float>& v)->float {
//					float s = 0;
//					for(const auto& cur : v) {
//						s += cur;
//					}
//					return s / v.size();
//				};
//				auto variance = [&](const std::vector<float>& v)->float {
//					float m = mean(v);
//					float s = 0;
//					for(const auto& cur : v) {
//						auto d = cur - m;
//						s += d*d;
//					}
//					return s / (v.size()-1);
//				};
//
//
//				std::cout << "Load vector: " << load << " - " << mean(load) << " / " << variance(load) << "\n";
//				std::cout << "Est. vector: " << newEstCosts << " - " << mean(newEstCosts) << " / " << variance(newEstCosts) << "\n";
//				std::cout << "Target Load: " << share << "\n";
//				std::cout << "Task shared: " << oldShare << "\n";
//				std::cout << "Node costs:  " << costs << "\n";
//				std::cout << "Task costs:  " << taskCosts << "\n";
//				std::cout << "In-distribution:  " << mapping << "\n";
//				std::cout << "Out-distribution: " << newMapping << "\n";
//				std::cout << toDecisionTree((1<<p.getPresumedRootAddress().getLayer()),newMapping) << "\n";
//				std::cout << "\n";
//			}

			// create new scheduling policy
			auto root = p.getPresumedRootAddress();
			auto log2 = root.getLayer();
			return { root, p.getGranularity(), toDecisionTree((1<<log2),newMapping) };
		}

	}

	// create a balanced work distribution based on the given load distribution
	DecisionTreeSchedulingPolicy DecisionTreeSchedulingPolicy::createReBalanced(const DecisionTreeSchedulingPolicy& p, const std::vector<float>& load) {
		NodeMask mask(load.size());
		return createReBalanced(p,load,mask);
	}

	// create a balanced work distribution based on the given load distribution
	DecisionTreeSchedulingPolicy DecisionTreeSchedulingPolicy::createReBalanced(const DecisionTreeSchedulingPolicy& p, const std::vector<float>& load, const NodeMask& mask) {

		// check input consistency
		assert_eq(load.size(),mask.totalNodes());

		// get given task distribution mapping


		// --- estimate costs per task ---

		// get number of nodes
		com::rank_t numNodes = load.size();

		// test that load vector has correct size
		assert_eq(numNodes,load.size())
			<< "Needs current load of all nodes!\n";

		// test that all load values are positive
		assert_true(std::all_of(load.begin(),load.end(),[](float x) { return x >= 0; }))
			<< "Measures loads must be positive!";

		// count number of tasks per node
		std::vector<int> oldShare(numNodes,0);
		std::vector<com::rank_t> mapping = p.getTaskDistributionMapping();
		for(const auto& cur : mapping) {
			assert_lt(cur,numNodes);
			oldShare[cur]++;
		}

		// compute average costs for tasks (default value)
		float sum = 0;
		for(const auto& cur : load) sum += cur;
		float avg = sum / mapping.size();

		// average costs per task on node
		std::vector<float> costs(numNodes);
		for(com::rank_t i=0; i<numNodes; i++) {
			if (oldShare[i] == 0) {
				costs[i] = avg;
			} else {
				costs[i] = load[i]/oldShare[i];
			}
		}

		// create vector of costs per task
		std::vector<float> taskCosts(mapping.size());
		for(std::size_t i=0; i<mapping.size(); i++) {
			taskCosts[i] = costs[mapping[i]];
		}


		// --- redistributing costs ---

		return rebalanceTasks(p,taskCosts,mask);
	}

	namespace {

		void sampleTaskTimes(const mon::TaskTimes& times, TaskPath cur, std::size_t depth, std::vector<float>& res) {

			// if we are deep enough ..
			if (cur.getLength() == depth) {
				res[cur.getPath()] = times.getTime(cur).count() / 1e6;
				return;
			}

			// otherwise process left and right
			sampleTaskTimes(times, cur.getLeftChildPath(), depth, res);
			sampleTaskTimes(times, cur.getRightChildPath(), depth, res);
		}

	}

	// create a balanced work distribution based on the given load distribution
	DecisionTreeSchedulingPolicy DecisionTreeSchedulingPolicy::createReBalanced(const DecisionTreeSchedulingPolicy& p, const mon::TaskTimes& times, const NodeMask& mask) {

		// extract task cost vector from task times
		std::size_t mappingSize = 1 << p.getGranularity();
		std::vector<float> taskCosts(mappingSize);

		// sample measured times
		sampleTaskTimes(times, TaskPath::root(), p.getGranularity(), taskCosts);

		// compute new schedule
		return rebalanceTasks(p,taskCosts,mask);

	}


	bool DecisionTreeSchedulingPolicy::isInvolved(const com::HierarchyAddress& addr, const TaskPath& path) const {

		// the root node is always involved
		if (addr == root) return true;

		// base case - the root path
		if (path.isRoot()) {

			switch(decide(root,path)) {
			case (Decision::Stay)  : return false;
			case (Decision::Left)  : return addr == root.getLeftChild();
			case (Decision::Right) : return addr == root.getRightChild();
			default: return false;
			}

			// not passed by
			return false;

		}

		// TODO: implement this in O(logN) instead of O(logN^2)

		// either the given address is involved in the parent or this node
		return isInvolved(addr,path.getParentPath()) || getTarget(path) == addr;
	}

	Decision DecisionTreeSchedulingPolicy::decide(const com::HierarchyAddress& addr, const TaskPath& path) const {
		// make sure this address is involved in the scheduling of this task
//		assert_pred2(isInvolved,addr,path);

		// if this is a leaf, there are not that many options :)
		if (addr.isLeaf()) return Decision::Stay;

		auto cur = path;
		while(true) {
			// for the root path, the decision is clear
			if (cur.isRoot()) return (root == addr) ? tree.get(cur) : Decision::Stay;

			// see whether the addressed node is the node targeted by the parent path
			auto parent = cur.getParentPath();
			if (getTarget(parent) == addr) {
				return tree.get(cur);
			}

			// walk one step further up
			cur = parent;
		}
	}

	bool DecisionTreeSchedulingPolicy::shouldSplit(const TaskPath& path) const {
		static uint32_t workerSlack = ceilLog2(hw::getWorkerPoolConfig(com::Node::getLocalRank()).size());

		// should stop splitting once within node and enough local tasks for all workers
		auto nodeLevel = getLevelReachingNode(path);

		// if the task is not even at node level => instruct to split
		if (!bool(nodeLevel)) return true;

		// give some extra slackiness for inter-node balancing (stealing)
		uint32_t limit = *nodeLevel + workerSlack + 5;

		// split, if limit is not yet reached
		return path.getLength() < limit;
	}

	bool DecisionTreeSchedulingPolicy::shouldAcquireData(const TaskPath& path) const {

		// should stop splitting once within node and enough local tasks for all workers
		auto nodeLevel = getLevelReachingNode(path);

		// if the task is not even at node level => do not acquire data
		if (!bool(nodeLevel)) return false;

		// acquire data as soon as a task in on the node level
		return true;
	}

	com::HierarchyAddress DecisionTreeSchedulingPolicy::getTarget(const TaskPath& path) const {

		// special case: root path
		if (path.isRoot()) {

			// determine target node of root task
			switch(decide(root,path)) {
			case Decision::Left : return root.getLeftChild();
			case Decision::Right : return root.getRightChild();
			default: return root;
			}

		}

		// get location of parent task
		auto res = getTarget(path.getParentPath());

		// simulate scheduling
		switch(decide(res,path)) {
		case Decision::Done  : return res;
		case Decision::Stay  : return res;
		case Decision::Left  : return res.getLeftChild();
		case Decision::Right : return res.getRightChild();
		}
		assert_fail();
		return res;
	}

	namespace {

		std::pair<com::HierarchyAddress,int32_t> obtainLevelReachingNode(const DecisionTreeSchedulingPolicy& policy, const TaskPath& path) {

			auto buildResult = [&](com::HierarchyAddress&& trg) {
				int level = trg.isLeaf()?path.getLength():-1;
				return std::make_pair<com::HierarchyAddress,int32_t>(std::move(trg),std::move(level));
			};

			// handle root case
			if (path.isRoot()) {
				return buildResult(policy.getTarget(path));
			}

			// step case
			auto res = obtainLevelReachingNode(policy,path.getParentPath());
			if (res.first.isLeaf()) return res;

			// simulate one scheduling step
			switch(policy.decide(res.first,path)) {
			case Decision::Done  : assert_fail() << "Should not be done, not reached a node yet!"; return res;
			case Decision::Stay  : return res;
			case Decision::Left  : return buildResult(res.first.getLeftChild());
			case Decision::Right : return buildResult(res.first.getRightChild());
			}
			assert_fail();
			return res;

		}

	}


	allscale::utils::optional<uint32_t> DecisionTreeSchedulingPolicy::getLevelReachingNode(const TaskPath& path) const {
		// TODO: improve this if it gets to costly (e.g. by materializing it in a table or caching it)
		int32_t level = obtainLevelReachingNode(*this,path).second;
		if (level < 0) return {};
		return level;
	}

	std::unique_ptr<SchedulingPolicy> DecisionTreeSchedulingPolicy::clone() const {
		return std::make_unique<DecisionTreeSchedulingPolicy>(*this);
	}

	std::unique_ptr<SchedulingPolicy> DecisionTreeSchedulingPolicy::loadAsUniquePtr(allscale::utils::ArchiveReader& in) {
		auto root = in.read<com::HierarchyAddress>();
		auto gran = in.read<int>();
		return std::make_unique<DecisionTreeSchedulingPolicy>( root, gran, in.read<DecisionTree>() );
	}

	void DecisionTreeSchedulingPolicy::storeInternal(allscale::utils::ArchiveWriter& out) const {
		out.write(root);
		out.write(granulartiy);
		out.write(tree);
	}

	void DecisionTreeSchedulingPolicy::printTo(std::ostream& out) const {
		out << tree << "Mapping: " << getTaskDistributionMapping();
	}

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
