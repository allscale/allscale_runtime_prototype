
#include <algorithm>
#include <cmath>

#include "allscale/utils/assert.h"
#include "allscale/utils/printer/vectors.h"

#include "allscale/utils/serializer/vectors.h"

#include "allscale/runtime/com/node.h"
#include "allscale/runtime/work/schedule_policy.h"

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



	bool RandomSchedulingPolicy::isInvolved(const com::HierarchyAddress&, const TaskPath&) const {
		return true; // every node may be involved
	}

	Decision RandomSchedulingPolicy::decide(const com::HierarchyAddress&, const TaskPath& path) const {
		// spread up tasks on root level, to avoid strong biases in task distribution depending on root decision
		if (path.getLength() < 3) return Decision::Stay;

		auto r = policy(generator);
		return (r < 0.33) ? Decision::Left  :
			   (r < 0.66) ? Decision::Right :
				  	        (path.getLength() < cutOffLevel)
							  	  ? Decision::Stay
								  : (( r < 0.83 ) ? Decision::Left : Decision::Right) ;
	}


	std::unique_ptr<SchedulingPolicy> RandomSchedulingPolicy::clone() const {
		return std::make_unique<RandomSchedulingPolicy>(cutOffLevel);
	}

	void RandomSchedulingPolicy::printTo(std::ostream& out) const {
		out << "RandomScheduling";
	}

	void RandomSchedulingPolicy::storeInternal(allscale::utils::ArchiveWriter& out) const {
		out.write(cutOffLevel);
	}

	std::unique_ptr<SchedulingPolicy> RandomSchedulingPolicy::loadAsUniquePtr(allscale::utils::ArchiveReader& in) {
		return std::make_unique<RandomSchedulingPolicy>(in.read<int>());
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

		std::vector<com::rank_t> getEqualDistribution(int numNodes, int numTasks) {
			std::vector<com::rank_t> res(numTasks);

			// fill it with an even load task distribution
			int share = numTasks / numNodes;
			int remainder = numTasks % numNodes;
			int c = 0;
			for(int i=0; i<numNodes; i++) {
				for(int j=0; j<share; j++) {
					res[c++] = i;
				}
				if (i<remainder) {
					res[c++] = i;
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


	// create a uniform distributing policy for N nodes
	DecisionTreeSchedulingPolicy DecisionTreeSchedulingPolicy::createUniform(int N, int granularity) {
		// some sanity checks
		assert_lt(0,N);

		// compute number of levels to be scheduled
		auto log2 = ceilLog2(N);
		auto levels = std::max(log2,granularity);

		// create initial task to node mapping
		int numTasks = (1<<levels);
		std::vector<std::uint32_t> mapping = getEqualDistribution(N,numTasks);

		// convert mapping in decision tree
		return { com::HierarchyAddress::getRootOfNetworkSize(N), levels, toDecisionTree((1<<log2),mapping) };
	}

	// create an uniform distributing policy for N nodes, with an auto-adjusted granularity
	DecisionTreeSchedulingPolicy DecisionTreeSchedulingPolicy::createUniform(int N) {
		return createUniform(N,std::max(ceilLog2(N)+3,5));
	}

	// create a balanced work distribution based on the given load distribution
	DecisionTreeSchedulingPolicy DecisionTreeSchedulingPolicy::createReBalanced(const DecisionTreeSchedulingPolicy& p, const std::vector<float>& load) {
		std::vector<bool> mask(load.size(),true);
		return createReBalanced(p,load,mask);
	}

	// create a balanced work distribution based on the given load distribution
	DecisionTreeSchedulingPolicy DecisionTreeSchedulingPolicy::createReBalanced(const DecisionTreeSchedulingPolicy& p, const std::vector<float>& load, const std::vector<bool>& mask) {

		// check input consistency
		assert_eq(load.size(),mask.size());

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
		float totalCosts = 0;
		std::vector<float> taskCosts(mapping.size());
		for(std::size_t i=0; i<mapping.size(); i++) {
			taskCosts[i] = costs[mapping[i]];
			totalCosts += taskCosts[i];
		}


		// --- redistributing costs ---

		// get number of now available nodes
		int availableNodes = 0;
		for(bool x : mask) if (x) availableNodes++;

		// if there is really none, make it at least one
		if (availableNodes < 1) availableNodes = 1;

		float share = totalCosts / availableNodes;

		float curCosts = 0;
		float nextGoal = share;
		com::rank_t curNode = 0;
		while(!mask[curNode]) curNode++;
		std::vector<com::rank_t> newMapping(mapping.size());
		for(std::size_t i=0; i<mapping.size(); i++) {

			// compute next costs
			auto nextCosts = curCosts + taskCosts[i];

			// if we cross a boundary
			if (curNode < (numNodes-1)) {

				// decide whether to switch to next node
				if (std::abs(curCosts-nextGoal) < std::abs(nextGoal-nextCosts)) {
					// stopping here is closer to the target
					curNode++;
					while(!mask[curNode]) curNode++;
					nextGoal += share;
				}
			}

			// else, just add current task to current node
			newMapping[i] = curNode;
			curCosts = nextCosts;
		}

		// for development, to estimate quality:

		const bool DEBUG = false;
		if (DEBUG) {
			// --- compute new load distribution ---
			std::vector<float> newEstCosts(numNodes,0);
			for(std::size_t i=0; i<mapping.size(); i++) {
				newEstCosts[newMapping[i]] += costs[mapping[i]];
			}

			// compute initial load imbalance variance
			auto mean = [](const std::vector<float>& v)->float {
				float s = 0;
				for(const auto& cur : v) {
					s += cur;
				}
				return s / v.size();
			};
			auto variance = [&](const std::vector<float>& v)->float {
				float m = mean(v);
				float s = 0;
				for(const auto& cur : v) {
					auto d = cur - m;
					s += d*d;
				}
				return s / (v.size()-1);
			};


			std::cout << "Load vector: " << load << " - " << mean(load) << " / " << variance(load) << "\n";
			std::cout << "Est. vector: " << newEstCosts << " - " << mean(newEstCosts) << " / " << variance(newEstCosts) << "\n";
			std::cout << "Target Load: " << share << "\n";
			std::cout << "Task shared: " << oldShare << "\n";
			std::cout << "Node costs:  " << costs << "\n";
			std::cout << "Task costs:  " << taskCosts << "\n";
			std::cout << "In-distribution:  " << mapping << "\n";
			std::cout << "Out-distribution: " << newMapping << "\n";
			std::cout << toDecisionTree((1<<p.getPresumedRootAddress().getLayer()),newMapping) << "\n";
			std::cout << "\n";
		}

		// create new scheduling policy
		auto root = p.getPresumedRootAddress();
		auto log2 = root.getLayer();
		return { root, p.granulartiy, toDecisionTree((1<<log2),newMapping) };
	}


	bool DecisionTreeSchedulingPolicy::isInvolved(const com::HierarchyAddress& addr, const TaskPath& path) const {

		// the root node is always involved
		if (addr == root) return true;

		// base case - the root path
		if (path.isRoot()) {

			// trace out the path of the root node
			auto cur = root;
			auto d= Decision::Left;
			while(d != Decision::Stay && d != Decision::Done && !cur.isLeaf()) {

				// move on one step
				switch(d = decide(cur,path)) {
				case Decision::Left:  cur = cur.getLeftChild(); break;
				case Decision::Right: cur = cur.getRightChild(); break;
				case Decision::Stay:  /* nothing */ break;
				case Decision::Done:  /* nothing */ break;
				}

				// if we are at the node we want to test => fine
				if (cur == addr) return true;
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
			if (cur.isRoot()) return tree.get(cur);

			// see whether the addressed node is the node targeted by the parent path
			auto parent = cur.getParentPath();
			if (getTarget(parent) == addr) {
				return tree.get(cur);
			}

			// walk one step further up
			cur = parent;
		}
	}

	com::HierarchyAddress DecisionTreeSchedulingPolicy::getTarget(const TaskPath& path) const {

		// get location of parent task
		auto res = (path.isRoot()) ? root : getTarget(path.getParentPath());

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
