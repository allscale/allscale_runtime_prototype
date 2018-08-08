
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

	namespace {

		int ceilLog2(int x) {
			int i = 0;
			int c = 1;
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
			DecisionTree res((1<<(2*log2)));

			// fill in decisions recursively
			fillTree(res,TaskPath::root(), 0, ceilLog2(num_nodes), 0, num_nodes, &*map.begin(),&*map.end());

			// done
			return std::move(res);
		}

	}

	DecisionTree::DecisionTree(int numNodes) : encoded((2*2*numNodes/8)) { // 2 bits for 2x the number of nodes
		assert_eq((1<<ceilLog2(numNodes)),numNodes) << "Number of nodes needs to be a power of 2!";
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

		com::rank_t traceTarget(const SchedulingPolicy& policy, TaskPath p) {
			auto res = policy.getTarget(p);
			while(!res.isLeaf()) {
				p = p.getLeftChildPath();
				res = policy.getTarget(p);
			}
			return res.getRank();
		}

	}

	std::vector<com::rank_t> SchedulingPolicy::getTaskDistributionMapping() const {
		std::vector<com::rank_t> res;
		for(const auto& cur : getAllPaths(granulartiy)) {
			if (cur.getLength() != granulartiy) continue;
			res.push_back(traceTarget(*this,cur));
		}
		return res;
	}


	// create a uniform distributing policy for N nodes
	SchedulingPolicy SchedulingPolicy::createUniform(int N, int granularity) {
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




	// create a balanced work distribution based on the given load distribution
	SchedulingPolicy SchedulingPolicy::createReBalanced(const SchedulingPolicy& p, const std::vector<float>& load) {

		// get given task distribution mapping

		// update mapping
		std::vector<com::rank_t> mapping = p.getTaskDistributionMapping();

		// --- estimate costs per task ---

		// get number of nodes
		com::rank_t numNodes = 0;
		for(const auto& cur : mapping) {
			numNodes = std::max<com::rank_t>(numNodes,cur+1);
		}

		// test that load vector has correct size
		assert_eq(numNodes,load.size())
			<< "Needs current load of all nodes!\n";

		// test that all load values are positive
		assert_true(std::all_of(load.begin(),load.end(),[](float x) { return x >= 0; }))
			<< "Measures loads must be positive!";

		// count number of tasks per node
		std::vector<int> oldShare(numNodes,0);
		for(const auto& cur : mapping) {
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

		float share = totalCosts / numNodes;


		float curCosts = 0;
		float nextGoal = share;
		com::rank_t curNode = 0;
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
			std::cout << "\n";
		}

		// create new scheduling policy
		auto root = p.getPresumedRootAddress();
		auto log2 = root.getLayer();
		return { root, p.granulartiy, toDecisionTree((1<<log2),newMapping) };
	}


	bool SchedulingPolicy::isInvolved(const com::HierarchyAddress& addr, const TaskPath& path) const {

		// only the root node is involved in scheduling the root path
		if (path.isRoot()) return addr == root;

		// TODO: implement this in O(logN) instead of O(logN^2)

		// either the given address is involved in the parent or this node
		return isInvolved(addr,path.getParentPath()) || getTarget(path) == addr;
	}

	Decision SchedulingPolicy::decide(const com::HierarchyAddress& addr, const TaskPath& path) const {
		// make sure this task is involved in the scheduling of this task
		assert_pred2(isInvolved,addr,path);

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

	com::HierarchyAddress SchedulingPolicy::getTarget(const TaskPath& path) const {
		// for roots it is easy
		if (path.isRoot()) return root;

		// for everything else, we walk recursive
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

	SchedulingPolicy SchedulingPolicy::load(allscale::utils::ArchiveReader& in) {
		auto root = in.read<com::HierarchyAddress>();
		auto gran = in.read<int>();
		return { root, gran, in.read<DecisionTree>() };
	}

	void SchedulingPolicy::store(allscale::utils::ArchiveWriter& out) const {
		out.write(root);
		out.write(granulartiy);
		out.write(tree);
	}

	std::ostream& operator<<(std::ostream& out, const SchedulingPolicy& p) {
		return out << p.tree << "Mapping: " << p.getTaskDistributionMapping();
	}

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
