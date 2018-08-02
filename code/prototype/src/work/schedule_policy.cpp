
#include "allscale/utils/assert.h"
#include "allscale/utils/printer/vectors.h"

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

		void fillTree(DecisionTree& tree, TaskPath p, std::size_t range_min, std::size_t range_max, const com::rank_t* min, const com::rank_t* max) {

//			std::cout << "\n";

			// the center of the task id range
			std::size_t center = range_min + (range_max - range_min)/2;

//			std::cout << "Covering range " << range_min << " - " << range_max << " : " << center << "\n";

			// process current
			auto cur_min = *min;
			auto cur_max = *(max-1);

//			std::cout << "\tRange: " << cur_min << " - " << cur_max << "\n";


			// the min/max forwarded to the nested call
			std::size_t nested_range_min = range_min;
			std::size_t nested_range_max = range_max;

			// make decision
			if (cur_max < center) {
				tree.set(p,Decision::Left);
				nested_range_max = center;
			} else if (center <= cur_min) {
				tree.set(p,Decision::Right);
				nested_range_min = center;
			} else {
				tree.set(p,Decision::Stay);
			}

			// check whether we are done
			if (min + 1 >= max) return;

			// fill current node

			// cover rest
			auto mid = min + (max - min)/2;

			// fill left
			fillTree(tree,p.getLeftChildPath(),nested_range_min,nested_range_max,min,mid);

			// fill right
			fillTree(tree,p.getRightChildPath(),nested_range_min,nested_range_max,mid,max);

		}


		DecisionTree toDecisionTree(com::rank_t num_nodes, const std::vector<com::rank_t>& map) {

			// create the resulting decision tree
			DecisionTree res(map.size());

			// fill in decisions recursively
			fillTree(res,TaskPath::root(), 0, num_nodes, &*map.begin(),&*map.end());

			// done
			return std::move(res);
		}

	}


	std::ostream& operator<<(std::ostream& out, Decision d) {
		switch(d) {
		case Decision::Stay:  return out << "Stay";
		case Decision::Left:  return out << "Left";
		case Decision::Right: return out << "Right";
		}
		return out << "?";
	}


	// updates a decision for a given path
	void DecisionTree::set(const TaskPath& path, Decision decision) {
		int pos = getPosition(path);
		assert_lt(2*pos,encoded.size());
		encoded[2*pos]     = int(decision) & 0x2;
		encoded[2*pos + 1] = int(decision) & 0x1;
	}

	// retrieves the decision for a given path
	Decision DecisionTree::get(const TaskPath& path) const {

		// check path length
		int pos = getPosition(path);

		if (2*pos > encoded.size()) return Decision::Stay;

		// extract encoded value
		int res = 0;
		if (encoded[2*pos  ]) res |= 0x2;
		if (encoded[2*pos+1]) res |= 0x1;
		return Decision(res);
	}



	// create a uniform distributing policy for N nodes
	SchedulingPolicy SchedulingPolicy::createUniform(int N, int extraDepth) {
		assert_lt(0,N);
		assert_le(0,extraDepth);
		return createBalanced(std::vector<float>(N,1.0),extraDepth);
	}

	// create a balanced work distribution based on the given load distribution
	SchedulingPolicy SchedulingPolicy::createBalanced(const std::vector<float>& loadDistribution, int extraDepth) {
		assert_lt(0,loadDistribution.size());
		assert_le(0,extraDepth);

		// get the number of nodes to be filled
		int N = loadDistribution.size();
		std::cout << "\tNumber of nodes: " << N << "\n";

		// compute number of levels to be scheduled
		auto log2 = ceilLog2(N);
		auto levels = log2 + extraDepth;
		std::cout << "\tNumber of task levels: " << levels << "\n";

		// create initial task to node mapping
		int numTasks = (1<<levels);
		std::vector<std::uint32_t> mapping = getEqualDistribution(N,numTasks);
		std::cout << "\tTask mapping: " << mapping << "\n";

		// balance load in mapping
		// TODO: implement

		// convert mapping in decision tree
		return toDecisionTree((1<<log2),mapping);
	}


	SchedulingPolicy SchedulingPolicy::load(allscale::utils::ArchiveReader&) {

	}

	void SchedulingPolicy::store(allscale::utils::ArchiveWriter&) const {

	}


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
