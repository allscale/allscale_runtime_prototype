
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
			assert_eq((1<<log2),map.size()) << "Input map is not of power-of-two size: " << map.size();

			// the resulting (unbalanced) tree has to be at most 2x as deep
			DecisionTree res((1<<(2*log2)));

			// fill in decisions recursively
			fillTree(res,TaskPath::root(), 0, ceilLog2(num_nodes), 0, num_nodes, &*map.begin(),&*map.end());

			// done
			return std::move(res);
		}

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
		assert_lt(pos/4,encoded.size());
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

		if (byte_pos > encoded.size()) return Decision::Done;

		// extract encoded value
		return Decision((encoded[byte_pos] >> bit_pos) & 0x3);
	}

	DecisionTree DecisionTree::load(allscale::utils::ArchiveReader& in) {
		return in.read<std::vector<uint8_t>>();
	}

	void DecisionTree::store(allscale::utils::ArchiveWriter& out) const {
		out.write(encoded);
	}

	namespace {

		void collectPaths(const TaskPath& cur, std::vector<TaskPath>& res, int depth) {
			if (depth < 0) return;
			res.push_back(cur);
			collectPaths(cur.getLeftChildPath(),res,depth-1);
			collectPaths(cur.getRightChildPath(),res,depth-1);
		}

		std::vector<TaskPath> getAll(int depth) {
			std::vector<TaskPath> res;
			collectPaths(TaskPath::root(),res,depth);
			return res;
		}

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
		return toDecisionTree((1<<log2),mapping);
	}




	// create a balanced work distribution based on the given load distribution
	SchedulingPolicy SchedulingPolicy::createReBalanced(const SchedulingPolicy& p, const std::vector<float>&) {
		assert_not_implemented();
		return p;
	}


	com::HierarchyAddress SchedulingPolicy::getTarget(const com::HierarchyAddress& root, const TaskPath& path) const {
		// for roots it is easy
		if (path.isRoot()) return root;

		// for everything else, we walk recursive
		auto res = getTarget(root,path.getParentPath());

		// simulate scheduling
		switch(decide(path)) {
		case Decision::Done  : return res;
		case Decision::Stay  : return res;
		case Decision::Left  : return res.getLeftChild();
		case Decision::Right : return res.getRightChild();
		}
		assert_fail();
		return res;
	}

	SchedulingPolicy SchedulingPolicy::load(allscale::utils::ArchiveReader& in) {
		return in.read<DecisionTree>();
	}

	void SchedulingPolicy::store(allscale::utils::ArchiveWriter& out) const {
		out.write(tree);
	}


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
