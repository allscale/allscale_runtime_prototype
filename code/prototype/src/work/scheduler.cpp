
#include "allscale/runtime/com/network.h"
#include "allscale/runtime/work/scheduler.h"
#include "allscale/runtime/work/task.h"

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
			auto& net = com::Node::getLocalNode().getNetwork();
			auto numNodes = net.numNodes();

			// the cut-off level for "forced" distribution
			return ceilLog2(numNodes) + 2;
		}

	}

	bool shouldSplit(const TaskPtr& t) {

		// should only be called for tasks that are actually splitable
		assert_true(t->isSplitable());

		// decide on the cut-off level
		return t->getId().getDepth() < getCutOffLevel();
	}


	/**
	 * An initial, simple scheduling function.
	 */
	com::rank_t getScheduleTarget(com::rank_t localRank, const TaskPtr& t) {

		// for very fine-grained tasks => process locally
		if (t->getId().getDepth() < getCutOffLevel()) return localRank;

		// for all others, determine a rank

		// get the network
		auto& net = com::Node::getLocalNode().getNetwork();
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

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
