#include <gtest/gtest.h>

#include <type_traits>

#include "allscale/utils/string_utils.h"
#include "allscale/utils/printer/vectors.h"

#include "allscale/runtime/com/node.h"
#include "allscale/runtime/com/hierarchy.h"
#include "allscale/runtime/work/schedule_policy.h"

namespace allscale {
namespace runtime {
namespace work {

	TEST(DecisionTree, Basic) {

		DecisionTree tree(8);

		TaskPath r = TaskPath::root();
		EXPECT_TRUE(r.isRoot());

		tree.set(r,Decision::Stay);
		EXPECT_EQ(Decision::Stay,tree.get(r));

		tree.set(r,Decision::Left);
		EXPECT_EQ(Decision::Left,tree.get(r));

		tree.set(r,Decision::Right);
		EXPECT_EQ(Decision::Right,tree.get(r));

		// try also another position
		auto p = r.getLeftChildPath().getRightChildPath();

		// default should be stay
		EXPECT_EQ(Decision::Stay,tree.get(p));

		tree.set(p,Decision::Stay);
		EXPECT_EQ(Decision::Stay,tree.get(p));

		tree.set(p,Decision::Left);
		EXPECT_EQ(Decision::Left,tree.get(p));

		tree.set(p,Decision::Right);
		EXPECT_EQ(Decision::Right,tree.get(p));

		// something deeper
		p = p.getLeftChildPath().getRightChildPath();
		EXPECT_EQ(Decision::Stay,tree.get(p));

	}

	TEST(SchedulePolicy, Basic) {

		// type properties
		EXPECT_FALSE(std::is_default_constructible<SchedulingPolicy>::value);
		EXPECT_TRUE(std::is_destructible<SchedulingPolicy>::value);

		EXPECT_TRUE(std::is_copy_constructible<SchedulingPolicy>::value);
		EXPECT_TRUE(std::is_move_constructible<SchedulingPolicy>::value);
		EXPECT_TRUE(std::is_copy_assignable<SchedulingPolicy>::value);
		EXPECT_TRUE(std::is_move_assignable<SchedulingPolicy>::value);

		// also serializable
		EXPECT_TRUE(allscale::utils::is_serializable<SchedulingPolicy>::value);

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


		com::HierarchyAddress getTarget(int netSize, const SchedulingPolicy& policy, const TaskPath& path) {
			// for roots it is easy
			if (path.isRoot()) return com::HierarchyAddress::getRootOfNetworkSize(netSize);

			// for everything else, we walk recursive
			auto res = getTarget(netSize,policy,path.getParentPath());

			// test whether there is any deeper level
			if (res.isLeaf()) return res;

			// simulate scheduling
			switch(policy.decide(path)) {
			case Decision::Stay  : return res;
			case Decision::Left  : return res.getLeftChild();
			case Decision::Right : return res.getRightChild();
			}
			assert_fail();
			return res;
		}

	}


	namespace {

		constexpr int ceilLog2(int x) {
			int i = 0;
			int c = 1;
			while (c<x) {
				c = c << 1;
				i++;
			}
			return i;
		}

	}


	TEST(SchedulePolicy, UniformFixed) {

		constexpr int NUM_NODES = 3;
		constexpr int CEIL_LOG_2_NUM_NODES = ceilLog2(NUM_NODES);
		constexpr int EXTRA_LEVELS = 1;

		// get uniform distributed policy
		auto u = SchedulingPolicy::createUniform(NUM_NODES,EXTRA_LEVELS);

		// get the list of all paths down to the given level
		auto max_length = CEIL_LOG_2_NUM_NODES + EXTRA_LEVELS;
		auto paths = getAll(max_length);

		// collect scheduling target on lowest level
		std::vector<com::rank_t> targets;
		for(const auto& cur : paths) {
			if (cur.getLength() != max_length) continue;
			auto target = getTarget(NUM_NODES,u,cur);
			EXPECT_EQ(0,target.getLayer());
			targets.push_back(target.getRank());
		}

		EXPECT_EQ("[0,0,0,1,1,1,2,2]",toString(targets));

	}


	TEST(DISABLED_SchedulePolicy, Uniform_N3_deeper) {

		// check larger combination of nodes and extra levels
		for(int n=1; n<16; n++) {
			for(int e = 1; e<=3; e++) {

				SCOPED_TRACE("n=" + toString(n) + ",e=" + toString(e));

				int NUM_NODES = n;
				int CEIL_LOG_2_NUM_NODES = ceilLog2(n);
				int EXTRA_LEVELS = CEIL_LOG_2_NUM_NODES+1; //e;

				// get uniform distributed policy
				auto u = SchedulingPolicy::createUniform(NUM_NODES,EXTRA_LEVELS);

				std::cout << u << "\n";

				// get the list of all paths down to the given level
				auto max_length = CEIL_LOG_2_NUM_NODES + EXTRA_LEVELS;
				auto paths = getAll(max_length);

				// collect scheduling target on lowest level
				std::vector<com::rank_t> targets;
				for(const auto& cur : paths) {
					if (cur.getLength() != max_length) continue;
					auto target = getTarget(NUM_NODES,u,cur);
					EXPECT_EQ(0,target.getLayer()) << cur;
					targets.push_back(target.getRank());
				}

				std::cout << toString(targets) << "\n";

				// check number of entries
				EXPECT_EQ((1<<max_length),targets.size());

				// check that ranks are in range
				for(const auto& cur : targets) {
					EXPECT_LE(0,cur);
					EXPECT_LT(cur,n);
				}

				// check that ranks are growing monotone
				for(std::size_t i=0; i<targets.size()-1; i++) {
					EXPECT_LE(targets[i],targets[i+1]);
				}

				// compute a histogram
				std::vector<std::size_t> hist(n,0);
				for(const auto& cur : targets) {
					hist[cur]++;
				}

				// expect distribution +/-1
				auto share = targets.size() / n;
				for(int i=0; i<n; i++) {
					EXPECT_TRUE(hist[i]==share || hist[i] == share+1)
							<< "Node:   " << i << "\n"
							<< "Share:  " << share << "\n"
							<< "Actual: " << hist[i] << "\n";
				}


			}
		}


	}

} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale