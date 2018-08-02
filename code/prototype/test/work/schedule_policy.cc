#include <gtest/gtest.h>

#include <type_traits>

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


	TEST(SchedulePolicy, Uniform) {

		SchedulingPolicy u = SchedulingPolicy::createUniform(3,1);

		// test paths:
		auto paths = getAll(3);
//		for(const auto& cur : paths) {
//			std::cout << cur << " => " << u.decide(cur) << "\n";
//		}

		for(const auto& cur : paths) {
			auto target = getTarget(3,u,cur);
			if (!target.isLeaf()) continue;
			std::cout << cur << " : " << getTarget(3,u,cur) << "\n";
		}



	}

	TEST(SchedulePolicy, Uniform_N3_deeper) {

		SchedulingPolicy u = SchedulingPolicy::createUniform(3,3);

		auto paths = getAll(6);
//		for(const auto& cur : paths) {
//			std::cout << cur << " => " << u.decide(cur) << "\n";
//		}

		for(const auto& cur : paths) {
			auto target = getTarget(3,u,cur);
			if (!target.isLeaf()) continue;
			std::cout << cur << " : " << getTarget(3,u,cur) << "\n";
		}


	}

} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale
