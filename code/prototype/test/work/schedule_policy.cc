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

	TEST(DecisionTree, Traits) {
		EXPECT_TRUE(allscale::utils::is_serializable<DecisionTree>::value);
	}

	TEST(DecisionTree, Basic) {

		DecisionTree tree(8);

		TaskPath r = TaskPath::root();
		EXPECT_TRUE(r.isRoot());

		// the default should be Done
		EXPECT_EQ(Decision::Done,tree.get(r));

		tree.set(r,Decision::Stay);
		EXPECT_EQ(Decision::Stay,tree.get(r));

		tree.set(r,Decision::Left);
		EXPECT_EQ(Decision::Left,tree.get(r));

		tree.set(r,Decision::Right);
		EXPECT_EQ(Decision::Right,tree.get(r));

		tree.set(r,Decision::Done);
		EXPECT_EQ(Decision::Done,tree.get(r));

		// try also another position
		auto p = r.getLeftChildPath().getRightChildPath();

		// the default should be Done
		EXPECT_EQ(Decision::Done,tree.get(p));

		tree.set(p,Decision::Stay);
		EXPECT_EQ(Decision::Stay,tree.get(p));

		tree.set(p,Decision::Left);
		EXPECT_EQ(Decision::Left,tree.get(p));

		tree.set(p,Decision::Right);
		EXPECT_EQ(Decision::Right,tree.get(p));

		tree.set(p,Decision::Done);
		EXPECT_EQ(Decision::Done,tree.get(p));

		// something deeper
		p = p.getLeftChildPath().getRightChildPath();
		EXPECT_EQ(Decision::Done,tree.get(p));

	}

	TEST(ExchangeableSchedulingPolicy, Basic) {

		// type properties
		EXPECT_FALSE(std::is_default_constructible<ExchangeableSchedulingPolicy>::value);
		EXPECT_TRUE(std::is_destructible<ExchangeableSchedulingPolicy>::value);

		EXPECT_TRUE(std::is_copy_constructible<ExchangeableSchedulingPolicy>::value);
		EXPECT_TRUE(std::is_move_constructible<ExchangeableSchedulingPolicy>::value);
		EXPECT_TRUE(std::is_copy_assignable<ExchangeableSchedulingPolicy>::value);
		EXPECT_TRUE(std::is_move_assignable<ExchangeableSchedulingPolicy>::value);

		// also serializable
		EXPECT_TRUE(allscale::utils::is_serializable<ExchangeableSchedulingPolicy>::value);

	}


	TEST(DecisionTreeSchedulingPolicy, Basic) {

		// type properties
		EXPECT_FALSE(std::is_default_constructible<DecisionTreeSchedulingPolicy>::value);
		EXPECT_TRUE(std::is_destructible<DecisionTreeSchedulingPolicy>::value);

		EXPECT_TRUE(std::is_copy_constructible<DecisionTreeSchedulingPolicy>::value);
		EXPECT_TRUE(std::is_move_constructible<DecisionTreeSchedulingPolicy>::value);
		EXPECT_TRUE(std::is_copy_assignable<DecisionTreeSchedulingPolicy>::value);
		EXPECT_TRUE(std::is_move_assignable<DecisionTreeSchedulingPolicy>::value);

		// but not serializable ...
		EXPECT_FALSE(allscale::utils::is_serializable<DecisionTreeSchedulingPolicy>::value);

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


		com::HierarchyAddress traceTarget(int netSize, const DecisionTreeSchedulingPolicy& policy, const TaskPath& path) {
			// for roots it is easy
			if (path.isRoot()) return com::HierarchyAddress::getRootOfNetworkSize(netSize);

			// for everything else, we walk recursive
			auto res = traceTarget(netSize,policy,path.getParentPath());

			// simulate scheduling
			switch(policy.decide(res,path)) {
			case Decision::Done  : return res;
			case Decision::Stay  : return res;
			case Decision::Left  : return res.getLeftChild();
			case Decision::Right : return res.getRightChild();
			}
			assert_fail();
			return res;
		}

		com::HierarchyAddress getTarget(int netSize, const DecisionTreeSchedulingPolicy& policy, const TaskPath& path) {

			// trace current path
			auto res = traceTarget(netSize,policy,path);

			// check if this is a leaf-level node
			if (res.isLeaf()) return res.getRank();

			// otherwise, this task is not fully scheduled yet, but its child-tasks will be, and they should all end up at the same point
			std::vector<TaskPath> children;
			collectPaths(path,children,path.getLength());

			// retrieve position of all children, make sure they all reach the same rank
			com::rank_t initial = -1;
			com::rank_t pos = initial;
			for(const auto& cur : children) {
				if (cur.getLength() != path.getLength()*2) continue;
				auto childTarget = traceTarget(netSize,policy,cur);
				EXPECT_TRUE(childTarget.isLeaf()) << cur;
				if (pos == initial) pos = childTarget.getRank();
				else EXPECT_EQ(pos,childTarget.getRank()) << "Parent: " << path << ", Child: " << cur;
			}
			return pos;
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


	TEST(DecisionTreeSchedulingPolicy, UniformFixed) {

		constexpr int NUM_NODES = 3;
		constexpr int CEIL_LOG_2_NUM_NODES = ceilLog2(NUM_NODES);
		constexpr int GRANULARITY = 3;

		// get uniform distributed policy
		auto u = DecisionTreeSchedulingPolicy::createUniform(NUM_NODES,GRANULARITY);

//		std::cout << u << "\n";

		// get the list of all paths down to the given level
		auto max_length = std::max(CEIL_LOG_2_NUM_NODES,GRANULARITY);
		auto paths = getAll(max_length);

		// collect scheduling target on lowest level
		std::vector<com::rank_t> targets;
		for(const auto& cur : paths) {
			EXPECT_EQ(traceTarget(NUM_NODES,u,cur),u.getTarget(cur));
			if (cur.getLength() != max_length) continue;
			auto target = getTarget(NUM_NODES,u,cur);
			EXPECT_EQ(0,target.getLayer());
			targets.push_back(target.getRank());
		}

		EXPECT_EQ("[0,0,0,1,1,1,2,2]",toString(targets));

	}

	TEST(DecisionTreeSchedulingPolicy, UniformFixedCoarse) {

		constexpr int NUM_NODES = 3;
		constexpr int CEIL_LOG_2_NUM_NODES = ceilLog2(NUM_NODES);
		constexpr int GRANULARITY = 2;

		// get uniform distributed policy
		auto u = DecisionTreeSchedulingPolicy::createUniform(NUM_NODES,GRANULARITY);

//		std::cout << u << "\n";

		// get the list of all paths down to the given level
		auto max_length = std::max(CEIL_LOG_2_NUM_NODES,GRANULARITY);
		auto paths = getAll(max_length);

		// collect scheduling target on lowest level
		std::vector<com::rank_t> targets;
		for(const auto& cur : paths) {
			EXPECT_EQ(traceTarget(NUM_NODES,u,cur),u.getTarget(cur));
			if (cur.getLength() != max_length) continue;
			auto target = getTarget(NUM_NODES,u,cur);
			EXPECT_EQ(0,target.getLayer());
			targets.push_back(target.getRank());
		}

		EXPECT_EQ("[0,0,1,2]",toString(targets));

	}

	TEST(DecisionTreeSchedulingPolicy, UniformFixedFine) {

		constexpr int NUM_NODES = 3;
		constexpr int CEIL_LOG_2_NUM_NODES = ceilLog2(NUM_NODES);
		constexpr int GRANULARITY = 5;

		// get uniform distributed policy
		auto u = DecisionTreeSchedulingPolicy::createUniform(NUM_NODES,GRANULARITY);

//		std::cout << u << "\n";

		// get the list of all paths down to the given level
		auto max_length = std::max(CEIL_LOG_2_NUM_NODES,GRANULARITY);
		auto paths = getAll(max_length);

		// collect scheduling target on lowest level
		std::vector<com::rank_t> targets;
		for(const auto& cur : paths) {
			EXPECT_EQ(traceTarget(NUM_NODES,u,cur),u.getTarget(cur));
			if (cur.getLength() != max_length) continue;
			auto target = getTarget(NUM_NODES,u,cur);
			EXPECT_EQ(0,target.getLayer());
			targets.push_back(target.getRank());
		}

		EXPECT_EQ("[0,0,0,0,0,0,0,0,0,0,0,1,1,1,1,1,1,1,1,1,1,1,2,2,2,2,2,2,2,2,2,2]",toString(targets));

	}


	TEST(DecisionTreeSchedulingPolicy, Uniform_N3_deeper) {

		// check larger combination of nodes and extra levels
		for(int n=1; n<16; n++) {
			for(int e = 1; e<=3; e++) {

				SCOPED_TRACE("n=" + toString(n) + ",e=" + toString(e));

				int NUM_NODES = n;
				int CEIL_LOG_2_NUM_NODES = ceilLog2(n);
				int GRANULARITY = CEIL_LOG_2_NUM_NODES + e;

				// get uniform distributed policy
				auto u = DecisionTreeSchedulingPolicy::createUniform(NUM_NODES,GRANULARITY);

				// get the list of all paths down to the given level
				auto max_length = std::max(CEIL_LOG_2_NUM_NODES,GRANULARITY);
				auto paths = getAll(max_length);

				// collect scheduling target on lowest level
				std::vector<com::rank_t> targets;
				for(const auto& cur : paths) {
					EXPECT_EQ(traceTarget(NUM_NODES,u,cur),u.getTarget(cur));
					if (cur.getLength() != max_length) continue;
					auto target = getTarget(NUM_NODES,u,cur);
					EXPECT_EQ(0,target.getLayer()) << cur;
					targets.push_back(target.getRank());
				}

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


	namespace {

		// simulates the scheduling processes within the actual task scheduler
		com::HierarchyAddress traceIndirectTarget(const DecisionTreeSchedulingPolicy& policy, const com::HierarchyAddress& cur, const TaskPath& path) {

			// if current node is not involved, forward to parent
			if (!policy.isInvolved(cur,path)) return traceIndirectTarget(policy,cur.getParent(),path);

			// the root node should be at the root location
			if (path.isRoot()) return cur;

			// get location of parent task
			auto parentLoc = traceIndirectTarget(policy,cur,path.getParentPath());

			// check the correctness of this tracer code
			EXPECT_EQ(parentLoc,policy.getTarget(path.getParentPath()));
			EXPECT_TRUE(policy.isInvolved(parentLoc,path));

			// compute where the parent has send this task
			switch(policy.decide(parentLoc,path)) {
			case Decision::Stay: {
				return parentLoc;
			}
			case Decision::Left: {
				EXPECT_FALSE(parentLoc.isLeaf());
				return parentLoc.getLeftChild();
			}
			case Decision::Right: {
				EXPECT_FALSE(parentLoc.isLeaf());
				return parentLoc.getRightChild();
			}
			case Decision::Done:
				EXPECT_TRUE(parentLoc.isLeaf());
				return cur;
			}

			assert_fail() << "Invalid decision!";
			return {};
		}

		template<typename Op>
		void forAllChildren(const com::HierarchyAddress& addr, const Op& op) {
			op(addr);
			if (addr.isLeaf()) return;
			forAllChildren(addr.getLeftChild(),op);
			forAllChildren(addr.getRightChild(),op);
		}

		void testAllSources(const DecisionTreeSchedulingPolicy& policy, const std::string& trg, const TaskPath& path) {
			forAllChildren(policy.getPresumedRootAddress(),[&](const com::HierarchyAddress& cur){
				EXPECT_EQ(trg,toString(traceIndirectTarget(policy,cur,path))) << "Origin: " << cur << "\nPath: " << path << "\n";
			});
		}

		void testAllSources(const DecisionTreeSchedulingPolicy& policy, const com::HierarchyAddress& trg, const TaskPath& path) {
			testAllSources(policy,toString(trg),path);
		}
	}

	TEST(DecisionTreeSchedulingPolicy,Redirect) {

		// start from wrong positions and see whether target can be located successfully

		for(int num_nodes=1; num_nodes<=10; num_nodes++) {

			// get a uniform distribution
			auto policy = DecisionTreeSchedulingPolicy::createUniform(num_nodes);

//			std::cout << "N=" << num_nodes << "\n" << policy << "\n";

			// test that all paths can reach their destination starting from any location
			for(const auto& path : getAll(8)) {
				testAllSources(policy,policy.getTarget(path),path);
			}

		}
	}

	TEST(DecisionTreeSchedulingPolicy, Rebalancing) {

		auto u = DecisionTreeSchedulingPolicy::createUniform(4,5);

		// providing a nicely balanced load should not cause any changes
		auto loadDist = std::vector<float>(4,1.0);
		auto b1 = DecisionTreeSchedulingPolicy::createReBalanced(u,loadDist);
		EXPECT_EQ(u.getTaskDistributionMapping(),b1.getTaskDistributionMapping());

		// alter the distribution
		loadDist[1] = 3;		// node 1 has 3x more load
		loadDist[3] = 2;		// node 3 has 2x more load
		auto b2 = DecisionTreeSchedulingPolicy::createReBalanced(u,loadDist);
		EXPECT_NE(u.getTaskDistributionMapping(),b2.getTaskDistributionMapping());


		// something more homogeneous
		loadDist[0] = 1.25;
		loadDist[1] = 1.5;
		loadDist[2] = 1.25;
		loadDist[3] = 2;
		auto b3 = DecisionTreeSchedulingPolicy::createReBalanced(u,loadDist);
		EXPECT_NE(u.getTaskDistributionMapping(),b3.getTaskDistributionMapping());


		// something pretty even
		loadDist[0] = 1.05;
		loadDist[1] = 0.98;
		loadDist[2] = 0.99;
		loadDist[3] = 1.04;
		auto b4 = DecisionTreeSchedulingPolicy::createReBalanced(u,loadDist);
		EXPECT_EQ(u.getTaskDistributionMapping(),b4.getTaskDistributionMapping());



		// test zero-load value
		loadDist[0] = 1.05;
		loadDist[1] = 0;
		loadDist[2] = 0.99;
		loadDist[3] = 1.04;
		auto b5 = DecisionTreeSchedulingPolicy::createReBalanced(u,loadDist);
		EXPECT_NE(u.getTaskDistributionMapping(),b5.getTaskDistributionMapping());

	}


	TEST(DecisionTreeSchedulingPolicy, Resizing) {

		auto u = DecisionTreeSchedulingPolicy::createUniform(4,5);

		// providing a nicely balanced load should not cause any changes
		auto loadDist = std::vector<float>(4,1.0);
		auto mask = std::vector<bool>(4,true);
		auto b1 = DecisionTreeSchedulingPolicy::createReBalanced(u,loadDist,mask);
		EXPECT_EQ(u.getTaskDistributionMapping(),b1.getTaskDistributionMapping());

		// remove node 2
		mask[2] = false;
		auto b2 = DecisionTreeSchedulingPolicy::createReBalanced(u,loadDist,mask);
		EXPECT_EQ("[0,0,0,0,0,0,0,0,0,0,0,1,1,1,1,1,1,1,1,1,1,3,3,3,3,3,3,3,3,3,3,3]",toString(b2.getTaskDistributionMapping()));

		// re-enable node 2
		loadDist[2] = 0;
		mask[2] = true;
		auto b3 = DecisionTreeSchedulingPolicy::createReBalanced(b2,loadDist,mask);
		EXPECT_EQ(u.getTaskDistributionMapping(),b3.getTaskDistributionMapping());

	}


	TEST(DecisionTreeSchedulingPolicy, ResizingDownToZero) {

		auto u = DecisionTreeSchedulingPolicy::createUniform(6,5);

		// providing a nicely balanced load should not cause any changes
		auto loadDist = std::vector<float>(6,1.0);
		auto mask = std::vector<bool>(6,true);
		auto b1 = DecisionTreeSchedulingPolicy::createReBalanced(u,loadDist,mask);
		EXPECT_EQ(u.getTaskDistributionMapping(),b1.getTaskDistributionMapping());

		// remove node 2
		mask[2] = false;
		auto b2 = DecisionTreeSchedulingPolicy::createReBalanced(u,loadDist,mask);
		EXPECT_EQ("[0,0,0,0,0,0,0,1,1,1,1,1,1,1,3,3,3,3,3,3,4,4,4,4,4,4,5,5,5,5,5,5]",toString(b2.getTaskDistributionMapping()));

		// remove node 4
		loadDist[2] = 0;
		mask[4] = false;
		auto b3 = DecisionTreeSchedulingPolicy::createReBalanced(b2,loadDist,mask);
		EXPECT_EQ("[0,0,0,0,0,0,0,0,0,1,1,1,1,1,1,1,1,3,3,3,3,3,3,3,5,5,5,5,5,5,5,5]",toString(b3.getTaskDistributionMapping()));

		// remove node 1
		loadDist[4] = 0;
		mask[1] = false;
		auto b4 = DecisionTreeSchedulingPolicy::createReBalanced(b3,loadDist,mask);
		EXPECT_EQ("[0,0,0,0,0,0,0,0,0,0,0,0,3,3,3,3,3,3,3,3,3,3,5,5,5,5,5,5,5,5,5,5]",toString(b4.getTaskDistributionMapping()));

		// remove node 5
		loadDist[1] = 0;
		mask[5] = false;
		auto b5 = DecisionTreeSchedulingPolicy::createReBalanced(b4,loadDist,mask);
		EXPECT_EQ("[0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3]",toString(b5.getTaskDistributionMapping()));

		// remove node 0
		loadDist[5] = 0;
		mask[0] = false;
		auto b6 = DecisionTreeSchedulingPolicy::createReBalanced(b5,loadDist,mask);
		EXPECT_EQ("[3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3]",toString(b6.getTaskDistributionMapping()));
	}


	TEST(DecisionTreeSchedulingPolicy, Scaling) {

		int N = 200;

		// create a policy for N nodes
		auto policy = DecisionTreeSchedulingPolicy::createUniform(N);

		std::cout << policy.getTaskDistributionMapping() << "\n";

	}

	TEST(DecisionTreeSchedulingPolicy, Scaling_Rebalancing) {

		int N = 200;

		// create a policy for N nodes
		auto u = DecisionTreeSchedulingPolicy::createUniform(N);

		std::vector<float> load(N,1.0);
		auto b = DecisionTreeSchedulingPolicy::createReBalanced(u,load);

		EXPECT_EQ(u.getTaskDistributionMapping(),b.getTaskDistributionMapping());

	}


} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale
