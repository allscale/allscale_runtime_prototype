#include <gtest/gtest.h>

#include "allscale/runtime/com/network.h"
#include "allscale/runtime/work/treeture.h"


namespace allscale {
namespace runtime {
namespace work {

	template<typename Op>
	void testInNode(const Op& op) {

		// get some network of any size
		auto network = com::Network::create();
		assert_true(network);

		auto& net = *network;
		net.installServiceOnNodes<TreetureStateService>();

		net.runOn(0,[&](com::Node&){
			op();
		});
	}

	TEST(Treeture, Serializable) {

		EXPECT_TRUE(allscale::utils::is_serializable<treeture<void>>::value);
		EXPECT_TRUE(allscale::utils::is_serializable<treeture<int>>::value);
	}

	TEST(Treeture, Processing) {

		testInNode([]{

			treeture<int> x;
			EXPECT_FALSE(x.valid());

			auto rank = com::Node::getLocalRank();

			// simulate the creation of a task
			TaskID id(12);
			TreetureStateService::getLocal().registerTask<int>(id);

			// and the attempt to get the result
			treeture<int> y(rank,id);
			EXPECT_TRUE(y.valid());
			EXPECT_FALSE(y.isDone());

			// simulate completion of task
			TreetureStateService::getLocal().setDone<int>(rank,id,14);
			EXPECT_TRUE(y.valid());
			EXPECT_TRUE(y.isDone());
			EXPECT_TRUE(y.valid());

			EXPECT_EQ(14,y.get_result());
		});

	}


	TEST(Treeture, SerializationNotOwning) {
		TaskID id(12);
		treeture<int> x;

		EXPECT_FALSE(x.valid());

		auto a = allscale::utils::serialize(x);
		treeture<int> y = allscale::utils::deserialize<treeture<int>>(a);

		EXPECT_FALSE(y.valid());
		EXPECT_FALSE(y.valid());
	}

	TEST(Treeture, SerializationOwning) {

		testInNode([]{

			// simulate the creation of a task
			TaskID id = getFreshId();
			TreetureStateService::getLocal().registerTask<int>(id);

			auto rank = com::Node::getLocalRank();

			// and the attempt to get the result
			treeture<int> x(rank,id);
			EXPECT_TRUE(x.valid());
			EXPECT_FALSE(x.isDone());

			// simulate transfere of task
			auto a = allscale::utils::serialize(x);

			EXPECT_FALSE(x.valid());

			treeture<int> y = allscale::utils::deserialize<treeture<int>>(a);

			EXPECT_FALSE(x.valid());
			EXPECT_TRUE(y.valid());


			// simulate completion of treeture
			TreetureStateService::getLocal().setDone<int>(rank,id,14);
			EXPECT_TRUE(y.valid());
			EXPECT_TRUE(y.isDone());
			EXPECT_TRUE(y.valid());

			EXPECT_EQ(14,y.get_result());

		});
	}

	TEST(Treeture, SerializationDone) {

		testInNode([]{

			// simulate the creation of a task
			TaskID id = getFreshId();
			TreetureStateService::getLocal().registerTask<int>(id);

			auto rank = com::Node::getLocalRank();

			// and the attempt to get the result
			treeture<int> x(rank,id);
			EXPECT_TRUE(x.valid());
			EXPECT_FALSE(x.isDone());

			// simulate completion of task
			TreetureStateService::getLocal().setDone<int>(rank,id,14);
			EXPECT_TRUE(x.valid());
			EXPECT_TRUE(x.isDone());
			EXPECT_TRUE(x.valid());

			EXPECT_EQ(14,x.get_result());

			// simulate transfer
			auto a = allscale::utils::serialize(x);

			// since the result is already here, no reason to invalidate x
			EXPECT_TRUE(x.valid());

			treeture<int> y = allscale::utils::deserialize<treeture<int>>(a);

			EXPECT_TRUE(x.valid());
			EXPECT_TRUE(y.valid());
			EXPECT_TRUE(y.isDone());
			EXPECT_EQ(14,y.get_result());

		});
	}



	TEST(Treeture_Void, Processing) {

		testInNode([]{

			treeture<void> x;
			EXPECT_FALSE(x.valid());

			auto rank = com::Node::getLocalRank();

			// simulate the creation of a task
			TaskID id = getFreshId();
			TreetureStateService::getLocal().registerTask<void>(id);

			// and the attempt to get the result
			treeture<void> y(rank,id);
			EXPECT_TRUE(y.valid());
			EXPECT_FALSE(y.isDone());

			// simulate completion of task
			TreetureStateService::getLocal().setDone(rank,id);
			EXPECT_TRUE(y.valid());
			EXPECT_TRUE(y.isDone());
			EXPECT_TRUE(y.valid());
		});

	}


	TEST(Treeture_Void, SerializationNotOwning) {
		treeture<void> x;

		EXPECT_FALSE(x.valid());

		auto a = allscale::utils::serialize(x);
		treeture<void> y = allscale::utils::deserialize<treeture<void>>(a);

		EXPECT_FALSE(y.valid());
		EXPECT_FALSE(y.valid());
	}

	TEST(Treeture_Void, SerializationOwning) {

		testInNode([]{

			// simulate the creation of a task
			TaskID id = getFreshId();
			TreetureStateService::getLocal().registerTask<void>(id);

			auto rank = com::Node::getLocalRank();

			// and the attempt to get the result
			treeture<void> x(rank,id);
			EXPECT_TRUE(x.valid());
			EXPECT_FALSE(x.isDone());

			// simulate transfere of task
			auto a = allscale::utils::serialize(x);

			// the original does not need to be invalidted
			EXPECT_TRUE(x.valid());

			treeture<void> y = allscale::utils::deserialize<treeture<void>>(a);

			EXPECT_TRUE(x.valid());
			EXPECT_TRUE(y.valid());


			// simulate completion of treeture
			TreetureStateService::getLocal().setDone(rank,id);
			EXPECT_TRUE(y.valid());
			EXPECT_TRUE(y.isDone());
			EXPECT_TRUE(y.valid());

		});
	}

	TEST(Treeture_Void, SerializationDone) {

		testInNode([]{

			// simulate the creation of a task
			TaskID id = getFreshId();
			TreetureStateService::getLocal().registerTask<void>(id);

			auto rank = com::Node::getLocalRank();

			// and the attempt to get the result
			treeture<void> x(rank,id);
			EXPECT_TRUE(x.valid());
			EXPECT_FALSE(x.isDone());

			// simulate completion of task
			TreetureStateService::getLocal().setDone(rank,id);
			EXPECT_TRUE(x.valid());
			EXPECT_TRUE(x.isDone());
			EXPECT_TRUE(x.valid());

			// simulate transfer
			auto a = allscale::utils::serialize(x);

			// since the result is already here, no reason to invalidate x
			EXPECT_TRUE(x.valid());

			treeture<void> y = allscale::utils::deserialize<treeture<void>>(a);

			EXPECT_TRUE(x.valid());
			EXPECT_TRUE(y.valid());
			EXPECT_TRUE(y.isDone());

		});
	}


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
