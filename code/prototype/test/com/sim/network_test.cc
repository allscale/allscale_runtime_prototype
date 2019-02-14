#include <gtest/gtest.h>

#include "allscale/runtime/com/sim/network.h"


namespace allscale {
namespace runtime {
namespace com {
namespace sim {

	/**
	 * A simple ping service.
	 */
	struct PingService {

		PingService(com::Node&) {}

		int ping(int x) { return x + 1; };

	};


	TEST(Network, Creation) {
		// just see that it can be instantiated
		Network net;
		EXPECT_EQ(1,net.numNodes());

		// also check that larger networks can be created
		Network net2(4);
		EXPECT_EQ(4,net2.numNodes());
	}

	TEST(Network, RunOperation) {
		// create a network with a two nodes
		Network net(2);
		EXPECT_EQ(2,net.numNodes());

		// run an operation on node 0
		net.runOn(0,[&](Node& node){
			EXPECT_EQ(0,node.getRank());
		});

		// run an operation on node 1
		net.runOn(1,[&](Node& node){
			EXPECT_EQ(1,node.getRank());
		});
	}

	TEST(Network, Ping) {

		// create a network with a two nodes
		Network net(2);
		EXPECT_EQ(2,net.numNodes());

		// install ping service
		net.installServiceOnNodes<PingService>();

		// run an operation on node 0
		net.runOn(0,[](Node&){
			auto& net = Network::getNetwork();
			auto ping = net.getRemoteProcedure(1,&PingService::ping);
			EXPECT_EQ(5,ping(4).get());
		});

	}

	TEST(Network, Stats) {

		// this test only works in a non-MPI setup
		#ifdef ENABLE_MPI
				return;
		#endif

		// create a network with a two nodes
		Network net(2);
		EXPECT_EQ(2,net.numNodes());

		// install ping service
		net.installServiceOnNodes<PingService>();

		// extract references to statistics
		{
			auto stats = net.getStatistics();
			auto stats0 = stats[0];
			auto stats1 = stats[1];

			EXPECT_EQ(0,stats0.received_bytes);
			EXPECT_EQ(0,stats0.sent_bytes);
			EXPECT_EQ(0,stats0.received_calls);
			EXPECT_EQ(0,stats0.sent_calls);

			EXPECT_EQ(0,stats1.received_bytes);
			EXPECT_EQ(0,stats1.sent_bytes);
			EXPECT_EQ(0,stats1.received_calls);
			EXPECT_EQ(0,stats1.sent_calls);
		}

		// send a message from 0 to 1
		net.runOn(0,[](Node&){
			auto& net = Network::getNetwork();
			auto ping = net.getRemoteProcedure(1,&PingService::ping);
			EXPECT_EQ(5,ping(4).get());
		});

		// extract references to statistics
		{
			auto stats = net.getStatistics();
			auto stats0 = stats[0];
			auto stats1 = stats[1];

			EXPECT_EQ(4,stats0.received_bytes);
			EXPECT_EQ(4,stats0.sent_bytes);
			EXPECT_EQ(0,stats0.received_calls);
			EXPECT_EQ(1,stats0.sent_calls);

			EXPECT_EQ(4,stats1.received_bytes);
			EXPECT_EQ(4,stats1.sent_bytes);
			EXPECT_EQ(1,stats1.received_calls);
			EXPECT_EQ(0,stats1.sent_calls);
		}


		// send two messages from 1 to 0
		net.runOn(1,[](Node&){
			auto& net = Network::getNetwork();
			auto ping = net.getRemoteProcedure(0,&PingService::ping);
			EXPECT_EQ(5,ping(4).get());
			EXPECT_EQ(9,ping(8).get());
		});

		// extract references to statistics
		{
			auto stats = net.getStatistics();
			auto stats0 = stats[0];
			auto stats1 = stats[1];

			EXPECT_EQ(12,stats0.received_bytes);
			EXPECT_EQ(12,stats0.sent_bytes);
			EXPECT_EQ(2,stats0.received_calls);
			EXPECT_EQ(1,stats0.sent_calls);

			EXPECT_EQ(12,stats1.received_bytes);
			EXPECT_EQ(12,stats1.sent_bytes);
			EXPECT_EQ(1,stats1.received_calls);
			EXPECT_EQ(2,stats1.sent_calls);
		}

		// reset the network statistics
		net.resetStatistics();

		// extract references to statistics
		{
			auto stats = net.getStatistics();
			auto stats0 = stats[0];
			auto stats1 = stats[1];

			EXPECT_EQ(0,stats0.received_bytes);
			EXPECT_EQ(0,stats0.sent_bytes);
			EXPECT_EQ(0,stats0.received_calls);
			EXPECT_EQ(0,stats0.sent_calls);

			EXPECT_EQ(0,stats1.received_bytes);
			EXPECT_EQ(0,stats1.sent_bytes);
			EXPECT_EQ(0,stats1.received_calls);
			EXPECT_EQ(0,stats1.sent_calls);
		}

	}

} // end of namespace sim
} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
