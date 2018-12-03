#include <gtest/gtest.h>

#ifndef ENABLE_MPI

	TEST(Dummy,NoMPI) {}

#else
#include "allscale/runtime/com/mpi/network.h"


namespace allscale {
namespace runtime {
namespace com {
namespace mpi {

	/**
	 * A simple ping service.
	 */
	struct PingService {

		PingService(com::Node&) {}

		int ping(int x) { return x + 1; };

	};


	TEST(Network, Retrieval) {
		// just see that it can be instantiated
		EXPECT_TRUE(Network::create());

		Network& net = Network::getNetwork();
		EXPECT_LE(1,net.numNodes());

		bool isPresent = false;
		net.runOn(0,[&](com::Node& cur){
			isPresent = true;
			EXPECT_EQ(0,cur.getRank());
			EXPECT_EQ(0,Node::getLocalRank());
			EXPECT_EQ(&net,&Network::getNetwork());
		});

		int counter = 0;
		net.runOnAll([&](com::Node& cur){
			counter++;
			EXPECT_EQ(Node::getLocalRank(),cur.getRank());
			EXPECT_EQ(&net,&Network::getNetwork());
		});

		// it is only run locally
		EXPECT_EQ(1,counter);
	}


	TEST(Network, RunOperation) {
		// retrieve the network
		auto network = Network::create();
		Network& net = *network;

		for(rank_t i=0; i<net.numNodes(); i++) {

			// run an operation on node i
			net.runOn(i,[&](Node& node){
				EXPECT_EQ(i,node.getRank());
			});

		}
	}

	TEST(Network, Ping) {

		// retrieve the network
		auto network = Network::create();
		ASSERT_TRUE(network);
		Network& net = *network;

		if(net.numNodes() < 2) {
			std::cout << "WARNING: test can only be conducted with 2 or more nodes.\n";
			return;
		}

		// install ping service
		net.installServiceOnNodes<PingService>();

		// barrier at the end of the setup
		net.sync();

		// run an operation on node 0
		net.runOn(0,[](Node&){
			auto& net = Network::getNetwork();
			auto ping = net.getRemoteProcedure(1,&PingService::ping);
			EXPECT_EQ(5,ping(4).get());
		});

		// barrier before removing services
		net.sync();

		// remove the service
		net.removeServiceOnNodes<PingService>();

	}

} // end of namespace mpi
} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale

#endif
