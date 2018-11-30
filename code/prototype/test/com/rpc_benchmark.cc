#include <gtest/gtest.h>

#include <chrono>
#include "allscale/runtime/com/network.h"


namespace allscale {
namespace runtime {
namespace com {

	struct MeasureService {

		MeasureService(Node&) {}

		// measured latency times
		std::map<std::pair<com::rank_t,com::rank_t>,uint64_t> times;

		int ping(int x) {
			return x+1;
		}

		void registerResult(com::rank_t from, com::rank_t to, uint64_t time) {
			times[{from,to}] = time;
		}

	};


	TEST(RPC, LattencyBenchmarkSequential) {

		using clock = std::chrono::high_resolution_clock;

		static constexpr int NUM_REPETITONS = 10;

		// create a network (implicit node creation)
		auto network = Network::create();
		ASSERT_TRUE(network);
		auto& net = *network;

		// check that network is ready
		if (net.numNodes() < 2) {
			std::cout << "WARNING: unable to benchmark RPC calls with less than 2 nodes.";
			return;
		}


		// finish setup
		net.installServiceOnNodes<MeasureService>();
		net.sync();

		// measure round-trip time between all nodes
		for(com::rank_t i=0; i<net.numNodes(); i++) {
			net.runOn(i,[&](Node&){

				auto& net = Network::getNetwork();

				for(com::rank_t j=0; j<net.numNodes(); j++) {

					// get the remote procedure
					auto ping = net.getRemoteProcedure(j,&MeasureService::ping);

					// measure round trip time 10 times
					auto begin = clock::now();
					for(int i=0; i<NUM_REPETITONS; i++) {
						EXPECT_EQ(i+1,ping(i));
					}
					auto end = clock::now();

					// compute average time
					auto time = std::chrono::duration_cast<std::chrono::microseconds>(end-begin).count() / NUM_REPETITONS;

					// print result
					std::cout << "Time: " << i << " => " << j << " : " << time << "us\n";
					net.getRemoteProcedure(0,&MeasureService::registerResult)(i,j,time);
				}

			});

			net.sync();
		}

		net.sync();

		// print result
		net.runOn(0,[&](Node& node){

			auto& net = Network::getNetwork();
			auto& s = node.getLocalService<MeasureService>();

			std::cout << "\nRound-trip times in us:\n";
			for(com::rank_t i=0; i<net.numNodes(); i++) {
				for(com::rank_t j=0; j<net.numNodes(); j++) {
					std::cout << s.times[{i,j}] << "\t";
				}
				std::cout << "\n";
			}


		});

	}


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
