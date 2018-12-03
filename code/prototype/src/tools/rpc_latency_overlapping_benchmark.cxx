
#include <chrono>
#include <iostream>
#include <map>

#include "allscale/utils/assert.h"
#include "allscale/runtime/com/network.h"

using namespace allscale::runtime;
using namespace allscale::runtime::com;


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


int main() {

	using clock = std::chrono::high_resolution_clock;

	static constexpr int NUM_REPETITONS = 100;

	// create a network (implicit node creation)
	auto network = Network::create();
	assert_true(network);
	auto& net = *network;

	// finish setup
	net.installServiceOnNodes<MeasureService>();
	net.sync();

	net.runOn(0,[&](Node&){
		std::cout << "Starting overlapping round-trip time measurements using " << NUM_REPETITONS << " repetitions\n";
	});

	// measure round-trip time between all nodes
	for(com::rank_t i=0; i<net.numNodes(); i++) {
		net.runOn(i,[&](Node&){

			auto& net = Network::getNetwork();

			for(com::rank_t j=0; j<net.numNodes(); j++) {

				// get the remote procedure
				auto ping = net.getRemoteProcedure(j,&MeasureService::ping);

				std::vector<RemoteCallResult<int>> res;
				res.reserve(NUM_REPETITONS);

				// measure round trip time 10 times
				auto begin = clock::now();
				for(int i=0; i<NUM_REPETITONS; i++) {
					res.push_back(ping(i));
				}

				// wait for all to finish
				for(int i=0; i<NUM_REPETITONS; i++) {
					if (res[i].get() != i+1) {
						std::cout << "Corrupted communication by sending ping from " << i << " to " << j << " ...\n";
					}
				}
				auto end = clock::now();

				// compute average time
				auto time = std::chrono::duration_cast<std::chrono::microseconds>(end-begin).count() / NUM_REPETITONS;

				// print result
				std::cout << "Time " << i << " => " << j << " : " << time << "us\n";

				// save result
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

	// done
	return EXIT_SUCCESS;
}
