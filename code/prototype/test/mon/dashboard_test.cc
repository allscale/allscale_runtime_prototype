#include <gtest/gtest.h>

#include <string>

#include "allscale/utils/printer/vectors.h"

#include "allscale/runtime/com/network.h"
#include "allscale/runtime/mon/dashboard.h"

namespace allscale {
namespace runtime {
namespace mon {

	TEST(Dashboard, Basic) {

		// create a network
		auto network = com::Network::create();
		auto& net = *network;

		installDashbordService(net);

		// sync after setup
		net.sync();

		// retrieve network state information
		net.runOn(0,[&](com::Node&){
			auto state = getSystemState(com::Network::getNetwork());
		});

		sleep(3);

		// retrieve network state information
		net.runOn(0,[&](com::Node&){
			auto state = getSystemState(com::Network::getNetwork());
		});

		// block before shutdown
		net.sync();

		// shutdown dashboard service
		shutdownDashbordService(net);

	}

} // end of namespace mon
} // end of namespace runtime
} // end of namespace allscale
