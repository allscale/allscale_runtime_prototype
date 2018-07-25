
#include <cstdlib>

#include "allscale/runtime/runtime.h"

#include "allscale/runtime/data/data_item_manager.h"
#include "allscale/runtime/work/worker.h"

namespace allscale {
namespace runtime {

	Runtime::Runtime(int numNodes) : network(numNodes) {

		// install data item manager services
		network.installServiceOnNodes<data::DataItemManagerService>();

		// install and start workers in nodes
		network.runOnAll([](com::Node& node){

			// install worker
			auto& worker = node.startService<work::Worker>();

			// start worker
			worker.start();

		});

	}

	void Runtime::shutdown() {

		// shut down workers
		network.runOnAll([](com::Node& node){
			node.getService<work::Worker>().stop();
		});

	}

	Runtime Runtime::create() {

		// get the number of nodes
		int num_nodes = 2;	// < by default we use two nodes
		if (auto val = std::getenv("ART_NUM_NODES")) {
			num_nodes = std::atoi(val);
			if (num_nodes < 1) num_nodes = 1;
		}

		// create and return the runtime
		return Runtime(num_nodes);
	}

} // end of namespace runtime
} // end of namespace allscale
