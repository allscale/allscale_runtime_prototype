
#include <cstdlib>

#include "allscale/runtime/runtime.h"

#include "allscale/runtime/data/data_item_manager.h"
#include "allscale/runtime/work/worker.h"
#include "allscale/runtime/work/scheduler.h"

namespace allscale {
namespace runtime {

	Runtime::Runtime(com::Network& net) : network(net) {

		// install data item manager services
		network.installServiceOnNodes<data::DataItemManagerService>();

		// install scheduler service
		work::installSchedulerService(network);

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

} // end of namespace runtime
} // end of namespace allscale
