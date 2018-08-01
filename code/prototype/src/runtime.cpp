
#include <cstdlib>

#include "allscale/runtime/runtime.h"

#include "allscale/runtime/data/data_item_manager.h"
#include "allscale/runtime/work/scheduler.h"
#include "allscale/runtime/work/treeture.h"
#include "allscale/runtime/work/worker.h"

namespace allscale {
namespace runtime {

	Runtime::Runtime(com::Network& net) : network(net) {

		// install data item manager services
		network.installServiceOnNodes<data::DataItemManagerService>();

		// install treeture service
		work::installTreetureStateService(network);

		// install scheduler service
		work::installSchedulerService(network);

		// install and start workers in nodes
		work::startWorker(network);

	}

	void Runtime::shutdown() {

		// shut down workers
		work::stopWorker(network);

	}

} // end of namespace runtime
} // end of namespace allscale
