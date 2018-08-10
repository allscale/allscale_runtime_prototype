
#include <cstdlib>

#include "allscale/runtime/runtime.h"

#include "allscale/runtime/data/data_item_manager.h"
#include "allscale/runtime/work/scheduler.h"
#include "allscale/runtime/work/treeture.h"
#include "allscale/runtime/work/worker.h"
#include "allscale/runtime/mon/dashboard.h"

namespace allscale {
namespace runtime {

	Runtime::Runtime(com::Network& net) : network(net) {

		// install data item manager services
		network.installServiceOnNodes<data::DataItemManagerService>();

		// install treeture service
		work::installTreetureStateService(network);

		// install scheduler service
		work::installSchedulerService(network);

		// install dashboard service
		mon::installDashbordService(network);

		// install and start workers in nodes
		work::startWorker(network);

		// wait for all network instances to be at this state
		network.sync();
	}

	void Runtime::shutdown() {

		// wait until all network instances are at this point
		network.sync();

		// shutdown dashboard service
		mon::shutdownDashbordService(network);

		// shut down workers
		work::stopWorker(network);

	}

} // end of namespace runtime
} // end of namespace allscale
