
#include <cstdlib>

#include "allscale/runtime/runtime.h"

#include "allscale/runtime/utils/timer.h"
#include "allscale/runtime/data/data_item_manager.h"
#include "allscale/runtime/work/scheduler.h"
#include "allscale/runtime/work/treeture.h"
#include "allscale/runtime/work/worker.h"
#include "allscale/runtime/mon/dashboard.h"
#include "allscale/runtime/mon/file_logger.h"

namespace allscale {
namespace runtime {

	Runtime::Runtime(com::Network& net) : network(net) {

		// start with the basic, periodic executor service
		utils::installPeriodicExecutorService(network);

		// install data item manager services
		network.installServiceOnNodes<data::DataItemManagerService>();

		// install treeture service
		work::installTreetureStateService(network);

		// install scheduler service
		work::installSchedulerService(network);

		// install dashboard service
		mon::installDashbordService(network);

		// install file logger service
		mon::installFileLoggerService(network);

		// install and start workers in nodes
		work::startWorkerPool(network);

		// wait for all network instances to be at this state
		network.sync();
	}

	void Runtime::shutdown() {

		// wait until all network instances are at this point
		network.sync();

		// start by stopping periodic operations
		utils::removePeriodicExecutorService(network);

		// wait until all periodic executor are down
		network.sync();

		// shutdown file logger service
		mon::shutdownFileLoggerService(network);

		// shutdown dashboard service
		mon::shutdownDashbordService(network);

		// shut down workers
		work::stopWorkerPool(network);

	}

} // end of namespace runtime
} // end of namespace allscale
