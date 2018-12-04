#include <gtest/gtest.h>

#include "allscale/runtime/com/network.h"
#include "allscale/runtime/data/data_item_manager.h"
#include "allscale/runtime/work/worker.h"


namespace allscale {
namespace runtime {
namespace work {

	TEST(WorkerPool, StartStop) {

		// create a pool
		WorkerPool pool;

		// start the worker pool
		pool.start();

		// stop the pool
		pool.stop();

	}


	TEST(WorkerPool, Processing) {

		int x = 0;

		// get some network of any size
		auto network = com::Network::create();
		assert_true(network);

		auto& net = *network;
		installTreetureStateService(net);
		data::installDataItemManagerService(net);
		startWorkerPool(net);

		net.runOn(0,[&](com::Node& node){
			node.getService<WorkerPool>().schedule(make_lambda_task(getFreshId(),[&]{
				x = 1;
			}));
		});

		// stop worker
		stopWorkerPool(net);

		// now x should be one
		EXPECT_EQ(1,x);

	}

	TEST(WorkerPool, ProcessingLoop) {

		int N = 100;
		int x = 0;

		// create the network
		auto network = com::Network::create();
		assert_true(network);

		auto& net = *network;
		installTreetureStateService(net);
		data::installDataItemManagerService(net);

		// start the worker
		startWorkerPool(net);

		// schedule N tasks
		net.runOn(0,[&](com::Node& node){
			auto& worker = node.getService<WorkerPool>();
			for(int i=0; i<N; i++) {
				worker.schedule(make_lambda_task(getFreshId(),[&]{
					x++;
				}));
			}
		});

		// stop the worker
		stopWorkerPool(net);

		// now x should be one
		EXPECT_EQ(N,x);
	}


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
