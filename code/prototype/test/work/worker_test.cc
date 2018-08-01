#include <gtest/gtest.h>

#include "allscale/runtime/com/network.h"
#include "allscale/runtime/data/data_item_manager.h"
#include "allscale/runtime/work/worker.h"


namespace allscale {
namespace runtime {
namespace work {

	TEST(Worker, StartStop) {

		// create a worker
		Worker worker;

		// start the worker
		worker.start();

		// stop the worker
		worker.stop();

	}


	TEST(Worker, Processing) {

		int x = 0;

		// get some network of any size
		auto network = com::Network::create();
		assert_true(network);

		auto& net = *network;
		installTreetureStateService(net);
		data::installDataItemManagerService(net);
		startWorker(net);

		net.runOn(0,[&](com::Node& node){
			node.getService<Worker>().schedule(make_lambda_task(getFreshId(),[&]{
				x = 1;
			}));
		});

		// stop worker
		stopWorker(net);

		// now x should be one
		EXPECT_EQ(1,x);

	}

	TEST(Worker, ProcessingLoop) {

		int N = 100;
		int x = 0;

		// create the network
		auto network = com::Network::create();
		assert_true(network);

		auto& net = *network;
		installTreetureStateService(net);
		data::installDataItemManagerService(net);

		// start the worker
		startWorker(net);

		// schedule N tasks
		net.runOn(0,[&](com::Node& node){
			auto& worker = node.getService<Worker>();
			for(int i=0; i<N; i++) {
				worker.schedule(make_lambda_task(getFreshId(),[&]{
					x++;
				}));
			}
		});

		// stop the worker
		stopWorker(net);

		// now x should be one
		EXPECT_EQ(N,x);
	}


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
