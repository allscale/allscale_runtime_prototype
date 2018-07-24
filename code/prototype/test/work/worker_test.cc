#include <gtest/gtest.h>

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

		// create a worker
		Worker worker;

		// start the worker
		worker.start();

		// schedule a task
		worker.schedule(make_lambda_task(TaskID(1),[&]{
			x = 1;
		}));

		// stop the worker
		worker.stop();

		// now x should be one
		EXPECT_EQ(1,x);
	}

	TEST(Worker, ProcessingLoop) {

		int N = 100;
		int x = 0;

		// create a worker
		Worker worker;

		// start the worker
		worker.start();

		// schedule a tasks
		for(int i=0; i<N; i++) {
			worker.schedule(make_lambda_task(TaskID(1),[&]{
				x++;
			}));
		}

		// stop the worker
		worker.stop();

		// now x should be one
		EXPECT_EQ(N,x);
	}


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
