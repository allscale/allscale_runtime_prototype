
#include "allscale/runtime/work/worker.h"

#include <thread>

namespace allscale {
namespace runtime {
namespace work {

	void Worker::start() {

		// switch from ready to startup
		State st = Ready;
		bool success = state.compare_exchange_strong(st,Startup);
		assert_true(success) << "Invalid state " << st << ": cannot start non-ready worker.";

		// start processing thread
		thread = std::thread([&]{ run(); });

		// mark as running
		st = Startup;
		success = state.compare_exchange_strong(st,Running);
		assert_true(success) << "Invalid state " << st << ": cannot switch to running state.";

	}

	void Worker::schedule(TaskPtr&& task) {
		// simply add the task to the queue
		assert_eq(Running,state) << "Invalid state: unable to assign work to non-running worker.";
		queue.enqueueFront(std::move(task));
	}

	void Worker::stop() {

		// switch from ready to startup
		State st = Running;
		bool success = state.compare_exchange_strong(st,Shutdown);
		assert_true(success) << "Invalid state " << st << ": cannot shut down non-running worker.";

		// wait for the thread to finish its work
		thread.join();

		// mark as running
		st = Shutdown;
		success = state.compare_exchange_strong(st,Terminated);
		assert_true(success) << "Invalid state " << st << ": cannot switch to terminated state.";

	}

	void Worker::run() {

		// while running ..
		while(true) {

			// process all tasks in the queue
			while(auto t = queue.dequeueBack()) {
				t->process();
			}

			// if terminated => terminate thread
			if (state != Running) return;

			// otherwise yield thread to allow other threads to run
			std::this_thread::yield();
		}

	}

} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale
