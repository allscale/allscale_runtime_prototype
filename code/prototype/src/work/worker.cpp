
#include "allscale/runtime/work/worker.h"

#include <thread>

#include "allscale/utils/unused.h"

#include "allscale/runtime/log/logger.h"
#include "allscale/runtime/com/network.h"
#include "allscale/runtime/data/data_item_manager.h"
#include "allscale/runtime/work/scheduler.h"
#include "allscale/runtime/mon/task_stats.h"


namespace allscale {
namespace runtime {
namespace work {

	thread_local Worker* tl_current_worker = nullptr;

	void startWorker(com::Network& net) {

		net.runOnAll([](com::Node& node){

			// install worker
			auto& worker = node.startService<Worker>();

			// start worker
			worker.start();

		});

	}

	void stopWorker(com::Network& net) {
		net.runOnAll([](com::Node& node){
			node.getService<Worker>().stop();
			node.stopService<Worker>();
		});
	}

	Worker& Worker::getLocalWorker() {
		assert_true(tl_current_worker) << "Can not localize worker outside of any worker thread!";
		return *tl_current_worker;
	}

	void Worker::start() {

		// switch from ready to startup
		State st = Ready;
		__allscale_unused bool success = state.compare_exchange_strong(st,Startup);
		assert_true(success) << "Invalid state " << st << ": cannot start non-ready worker.";

		// start processing thread
		if (node) {
			// run in proper network and node context node context
			auto& network = com::Network::getNetwork();
			thread = std::thread([&]{
				network.runOn(node->getRank(),[&](com::Node&){
					run();
				});
			});
		} else {
			// run without node context
			thread = std::thread([&]{ run(); });
		}

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
		__allscale_unused bool success = state.compare_exchange_strong(st,Shutdown);
		assert_true(success) << "Invalid state " << st << ": cannot shut down non-running worker.";

		// wait for the thread to finish its work
		thread.join();

		// mark as running
		st = Shutdown;
		success = state.compare_exchange_strong(st,Terminated);
		assert_true(success) << "Invalid state " << st << ": cannot switch to terminated state.";

	}

	void Worker::run() {

		// set thread-local worker
		tl_current_worker = this;

		// while running ..
		while(true) {

			// process all tasks in the queue
			while(step()) {}

			// if terminated => terminate thread
			if (state > Running) return;

			// otherwise yield thread to allow other threads to run
			std::this_thread::yield();
		}

		// reset thread local worker
		tl_current_worker = nullptr;
	}

	bool Worker::step() {

		// process a task if available
		if (auto t = queue.dequeueBack()) {

			// get a reference to the local data item manager
			auto dim = (node) ? &data::DataItemManagerService::getLocalService() : nullptr;

			// ask the scheduler what to do
			if (t->isSplitable() && shouldSplit(t)) {

				// the decision was to split the task, so do so
				auto reqs = t->getSplitRequirements();

				// log this action
				DLOG << "Splitting " << t->getId() << " on node " << rank << " with requirements " << reqs << "\n";

				// allocate requirements (blocks till ready)
				if (dim) dim->allocate(reqs);

				// in this case we split the task
				t->split();

				// free requirements
				if (dim) dim->release(reqs);

				DLOG << "Splitting " << t->getId() << " on node " << rank << " completed\n";

				// increment split counter
				splitCounter++;

			} else {

				using clock = std::chrono::high_resolution_clock;

				// in this case we process the task
				auto reqs = t->getProcessRequirements();

				// log this action
				DLOG << "Processing " << t->getId() << " on node " << rank << " with requirements " << reqs << "\n";

				// allocate requirements (blocks till ready)
				if (dim) dim->allocate(reqs);

				// process this task
				auto begin = clock::now();
				t->process();
				auto end = clock::now();

				// free requirements
				if (dim) dim->release(reqs);

				DLOG << "Processing " << t->getId() << " on node " << rank << " completed\n";

				// increment processed counter
				processedCounter++;

				// increment workload counter (by the fraction of work processed)
				processedWork = processedWork + 1.0/(1<<t->getId().getDepth());

				// increment processing time
				processTime = processTime.load() + (end - begin);

				// keep task statistics up to date
				{
					guard g(taskTimesLock);
					taskTimes.add(t->getId(),(end-begin));
				}

			}

			return true;
		}

		// no task was available
		return false;

	}


	void yield() {
		// attempt to process another task while waiting
		if (Worker* worker = tl_current_worker) {
			worker->step();
		} else {
			// yield this thread (the main thread, not a worker)
			using namespace std::chrono_literals;
			std::this_thread::sleep_for(10ms);
		}
	}

} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale
