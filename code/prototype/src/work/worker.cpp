
#include "allscale/runtime/work/worker.h"

#include <thread>

#include "allscale/utils/unused.h"

#include "allscale/runtime/log/logger.h"
#include "allscale/runtime/com/network.h"
#include "allscale/runtime/hw/model.h"
#include "allscale/runtime/data/data_item_manager.h"
#include "allscale/runtime/work/scheduler.h"
#include "allscale/runtime/mon/task_stats.h"


namespace allscale {
namespace runtime {
namespace work {

	thread_local Worker* tl_current_worker = nullptr;

	void startWorkerPool(com::Network& net) {

		net.runOnAll([](com::Node& node){

			// install worker pool
			auto& pool = node.startService<WorkerPool>();

			// start workers in pool
			pool.start();

		});

	}

	void stopWorkerPool(com::Network& net) {
		net.runOnAll([](com::Node& node){
			node.getService<WorkerPool>().stop();
			node.stopService<WorkerPool>();
		});
	}

	void Worker::start() {

		// switch from ready to startup
		State st = Ready;
		__allscale_unused bool success = state.compare_exchange_strong(st,Startup);
		assert_true(success) << "Invalid state " << st << ": cannot start non-ready worker.";

		// start processing thread
		if (pool.node) {
			// run in proper network and node context node context
			auto& network = com::Network::getNetwork();
			thread = std::thread([&]{
				network.runOn(pool.node->getRank(),[&](com::Node&){
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

	void Worker::stop() {

		// switch from ready to startup
		State st = Running;
		__allscale_unused bool success = state.compare_exchange_strong(st,Shutdown);
		assert_true(success) << "Invalid state " << st << ": cannot shut down non-running worker.";

		// wait for the thread to finish its work
		thread.join();

		// mark as terminated
		st = Shutdown;
		success = state.compare_exchange_strong(st,Terminated);
		assert_true(success) << "Invalid state " << st << ": cannot switch to terminated state.";

	}

	void Worker::run() {

		// set thread-local worker
		tl_current_worker = this;

		// fix thread affinity
		pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &config.affinityMask);

		// while running ..
		while(state < Shutdown) {

			// contribute this worker to process fibers
			pool.fiberContext.yield(true);

		}

		// reset thread local worker
		tl_current_worker = nullptr;
	}

	namespace {

		void resumeHandler(void* task) {
			Task::notifyResume(reinterpret_cast<Task*>(task));
		};
	}

	void Worker::process(const TaskPtr& task) {

		// get a reference to the local data item manager
		auto dim = (pool.node) ? &data::DataItemManagerService::getLocalService() : nullptr;

		// ask the scheduler what to do
		if (task->isSplitable() && shouldSplit(task)) {

			// test whether this task should handle the data acquisition for its child-tasks
			TaskRequirementsCollector collector(*pool.node, *dim);
			if (dim && !task->getRequirementCollector() && shouldAcquireData(task)) {

				// instruct requirements collector to retrieve required data
				collector.acquire(
					task->getDependencies(),
					task->getProcessRequirements()
				);

				// link the collector with this task and its sub-tasks
				task->setRequirementCollector(&collector);
			}

			// the decision was to split the task, so do so
			auto reqs = task->getSplitRequirements();

			// log this action
			DLOG << "Splitting " << task->getId() << " on node " << (pool.node?pool.node->getRank():0) << " with requirements " << reqs << "\n";

			// allocate requirements (blocks till ready)
			if (dim) dim->allocate(reqs);

			// in this case we split the task
			task->split();

			// free requirements
			if (dim) dim->release(reqs);

			DLOG << "Splitting " << task->getId() << " on node " << (pool.node?pool.node->getRank():0) << " completed\n";

			// increment split counter
			splitCounter++;

			// make sure collector is done
			collector.wait();

		} else {

			using clock = std::chrono::high_resolution_clock;

			// in this case we process the task
			auto reqs = task->getProcessRequirements();

			// check whether there is already a requirement collector for this task
			auto collector = task->getRequirementCollector();
			if (collector) {

				// wait for required data
				collector->waitFor(reqs);

			} else {

				// wait until task dependencies are satisfied
				task->getDependencies().wait();

				// allocate requirements (blocks till ready)
				if (dim) dim->allocate(reqs);

				// suspend, and rate as medium priority
				allscale::utils::fiber::suspend(allscale::utils::fiber::Priority::MEDIUM);

			}

			// log this action
			DLOG << "Processing " << task->getId() << " on node " << (pool.node?pool.node->getRank():0) << " with requirements " << reqs << "\n";

			// process this task
			auto begin = clock::now();
			task->process();
			auto end = clock::now();

			DLOG << "Processing " << task->getId() << " on node " << (pool.node?pool.node->getRank():0) << " completed\n";

			if (!collector) {

				// free requirements
				if (dim) dim->release(reqs);

			}

			// increment processed counter
			processedCounter++;

			// keep task statistics up to date
			if (task->getId().getRootID() != 0) {	// < exclude main task
				guard g(statisticsLock);

				// increment workload counter (by the fraction of work processed)
				processedWork += 1.0/(1<<task->getId().getDepth());

				// increment processing time
				processTime += (end - begin);

				// update task statistics
				taskTimes.add(task->getId(),(end-begin));
			}

		}

	}


	WorkerPool::WorkerPool(allscale::utils::FiberContext& ctxt, com::Node* node)
		: fiberContext(ctxt), node(node) {

		// get rank
		com::rank_t rank = (node ? node->getRank() : 0);
		auto config = hw::getWorkerPoolConfig(rank);

		// start up worker threads
		workers.reserve(config.size());
		for(const auto& workerConfig : config) {
			workers.push_back(std::make_unique<Worker>(*this,workerConfig));
		}
	}

	void WorkerPool::start() {
		startTime = clock::now();
		for(auto& cur : workers) {
			cur->start();
		}
	}

	void WorkerPool::stop() {
		for(auto& cur : workers) {
			cur->stop();
		}
	}

	void WorkerPool::schedule(TaskPtr&& task) {

		// get address of task
		auto taskPointer = task.get();

		// set up event handler
		allscale::utils::fiber::FiberEvents taskEventHandler;
		taskEventHandler.resume  = { &resumeHandler, taskPointer };

		// initiate task
		fiberContext.start([&,t{move(task)}]{

			// suspend this task, to allow parent task to continue spawning tasks
			allscale::utils::fiber::suspend(allscale::utils::fiber::Priority::LOW);

			// process task in current worker - fall back to worker 0
			auto worker = tl_current_worker ? tl_current_worker : workers[0].get();

			assert_true(worker);
			worker->process(t);

		}, allscale::utils::fiber::Priority::MEDIUM, taskEventHandler);

	}

	std::uint32_t WorkerPool::getNumSplitTasks() const {
		std::uint32_t res = 0;
		for(auto& cur : workers) {
			res += cur->getNumSplitTasks();
		}
		return res;
	}

	std::uint32_t WorkerPool::getNumProcessedTasks() const {
		std::uint32_t res = 0;
		for(auto& cur : workers) {
			res += cur->getNumProcessedTasks();
		}
		return res;
	}

	double WorkerPool::getProcessedWork() const {
		double res = 0;
		for(auto& cur : workers) {
			res += cur->getProcessedWork();
		}
		return res;
	}

	std::chrono::nanoseconds WorkerPool::getProcessTime() const {
		std::chrono::nanoseconds res(0);
		for(auto& cur : workers) {
			res += cur->getProcessTime();
		}
		return res;
	}

	mon::TaskTimes WorkerPool::getTaskTimeSummary() const {
		mon::TaskTimes res;
		for(auto& cur : workers) {
			res += cur->getTaskTimeSummary();
		}
		return res;
	}

	TaskStatisticEntry WorkerPool::getLocalStatistics() const {

		auto ms = [](auto time) {
			return std::chrono::duration_cast<std::chrono::milliseconds>(time);
		};

		TaskStatisticEntry res;
		res.rank = node->getRank();
		res.split_tasks = getNumSplitTasks();
		res.processed_tasks = getNumProcessedTasks();
		res.estimated_workload = getProcessedWork();
		res.processingTime = ms(getProcessTime());
		res.idleTime = ms(fiberContext.getTotalIdleTime());
		res.totalRuntime = ms((clock::now() - startTime) * getNumWorkers());
		return res;
	}

	TaskStatistic WorkerPool::getStatistics() const {
		TaskStatistic stats;
		auto& network = com::Network::getNetwork();

		// trigger remote calls
		std::vector<com::RemoteCallResult<TaskStatisticEntry>> futures;
		for(com::rank_t i=0; i<network.numNodes(); i++) {
			futures.push_back(network.getRemoteProcedure(i,&WorkerPool::getLocalStatistics)());
		}

		// collect results
		for(auto& cur : futures) {
			stats.add(cur.get());
		}

		// done
		return stats;
	}

} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale
