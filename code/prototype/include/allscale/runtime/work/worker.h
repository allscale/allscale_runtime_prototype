/*
 * The prototype implementation of the scheduler interface.
 *
 *  Created on: Jul 24, 2018
 *      Author: herbert
 */

#pragma once

#include <atomic>
#include <chrono>
#include <thread>

#include "allscale/runtime/com/node.h"
#include "allscale/runtime/hw/model.h"

#include "allscale/runtime/work/work_queue.h"
#include "allscale/runtime/mon/task_stats.h"

namespace allscale {
namespace runtime {
namespace work {

	/**
	 * A function to be called by tasks blocking within a worker.
	 */
	void yield();

	/**
	 * Installs and starts worker pool on the given network.
	 */
	void startWorkerPool(com::Network&);

	/**
	 * Stops all worker pool on the given network (shutdown).
	 */
	void stopWorkerPool(com::Network&);


	// forward declaration
	class WorkerPool;


	/**
	 * A simple class wrapping a worker thread running within a node.
	 */
	class Worker {

		// the state of workers
		enum State {
			Ready,			// < created, not yet running
			Startup,		// < worker is currently starting up
			Running,		// < running
			Shutdown,		// < worker is currently shutting down
			Terminated		// < terminated (will never run again)
		};

		// the pool being a part of
		WorkerPool& pool;

		// the configuration for this worker retrieved from the HW model
		hw::WorkerConfig config;

		// a flag indicating whether this worker is still active
		std::atomic<State> state;

		// the thread conducting the actual work
		std::thread thread;

		// -- monitoring metrics --

		// the number of tasks split by this worker
		std::atomic<std::uint32_t> splitCounter;

		// the number of tasks processed by this worker
		std::atomic<std::uint32_t> processedCounter;

		// tracing the load processed by this worker, considering the task granularity
		std::atomic<double> processedWork;

		// amount of time spend in processing tasks (in nanoseconds)
		std::atomic<std::chrono::nanoseconds> processTime;

		// a statistic for the observed task execution times
		mon::TaskTimes taskTimes;

		// a lock to synchronize on task time statistic updates
		mutable std::mutex taskTimesLock;

		using guard = std::lock_guard<std::mutex>;

	public:

		Worker(WorkerPool& pool, const hw::WorkerConfig& config)
			: pool(pool), config(config), state(Ready),
			  splitCounter(0), processedCounter(0), processedWork(0), processTime(std::chrono::nanoseconds(0)) {}

		Worker(const Worker&) = delete;
		Worker(Worker&&) = delete;

		/**
		 * Starts this worker. The worker will
		 * start processing work from the queue in an extra thread.
		 */
		void start();

		/**
		 * Stops this worker. The worker will finish all tasks
		 * in the queue and terminate the thread.
		 */
		void stop();

		/**
		 * Obtains the number of split tasks.
		 */
		std::uint32_t getNumSplitTasks() const {
			return splitCounter;
		}

		/**
		 * Obtains the number of processed tasks.
		 */
		std::uint32_t getNumProcessedTasks() const {
			return processedCounter;
		}

		/**
		 * Obtains an estimate of the processed work.
		 */
		double getProcessedWork() const {
			return processedWork;
		}

		/**
		 * Obtains the amount of time spend on task processing.
		 */
		std::chrono::nanoseconds getProcessTime() const {
			return processTime;
		}

		/**
		 * Obtains a summary of the task execution time processed by this worker.
		 */
		mon::TaskTimes getTaskTimeSummary() const {
			guard g(taskTimesLock);
			return taskTimes;
		}

		/**
		 * A function to be called by tasks blocking within a worker.
		 */
		friend void yield();

	private:

		/**
		 * An internal function processing tasks (the one processed by the managed thread).
		 */
		void run();

		/**
		 * An internal function processing zero or one scheduling steps.
		 * Returns true if a step was processed, false otherwise.
		 */
		bool step();

	};


	/**
	 * A pool to be integratable in a node to processes tasks concurrently.
	 */
	class WorkerPool {

		friend class Worker;

		// the work queue to be processed by this worker
		WorkQueue queue;

		// the list of workers being part of this pool
		std::vector<std::unique_ptr<Worker>> workers;

		// the node this worker is working on (if there is one)
		com::Node* node;

		WorkerPool(com::Node* node);

	public:

		WorkerPool()
			: WorkerPool(nullptr) {}

		WorkerPool(com::Node& node)
			: WorkerPool(&node) {};

		WorkerPool(const WorkerPool&) = delete;
		WorkerPool(WorkerPool&&) = delete;


		/**
		 * Starts this worker. The worker will
		 * start processing work from the queue in an extra thread.
		 */
		void start();

		/**
		 * Stops this worker. The worker will finish all tasks
		 * in the queue and terminate the thread.
		 */
		void stop();

		/**
		 * Obtains a reference to the local worker pool.
		 */
		static WorkerPool& getLocalWorkerPool();

		/**
		 * Adds the given task to the work-queue of this worker pool.
		 * The pool will take ownership of the task.
		 */
		void schedule(TaskPtr&&);

		/**
		 * Obtains the number of workers maintained within this pool.
		 */
		std::size_t getNumWorkers() const {
			return workers.size();
		}

		/**
		 * Obtains the number of split tasks.
		 */
		std::uint32_t getNumSplitTasks() const;

		/**
		 * Obtains the number of processed tasks.
		 */
		std::uint32_t getNumProcessedTasks() const;

		/**
		 * Obtains an estimate of the processed work.
		 */
		double getProcessedWork() const;

		/**
		 * Obtains the amount of time spend on task processing.
		 */
		std::chrono::nanoseconds getProcessTime() const;

		/**
		 * Obtains a summary of the task execution time processed by this worker.
		 */
		mon::TaskTimes getTaskTimeSummary() const;

		/**
		 * A function to be called by tasks blocking within a worker.
		 */
		friend void yield();

		/**
		 * A function to schedule.
		 */
		friend void schedule(TaskPtr&&);

	};


} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale
