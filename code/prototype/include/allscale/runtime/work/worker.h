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

#include "allscale/runtime/work/work_queue.h"

namespace allscale {
namespace runtime {
namespace work {

	/**
	 * A function to be called by tasks blocking within a worker.
	 */
	void yield();

	/**
	 * Installs and starts worker on the given network.
	 */
	void startWorker(com::Network&);

	/**
	 * Stops all worker on the given network (shutdown).
	 */
	void stopWorker(com::Network&);

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

		// the work queue to be processed by this worker
		WorkQueue queue;

		// a flag indicating whether this worker is still active
		std::atomic<State> state;

		// the thread conducting the actual work
		std::thread thread;

		// the rank this worker is running on
		com::rank_t rank;

		// the node this worker is working on (if there is one)
		com::Node* node;

		// the number of tasks split by this worker
		std::atomic<std::uint32_t> splitCounter;

		// the number of tasks processed by this worker
		std::atomic<std::uint32_t> processedCounter;

		// tracing the load processed by this worker, considering the task granularity
		std::atomic<double> processedWork;

		// amount of time spend in processing tasks (in nanoseconds)
		std::atomic<std::chrono::nanoseconds> processTime;

	public:

		Worker(com::rank_t rank = 0)
			: state(Ready), rank(rank), node(nullptr),
			  splitCounter(0), processedCounter(0), processedWork(0), processTime(std::chrono::nanoseconds(0)) {}

		Worker(com::Node& node) : Worker(node.getRank()) {
			this->node = &node;
		}

		Worker(const Worker&) = delete;
		Worker(Worker&&) = delete;

		/**
		 * Obtains a reference to the local worker.
		 */
		static Worker& getLocalWorker();

		/**
		 * Starts this worker. The worker will
		 * start processing work from the queue in an extra thread.
		 */
		void start();

		/**
		 * Adds the given task to the work-queue of this worker.
		 * The worker will take ownership of the task.
		 */
		void schedule(TaskPtr&&);

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
		 * A function to be called by tasks blocking within a worker.
		 */
		friend void yield();

		/**
		 * A function to schedule.
		 */
		friend void schedule(TaskPtr&&);

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


} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale
