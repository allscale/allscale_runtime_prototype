/*
 * The prototype implementation of the scheduler interface.
 *
 *  Created on: Jul 24, 2018
 *      Author: herbert
 */

#pragma once

#include <atomic>
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

		// the number of tasks processed by this worker
		std::uint32_t taskCounter;

	public:

		Worker(com::rank_t rank = 0) : state(Ready), rank(rank), node(nullptr), taskCounter(0) {}

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
		 * Obtains the number of processed tasks.
		 */
		std::uint32_t getNumProcessedTasks() const {
			return taskCounter;
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
