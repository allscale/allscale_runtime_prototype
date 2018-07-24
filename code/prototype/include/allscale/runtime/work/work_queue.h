/*
 * The prototype implementation of the scheduler interface.
 *
 *  Created on: Jul 24, 2018
 *      Author: herbert
 */

#pragma once

#include <mutex>
#include <list>

#include "allscale/runtime/com/node_service.h"

#include "allscale/runtime/work/task.h"

namespace allscale {
namespace runtime {
namespace work {

	/**
	 * A simple work queue implementation.
	 */
	class WorkQueue {

		// a lock for queue operations
		mutable std::mutex lock;

		// the queue of tasks maintained
		std::list<TaskPtr> queue;

	public:

		/**
		 * Determines whether this queue is empty.
		 */
		bool empty() const;

		/**
		 * Determines the number of tasks in the queue.
		 */
		std::size_t size() const;

		/**
		 * Enqueues a new task at the front of the queue.
		 * The queue takes ownership of the task.
		 */
		void enqueueFront(TaskPtr&& task);

		/**
		 * Enqueues a new task at the back of the queue.
		 * The queue takes ownership of the task.
		 */
		void enqueueBack(TaskPtr&& task);

		/**
		 * Dequeues a task from the front of the queue.
		 * The queue hands of ownership of the task.
		 */
		TaskPtr dequeueFront();

		/**
		 * Dequeues a task from the back of the queue.
		 * The queue hands of ownership of the task.
		 */
		TaskPtr dequeueBack();

	};

} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale
