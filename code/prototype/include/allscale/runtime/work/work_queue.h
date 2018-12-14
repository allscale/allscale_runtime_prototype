/*
 * The prototype implementation of the scheduler interface.
 *
 *  Created on: Jul 24, 2018
 *      Author: herbert
 */

#pragma once

#include <mutex>
#include <deque>

#include <allscale/utils/spinlock.h>
#include "allscale/runtime/com/node_service.h"

#include "allscale/runtime/work/task.h"

namespace allscale {
namespace runtime {
namespace work {

	/**
	 * A simple work queue implementation.
	 */
	class WorkQueue {

		// determine lock type to be used
		using lock_t = std::mutex;
		using guard = std::lock_guard<lock_t>;

		// a lock for queue operations
		mutable lock_t lock;

		// the queue of tasks maintained
		std::deque<TaskPtr> queue;

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
