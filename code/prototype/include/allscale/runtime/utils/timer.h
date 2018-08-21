/*
 * A header for a generic service performing periodic operations.
 *
 *  Created on: Aug 16, 2018
 *      Author: herbert
 */

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <thread>
#include <mutex>
#include <queue>

#include "allscale/runtime/com/network.h"

namespace allscale {
namespace runtime {
namespace utils {

	// the type of operation to schedule
	using task_t = std::function<bool()>;

	class PeriodicExecutor {

		using clock = std::chrono::high_resolution_clock;
		using time_point = typename clock::time_point;
		using duration = typename clock::duration;

		struct Entry {
			task_t task;
			time_point next;
			duration interval;

			bool operator<(const Entry& other) const {
				return next > other.next; // smallest first
			}
		};

		std::thread thread;

		std::mutex mutex;

		std::condition_variable var;

		std::atomic<bool> alive;

		std::priority_queue<Entry> queue;

		using guard = std::lock_guard<std::mutex>;

	public:

		// creates a new executor, running.
		PeriodicExecutor();

		// stops this executor
		~PeriodicExecutor();

		/**
		 * Registers the given function to be periodically invoked.
		 */
		void runPeriodically(const task_t& op, const std::chrono::milliseconds& interval = std::chrono::seconds(1));

		/**
		 * Registers the given function to be periodically invoked.
		 */
		template<typename Op>
		void runPeriodically(const Op& op, const std::chrono::milliseconds& interval = std::chrono::seconds(1)) {
			runPeriodically(task_t(op),interval);
		}

	private:

		void run();

	};


	class PeriodicExecutorService {

		PeriodicExecutor executor;

	public:

		PeriodicExecutorService(com::Node&) {}

		template<typename Op>
		void runPeriodically(const Op& op, const std::chrono::seconds& interval = std::chrono::seconds(1)) {
			executor.runPeriodically(op,interval);
		}

	};


	void installPeriodicExecutorService(com::Network& net);

	void removePeriodicExecutorService(com::Network& net);


} // end namespace util
} // end namespace runtime
} // end namespace allscale
