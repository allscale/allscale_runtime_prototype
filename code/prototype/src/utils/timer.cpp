/*
 * A generic utility to run periodic tasts in a system.
 *
 *  Created on: Aug 16, 2018
 *      Author: herbert
 */


#include "allscale/runtime/utils/timer.h"

namespace allscale {
namespace runtime {
namespace utils {


	PeriodicExecutor::PeriodicExecutor() : alive(true) {
		// start thread
		thread = std::thread([&]{ run(); });
	}

	PeriodicExecutor::~PeriodicExecutor() {

		// set alive to false
		{
			guard g(mutex);
			alive = false;
		}

		// signal change to worker
		var.notify_one();

		// wait for thread to finish
		thread.join();
	}

	void PeriodicExecutor::runPeriodically(const task_t& op, const std::chrono::milliseconds& interval) {
		assert_gt(interval.count(),0);
		guard g(mutex);
		auto next = clock::now() + interval;
		auto cur = (queue.empty()) ? next : queue.top().next;
		queue.push({ op, next, interval });
		if (next <= cur) var.notify_one();
	}

	void PeriodicExecutor::run() {

		// get ownership of state
		std::unique_lock<std::mutex> g(mutex);

		// process the task queue
		while(alive) {

			// get next event
			if (queue.empty()) {
				var.wait(g);
			} else {
				// wait for next event to be ready
				auto next = queue.top().next;
				var.wait_until(g, next);

				// kill if done
				if (!alive) return;

				// check whether it is time
				auto now = clock::now();
				if (now <= queue.top().next) {
					// this was a spurious wake up or a notify for a new task => sleep again
					continue;
				}

				// run the next task
				while (!queue.empty() && now >= queue.top().next) {
					Entry entry = queue.top();
					queue.pop();
					auto& task = entry.task;
					if (task()) {
						queue.push({ task, entry.next + entry.interval, entry.interval });
					}
				}
			}
		}

	}


	void installPeriodicExecutorService(com::Network& net) {
		net.installServiceOnNodes<PeriodicExecutorService>();
	}

	void removePeriodicExecutorService(com::Network& net) {
		net.removeServiceOnNodes<PeriodicExecutorService>();
	}



} // end namespace util
} // end namespace runtime
} // end namespace allscale
