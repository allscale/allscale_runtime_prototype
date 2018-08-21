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

			// signal change to worker
			var.notify_one();
		}

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

		// process the task queue
		while(alive) {

			// split to next time
			std::unique_lock<std::mutex> g(mutex);

			// get next event
			if (queue.empty()) {
				var.wait(g);
			} else {
				// wait for next event to be ready
				var.wait_until(g,queue.top().next, [&]{
					return !alive || clock::now() >= queue.top().next;
				});

				// kill if done
				if (!alive) return;

				// run the next task
				auto now = clock::now();
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
