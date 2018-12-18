
#include "allscale/runtime/work/work_queue.h"

namespace allscale {
namespace runtime {
namespace work {


	bool WorkQueue::empty() const {
		return isEmpty.load(std::memory_order_relaxed);
	}

	std::size_t WorkQueue::size() const {
		guard g(lock);
		return queue.size();
	}

	void WorkQueue::enqueueFront(TaskPtr&& task) {
		guard g(lock);
		assert_true(task);
		assert_true(task->isReady());
		queue.push_front(std::move(task));
		isEmpty.store(false, std::memory_order_relaxed);
	}

	void WorkQueue::enqueueBack(TaskPtr&& task) {
		guard g(lock);
		assert_true(task);
		assert_true(task->isReady());
		queue.push_back(std::move(task));
		isEmpty.store(false, std::memory_order_relaxed);
	}

	TaskPtr WorkQueue::dequeueFront() {
		if(isEmpty) return {};

		guard g(lock);
		if (queue.empty()) return {};
		auto res = std::move(queue.front());
		queue.pop_front();
		assert_true(res);
		assert_true(res->isReady());

		if(queue.empty()) isEmpty.store(true, std::memory_order_relaxed);

		return res;
	}

	TaskPtr WorkQueue::dequeueBack() {
		if (isEmpty) return {};

		guard g(lock);
		if (queue.empty()) return {};
		auto res = std::move(queue.back());
		queue.pop_back();
		assert_true(res);
		assert_true(res->isReady());

		if(queue.empty()) isEmpty.store(true, std::memory_order_relaxed);

		return res;
	}


} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale
