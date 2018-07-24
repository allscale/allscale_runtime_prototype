
#include "allscale/runtime/work/work_queue.h"

namespace allscale {
namespace runtime {
namespace work {


	using guard = std::lock_guard<std::mutex>;

	bool WorkQueue::empty() const {
		guard g(lock);
		return queue.empty();
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
	}

	void WorkQueue::enqueueBack(TaskPtr&& task) {
		guard g(lock);
		assert_true(task);
		assert_true(task->isReady());
		queue.push_back(std::move(task));
	}

	TaskPtr WorkQueue::dequeueFront() {
		guard g(lock);
		if (queue.empty()) return {};
		auto res = std::move(queue.front());
		queue.pop_front();
		assert_true(res);
		assert_true(res->isReady());
		return std::move(res);
	}

	TaskPtr WorkQueue::dequeueBack() {
		guard g(lock);
		if (queue.empty()) return {};
		auto res = std::move(queue.back());
		queue.pop_back();
		assert_true(res);
		assert_true(res->isReady());
		return std::move(res);
	}


} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale
