
#include <atomic>

#include "allscale/runtime/work/task_id.h"
#include "allscale/runtime/work/task.h"

namespace allscale {
namespace runtime {
namespace work {

	TaskID getFreshId() {
		static std::atomic<int> taskCounter(0);
		return ++taskCounter;
	}

	TaskID getNewChildId() {
		Task* current_task = Task::getCurrent();
		assert_true(current_task) << "No current task in active context!";

		// increment number of children
		auto pos = current_task->num_children++;

		// determine child's id
		if (pos == 0) {
			return current_task->getId().getLeftChild();
		} else if (pos == 1) {
			return current_task->getId().getRightChild();
		} else {
			assert_not_implemented() << "Unsupported number of children: " << pos;
		}
		return {};
	}

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
