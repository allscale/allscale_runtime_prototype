
#include "allscale/runtime/work/task.h"

#include "allscale/utils/unused.h"

#include "allscale/utils/fiber.h"

namespace allscale {
namespace runtime {
namespace work {

	thread_local Task* tl_current_task = nullptr;

	data::DataItemRequirements Task::getProcessRequirements() const {
		return {};
	}

	void Task::process() {

		// update state to running
		State st = Ready;
		__allscale_unused bool success = state.compare_exchange_strong(st,Running);
		assert_true(success) << "Attempted to start non-ready task, actual state: " << st << "\n";

		// set up current task
		auto oldTask = tl_current_task;
		tl_current_task = this;

		// process task
		processInternal();

		// reset up current task
		assert_eq(tl_current_task,this);
		tl_current_task = oldTask;

		// update state done
		st = Running;
		success = state.compare_exchange_strong(st,Finished);
		assert_true(success) << "Attempted to finish non-running task, actual state: " << st << "\n";

	}

	data::DataItemRequirements Task::getSplitRequirements() const {
		return {};
	}

	void Task::split() {

		// update state to running
		State st = Ready;
		__allscale_unused bool success = state.compare_exchange_strong(st,Running);
		assert_true(success) << "Attempted to start non-ready task, actual state: " << st << "\n";

		// set up current task
		auto oldTask = tl_current_task;
		tl_current_task = this;

		// split this task
		splitInternal();

		// reset up current task
		assert_eq(tl_current_task,this);
		tl_current_task = oldTask;

		// update state done
		st = Running;
		success = state.compare_exchange_strong(st,Finished);
		assert_true(success) << "Attempted to finish non-running task, actual state: " << st << "\n";

	}

	// support printing of task states
	std::ostream& operator<<(std::ostream& out, const Task::State& state) {
		switch(state) {
		case Task::Ready:    return out << "Ready";
		case Task::Running:  return out << "Running";
		case Task::Finished: return out << "Finished";
		}
		return out << "?";
	}

	// support printing of tasks
	std::ostream& operator<<(std::ostream& out, const Task& task) {
		return out << task.getId() << ":" << task.state;
	}

	void Task::notifySuspend(Task* task) {
		// nothing to do yet ..
		std::cout << "\t\tSuspending " << task->getId() << " in " << allscale::utils::FiberPool::getCurrentFiber() << "\n";
	}

	void Task::notifyResume(Task* task) {
		// reset thread local task reference
		tl_current_task = task;
		std::cout << "\t\tResuming " << task->getId() << "\n";
	}

	Task* Task::getCurrent() {
		return tl_current_task;
	}

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
