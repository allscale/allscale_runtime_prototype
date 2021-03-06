
#include "allscale/runtime/work/task.h"

#include "allscale/utils/unused.h"

namespace allscale {
namespace runtime {
namespace work {

	__attribute__ ((noinline))
	Task*& tl_current_task() {
		static thread_local Task* current_task = nullptr;
		return current_task;
	}

	const data::DataItemRequirements& Task::getProcessRequirements() const {
		static const data::DataItemRequirements empty;
		return empty;
	}

	void Task::process() {

		// update state to running
		State st = Ready;
		__allscale_unused bool success = state.compare_exchange_strong(st,Running);
		assert_true(success) << "Attempted to start non-ready task, actual state: " << st << "\n";

		// set up current task
		tl_current_task() = this;

		// process task
		processInternal();

		// update state done
		st = Running;
		success = state.compare_exchange_strong(st,Finished);
		assert_true(success) << "Attempted to finish non-running task, actual state: " << st << "\n";

	}

	const data::DataItemRequirements& Task::getSplitRequirements() const {
		static const data::DataItemRequirements empty;
		return empty;
	}

	void Task::split() {

		// update state to running
		State st = Ready;
		__allscale_unused bool success = state.compare_exchange_strong(st,Running);
		assert_true(success) << "Attempted to start non-ready task, actual state: " << st << "\n";

		// set up current task
		tl_current_task() = this;

		// split this task
		splitInternal();

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

	void Task::notifyResume(Task* task) {
		// reset thread local task reference
		tl_current_task() = task;
	}

	Task* Task::getCurrent() {
		return tl_current_task();
	}

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
