
#include "allscale/runtime/work/task.h"

namespace allscale {
namespace runtime {
namespace work {


	void Task::process() {

		// update state to running
		State st = Ready;
		bool success = state.compare_exchange_strong(st,Running);
		assert_true(success) << "Attempted to start non-ready task, actual state: " << st << "\n";

		// process task
		processInternal();

		// update state done
		st = Running;
		success = state.compare_exchange_strong(st,Finished);
		assert_true(success) << "Attempted to finish non-running task, actual state: " << st << "\n";

	}

	void Task::split() {

		// update state to running
		State st = Ready;
		bool success = state.compare_exchange_strong(st,Running);
		assert_true(success) << "Attempted to start non-ready task, actual state: " << st << "\n";

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


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
