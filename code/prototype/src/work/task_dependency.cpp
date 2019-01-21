
#include <atomic>

#include "allscale/utils/printer/vectors.h"

#include "allscale/runtime/work/task_dependency.h"
#include "allscale/runtime/work/treeture.h"


namespace allscale {
namespace runtime {
namespace work {


	void TaskDependency::wait() const {

		// if there is no reference, don't wait
		if (!reference) return;

		// wait for completion
		//std::cout << "Waiting for " << *reference << " ..\n";
		work::TreetureStateService::getLocal().wait(*reference);
		//std::cout << "Dependency to " << *reference << " completed!\n";

		// reset internal reference to not wait again
		//reference = allscale::utils::optional<TaskRef>();

	}


	std::ostream& operator<<(std::ostream& out, const TaskDependency& dep) {
		if (!bool(dep.reference)) {
			return out << "none";
		}
		return out << *dep.reference;
	}

	std::ostream& operator<<(std::ostream& out, const TaskDependencies& deps) {
		return out << deps.dependencies;
	}

} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale
