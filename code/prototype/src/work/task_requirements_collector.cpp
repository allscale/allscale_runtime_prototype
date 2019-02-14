
#include <atomic>

#include "allscale/runtime/work/task_requirements_collector.h"


namespace allscale {
namespace runtime {
namespace work {

	namespace fi = allscale::utils::fiber;


	TaskRequirementsCollector::~TaskRequirementsCollector() {
		if (requirements) {
			dataItemManager.release(*requirements);
		}
	}

	void TaskRequirementsCollector::acquire(const TaskDependencies& deps, const data::DataItemRequirements& reqs) {

		// make sure this is only called once
		assert_false(dependencies);
		assert_false(requirements);

		// retrieve fiber context
		auto& fiberContext = node.getFiberContext();

		// set up dependencies
		if (!deps.empty()) {
			dependencies = &deps;
			dependenciesDone = fiberContext.getEventRegister().create();
		}

		// set up requirements
		if (!reqs.empty()) {
			requirements = &reqs;
			requirementsDone = fiberContext.getEventRegister().create();
		}

		// if there are no dependencies and no requirements, we are done
		if (!dependencies && !requirements) return;

		// start fiber retrieving data in fiber
		fiberContext.start([this]{

			// retrieve the event register to signal events
			auto& eventReg = node.getFiberContext().getEventRegister();

			// step 1: wait for dependencies
			if (dependencies) {
				dependencies->wait();
				eventReg.trigger(dependenciesDone);
			}


			// step 2: collect data
			if (requirements) {

				// allocate data
				dataItemManager.allocate(*requirements);

				// signal done
				eventReg.trigger(requirementsDone);
			}

		}, fi::Priority::HIGH);


	}

	void TaskRequirementsCollector::waitFor(const data::DataItemRequirements& reqs) const {

		// skip if there is nothing to wait for
		if (!dependencies && !requirements) return;

		// wait for task dependencies to be covered
		if (dependencies) {
			fi::suspend(dependenciesDone, fi::Priority::HIGH);
		}

		// if there are no (remote) requirements ..
		if (!requirements || dataItemManager.isCoveredLocally(reqs)) {
			// .. reduce priority of this fiber ..
			fi::suspend(fi::Priority::MEDIUM);
			return;	// .. and be done
		}

		// wait for data to be available
		fi::suspend(requirementsDone, fi::Priority::HIGH);

		// reduce priority and be done
		fi::suspend(fi::Priority::MEDIUM);
		return;
	}

	void TaskRequirementsCollector::wait() const {
		fi::suspend(dependenciesDone);
		fi::suspend(requirementsDone);
	}


} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale
