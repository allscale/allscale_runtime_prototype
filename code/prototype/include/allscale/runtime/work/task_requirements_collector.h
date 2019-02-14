/*
 * Infrastructure to realize task dependencies.
 *
 *  Created on: Jan 21, 2019
 *      Author: herbert
 */

#pragma once

#include <vector>

#include "allscale/utils/assert.h"
#include "allscale/utils/optional.h"

#include "allscale/runtime/data/data_item_requirement.h"
#include "allscale/runtime/data/data_item_manager.h"
#include "allscale/runtime/work/task_dependency.h"

namespace allscale {
namespace runtime {
namespace work {

	class TaskRequirementsCollector {

		// the node working for
		com::Node& node;

		// the data item manager to work with
		data::DataItemManagerService& dataItemManager;

		// the task dependencies to be considered
		const work::TaskDependencies* dependencies = nullptr;

		// the data requirements to be retrieved
		const data::DataItemRequirements* requirements = nullptr;

		// the event triggered once dependencies are covered
		allscale::utils::fiber::EventId dependenciesDone;

		// the event triggered once data requirements are covered
		allscale::utils::fiber::EventId requirementsDone;

	public:

		TaskRequirementsCollector(com::Node& node, data::DataItemManagerService& dim)
			: node(node), dataItemManager(dim) {};

		TaskRequirementsCollector(const TaskRequirementsCollector&) = delete;
		TaskRequirementsCollector(TaskRequirementsCollector&&) = delete;

		~TaskRequirementsCollector();

		/**
		 * Tasks this collector to acquire the given requirements considering the
		 * stated task dependencies.
		 *
		 * This function may only be called once. Dependencies and requirements must
		 * survive the life-cycle of the collector object.
		 */
		void acquire(const work::TaskDependencies& deps, const data::DataItemRequirements& reqs);

		/**
		 * Waits for the availability of the given requirements.
		 *
		 * The requirements must be a subset of the requirements set up using the acquire call.
		 */
		void waitFor(const data::DataItemRequirements& reqs) const;

		/**
		 * Waits for the collection to be completed.
		 */
		void wait() const;
	};

} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale
