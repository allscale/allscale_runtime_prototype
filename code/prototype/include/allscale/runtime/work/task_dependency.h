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

#include "allscale/utils/serializer/vectors.h"
#include "allscale/utils/serializer/optionals.h"

#include "allscale/runtime/work/task_reference.h"

namespace allscale {
namespace runtime {
namespace work {

	/**
	 * Models a single dependency to a task.
	 */
	class TaskDependency {

		friend class TaskDependencies;

		using TaskRefOpt = allscale::utils::optional<TaskRef>;

		// the task depending on, none if not depending
		mutable TaskRefOpt reference;

	public:

		// creates a dependency always satisfied
		TaskDependency() {};

		// creates a dependency on the given reference
		TaskDependency(const TaskRef& ref) : reference(ref) {}

		// a constructor accepting an optional reference
		TaskDependency(const TaskRefOpt& ref) : reference(ref) {}

		// a constructor accepting an optional reference
		TaskDependency(TaskRefOpt&& ref) : reference(std::move(ref)) {}

		// -- features --

		void wait() const;

		// -- serialization support --

		void store(allscale::utils::ArchiveWriter& out) const {
			out.write(reference);
		}

		static TaskDependency load(allscale::utils::ArchiveReader& in) {
			return in.read<TaskRefOpt>();
		}

		// -- print support --

		friend std::ostream& operator<<(std::ostream& out, const TaskDependency&);

	};

	/**
	 * Represents a set of dependencies.
	 */
	class TaskDependencies {

		std::vector<TaskDependency> dependencies;

	public:

		template<typename ... Dependencies>
		TaskDependencies(Dependencies&& ... deps)
			: dependencies{ std::move(deps) ... } {}

		TaskDependencies(std::vector<TaskDependency>&& deps)
			: dependencies(deps) {}

		// -- features --

		void wait() const;

		// -- serialization support --

		void store(allscale::utils::ArchiveWriter& out) const {
			out.write(dependencies);
		}

		static TaskDependencies load(allscale::utils::ArchiveReader& in) {
			return in.read<std::vector<TaskDependency>>();
		}

		// -- print support --

		friend std::ostream& operator<<(std::ostream& out, const TaskDependencies&);

	};

} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale
