#pragma once

#include <ostream>
#include <vector>
#include <chrono>

#include "allscale/utils/serializer.h"
#include "allscale/runtime/work/task_id.h"

namespace allscale {
namespace runtime {
namespace mon {

	using time_t = std::chrono::nanoseconds;

	/**
	 * A utility class to track the execution time of tasks and sub-tasks.
	 * Internally, times of tasks are aggregated on a unspecified level.
	 */
	struct TaskTimes {

		std::vector<time_t> times;

	public:

		TaskTimes();

		// -- modifiers --

		void add(const work::TaskID& id, const time_t& t);

		void add(const work::TaskPath& path, const time_t& t);


		// -- observers --

		time_t getTime(const work::TaskPath& path) const;


		// -- arithmetic --

		TaskTimes& operator+=(const TaskTimes& other);

		TaskTimes operator-(const TaskTimes& other) const;

		TaskTimes operator/(float f) const;

		// -- serialization --

		void store(allscale::utils::ArchiveWriter& out) const;

		static TaskTimes load(allscale::utils::ArchiveReader& in);

		// -- printing --

		friend std::ostream& operator<<(std::ostream&, const TaskTimes&);

	};

} // end of namespace mon
} // end of namespace runtime
} // end of namespace allscale
