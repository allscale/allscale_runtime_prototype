#pragma once

#include <iomanip>
#include <map>

#include "allscale/utils/serializer.h"
#include "allscale/runtime/com/node.h"

namespace allscale {
namespace runtime {
namespace work {

	/**
	 * A summary of the task processing statistics of one node.
	 */
	struct TaskStatisticEntry : public allscale::utils::trivially_serializable {
		com::rank_t rank = 0;
		std::uint32_t split_tasks = 0;
		std::uint32_t processed_tasks = 0;
		double estimated_workload = 0;
	};

	/**
	 * A system wide aggregation of task processing statistics.
	 */
	class TaskStatistic {

		std::map<com::rank_t,TaskStatisticEntry> entries;

	public:

		void add(const TaskStatisticEntry& entry) {
			entries[entry.rank] = entry;
		}

		friend std::ostream& operator<<(std::ostream& out, const TaskStatistic& stats);

	};

} // end namespace work
} // end namespace runtime
} // end namespace allscale
