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
		com::rank_t rank;
		std::uint32_t split_tasks;
		std::uint32_t processed_tasks;
		double estimated_workload;
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

		friend std::ostream& operator<<(std::ostream& out, const TaskStatistic& stats) {
			out << "rank, split_tasks, processed_tasks, est_workload\n";
			for(const auto& cur : stats.entries) {
				auto& entry = cur.second;
				out << std::setw( 4) << entry.rank << ",";
				out << std::setw(12) << entry.split_tasks << ",";
				out << std::setw(16) << entry.processed_tasks << ",";
				out << std::setw(13) << entry.estimated_workload << "\n";
			}
			return out;
		}

	};

} // end namespace work
} // end namespace runtime
} // end namespace allscale
