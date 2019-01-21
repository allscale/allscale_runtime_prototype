#include "allscale/runtime/work/task_statistic.h"

namespace allscale {
namespace runtime {
namespace work {

	std::ostream& operator<<(std::ostream& out, const TaskStatistic& stats) {
		out << "------------------------------------------------------------------------------------------\n";
		out << "Task execution statistics:\n";
		out << "------------------------------------------------------------------------------------------\n";

		std::uint32_t total_split = 0;
		std::uint32_t total_processed = 0;
		double total_workload = 0;

		out << "rank, split_tasks, processed_tasks, est_workload\n";
		for(const auto& cur : stats.entries) {
			auto& entry = cur.second;
			out << std::setw( 4) << entry.rank << ",";
			out << std::setw(12) << entry.split_tasks << ",";
			out << std::setw(16) << entry.processed_tasks << ",";
			out << std::setw(13) << entry.estimated_workload << "\n";

			// aggregate
			total_split += entry.split_tasks;
			total_processed += entry.processed_tasks;
			total_workload += entry.estimated_workload;
		}
		out << "------------------------------------------------------------------------------------------\n";
		out << " sum,";
		out << std::setw(12) << total_split << ",";
		out << std::setw(16) << total_processed << ",";
		out << std::setw(13) << total_workload << "\n";
		out << "------------------------------------------------------------------------------------------\n";
		return out;
	}

} // end namespace work
} // end namespace runtime
} // end namespace allscale
