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
		std::chrono::milliseconds processingTime {0};
		std::chrono::milliseconds idleTime {0};
		std::chrono::milliseconds overheadTime {0};

		out << "rank, split_tasks, processed_tasks, est_workload, processing[ms],   idle[ms], overhead[ms]\n";
		for(const auto& cur : stats.entries) {
			auto& entry = cur.second;

			auto overhead = entry.totalRuntime - (entry.processingTime + entry.idleTime);

			out << std::setw( 4) << entry.rank << ",";
			out << std::setw(12) << entry.split_tasks << ",";
			out << std::setw(16) << entry.processed_tasks << ",";
			out << std::setw(13) << entry.estimated_workload << ",";

			out << std::setw(15) << entry.processingTime.count() << ",";
			out << std::setw(11) << entry.idleTime.count() << ",";
			out << std::setw(13) << overhead.count() << "\n";

			// aggregate
			total_split += entry.split_tasks;
			total_processed += entry.processed_tasks;
			total_workload += entry.estimated_workload;

			processingTime += entry.processingTime;
			idleTime += entry.idleTime;
			overheadTime += overhead;
		}
		out << "------------------------------------------------------------------------------------------\n";
		out << " sum,";
		out << std::setw(12) << total_split << ",";
		out << std::setw(16) << total_processed << ",";
		out << std::setw(13) << total_workload << ",";
		out << std::setw(15) << processingTime.count() << ",";
		out << std::setw(11) << idleTime.count() << ",";
		out << std::setw(13) << overheadTime.count() << "\n";

		out << "------------------------------------------------------------------------------------------\n";
		return out;
	}

} // end namespace work
} // end namespace runtime
} // end namespace allscale
