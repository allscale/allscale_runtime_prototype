#include <iomanip>

#include "allscale/runtime/data/data_item_statistic.h"

namespace allscale {
namespace runtime {
namespace data {

	DataItemManagerStatisticEntry& DataItemManagerStatisticEntry::operator+=(const DataItemManagerStatisticEntry& other) {

		locate_calls += other.locate_calls;
		retrieve_calls += other.retrieve_calls;
		acquire_calls += other.acquire_calls;

		allocate_calls += other.allocate_calls;
		release_calls += other.release_calls;

		return *this;
	}

	std::ostream& operator<<(std::ostream& out, const DataItemManagerStatistic& stats) {
		out << "------------------------------------------------------------------------------------------\n";
		out << "Data Item Manager statistics:\n";
		out << "------------------------------------------------------------------------------------------\n";

		DataItemManagerStatisticEntry sum;

		out << "rank, locate_calls, retrieve_calls, acquire_calls, allocate_calls, release_calls\n";
		for(const auto& cur : stats.entries) {
			auto& entry = cur.second;
			out << std::setw( 4) << cur.first << ",";
			out << std::setw(13) << entry.locate_calls << ",";
			out << std::setw(15) << entry.retrieve_calls << ",";
			out << std::setw(14) << entry.acquire_calls << ",";
			out << std::setw(15) << entry.allocate_calls << ",";
			out << std::setw(14) << entry.release_calls << "\n";

			sum += entry;
		}
		out << "------------------------------------------------------------------------------------------\n";
		out << " sum,";
		out << std::setw(13) << sum.locate_calls << ",";
		out << std::setw(15) << sum.retrieve_calls << ",";
		out << std::setw(14) << sum.acquire_calls << ",";
		out << std::setw(15) << sum.allocate_calls << ",";
		out << std::setw(14) << sum.release_calls << "\n";

		out << "------------------------------------------------------------------------------------------\n";
		return out;
	}

} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
