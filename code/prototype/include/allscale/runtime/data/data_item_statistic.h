#pragma once

#include <map>

#include "allscale/utils/serializer.h"
#include "allscale/runtime/com/node.h"

namespace allscale {
namespace runtime {
namespace data {

	/**
	 * A summary of the task processing statistics of one node.
	 */
	struct DataItemManagerStatisticEntry : public allscale::utils::trivially_serializable {
		uint64_t locate_calls = 0;
		uint64_t retrieve_calls = 0;
		uint64_t acquire_calls = 0;

		uint64_t allocate_calls = 0;
		uint64_t release_calls = 0;

		DataItemManagerStatisticEntry& operator+=(const DataItemManagerStatisticEntry&);
	};

	/**
	 * A system wide aggregation of task processing statistics.
	 */
	class DataItemManagerStatistic {

		std::map<com::rank_t,DataItemManagerStatisticEntry> entries;

	public:

		void add(com::rank_t rank, const DataItemManagerStatisticEntry& entry) {
			entries[rank] = entry;
		}

		friend std::ostream& operator<<(std::ostream& out, const DataItemManagerStatistic& stats);

	};

} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
