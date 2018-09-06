#include <iomanip>

#include "allscale/runtime/com/statistics.h"
#include "allscale/runtime/com/network.h"

namespace allscale {
namespace runtime {
namespace com {

	std::ostream& operator<<(std::ostream& out, const NodeStatistics& entry) {
		return out
			<< std::setw(15) << entry.received_bytes << ','
			<< std::setw(11) << entry.sent_bytes << ','
			<< std::setw(15) << entry.received_calls << ','
			<< std::setw(11) << entry.sent_calls
			<< std::setw(17) << entry.received_bcasts << ','
			<< std::setw(12) << entry.sent_bcasts;
	}

	std::ostream& operator<<(std::ostream& out, const NetworkStatistics& stats) {
		out << std::setw(10);
		out << "rank, bytes_received, bytes_sent, received_calls, sent_calls, received_bcasts, send_bcasts\n";
		for(std::size_t i=0; i<stats.stats.size(); i++) {
			out << std::setw(4) << i << "," << stats.stats[i] << "\n";
		}
		return out;
	}

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
