#include <iomanip>

#include "allscale/runtime/com/sim/network.h"

namespace allscale {
namespace runtime {
namespace com {
namespace sim {

	Network::Network(size_t size) : stats(size) {
		nodes.reserve(size);
		for(size_t i=0; i<size; i++) {
			nodes.emplace_back(i);
		}
	}

	std::unique_ptr<Network> Network::create() {
		// get the number of nodes
		int num_nodes = 4;	// < by default we use four nodes
		if (auto val = std::getenv("ART_NUM_NODES")) {
			num_nodes = std::atoi(val);
			if (num_nodes < 1) num_nodes = 1;
		}
		return std::make_unique<Network>(num_nodes);
	}

	std::ostream& operator<<(std::ostream& out, const Network::Statistics::Entry& entry) {
		return out
				<< std::setw(15) << entry.received_bytes << ','
				<< std::setw(11) << entry.sent_bytes << ','
				<< std::setw(15) << entry.received_calls << ','
				<< std::setw(11) << entry.sent_calls
				<< std::setw(17) << entry.received_bcasts << ','
				<< std::setw(12) << entry.sent_bcasts;
 	}

	std::ostream& operator<<(std::ostream& out, const Network::Statistics& stats) {
		out << std::setw(10);
		out << "rank, bytes_received, bytes_sent, received_calls, sent_calls, received_bcasts, send_bcasts\n";
		for(std::size_t i=0; i<stats.stats.size(); i++) {
			out << std::setw(4) << i << "," << stats.stats[i] << "\n";
		}
		return out;
 	}

	static thread_local Network* tl_current_network;


	// obtains the enclosing network instance
	Network& Network::getNetwork() {
		assert_true(tl_current_network) << "No local network!";
		return *tl_current_network;
	}

	void Network::setLocalNetwork() const {
		assert_false(tl_current_network);
		tl_current_network = const_cast<Network*>(this);
	}

	void Network::resetLocalNetwork() const {
		assert_true(tl_current_network);
		tl_current_network = nullptr;
	}


} // end of namespace sim
} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
