/**
 * Provides an implementation for a network statistic service.
 */
#pragma once

#include <cstdint>
#include <ostream>
#include <vector>

#include "allscale/utils/serializer.h"
#include "allscale/runtime/com/node.h"

namespace allscale {
namespace runtime {
namespace com {

	/**
	 * The statistics entry per node.
	 */
	struct NodeStatistics : public utils::trivially_serializable {

		std::uint64_t sent_bytes = 0;			// < number of sent bytes
		std::uint64_t received_bytes = 0;		// < number of received bytes
		std::uint32_t sent_calls = 0;	  		// < number of sent calls
		std::uint32_t received_calls = 0;		// < number or received calls
		std::uint32_t sent_bcasts = 0;			// < number of sent broadcasts
		std::uint32_t received_bcasts = 0;		// < number of received broadcasts

		friend std::ostream& operator<<(std::ostream&,const NodeStatistics&);

		NodeStatistics& operator+=(const NodeStatistics& other);

	};



	/**
	 * Data and message transfer statistics on a network.
	 */
	class NetworkStatistics {

		/**
		 * The per-node statistics maintained in a list.
		 */
		std::vector<NodeStatistics> stats;

	public:

		/**
		 * Creates a new instance for the given number of nodes.
		 */
		NetworkStatistics(size_t size) : stats(size) {}

		/**
		 * Obtain the statistics of some node.
		 */
		NodeStatistics& operator[](rank_t rank) {
			assert_lt(rank, stats.size());
			return stats[rank];
		}

		/**
		 * Obtain the statistics of some node.
		 */
		const NodeStatistics& operator[](rank_t rank) const {
			assert_lt(rank, stats.size());
			return stats[rank];
		}

		/**
		 * Support printing of statistics.
		 */
		friend std::ostream& operator<<(std::ostream&,const NetworkStatistics&);

	};


	class NetworkStatisticService {

		NodeStatistics local;

	public:

		NetworkStatisticService(const Node&) {};

		/**
		 * Obtains the network statistics of the local node.
		 */
		NodeStatistics getNodeStats() const {
			return local;
		}

		/**
		 * Obtains a mutable local node statistics.
		 */
		NodeStatistics& getLocalNodeStats() {
			return local;
		}

		/**
		 * Reset the node statistics for this node.
		 */
		void resetNodeStats() {
			local = NodeStatistics();
		}

	};


} // end namespace com
} // end namespace runtime
} // end namespace allscale
