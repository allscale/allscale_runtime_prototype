#pragma once

#include <ostream>
#include <vector>

#include "allscale/utils/serializer.h"

#include "allscale/runtime/com/network.h"
#include "allscale/runtime/data/data_item_region.h"

namespace allscale {
namespace runtime {
namespace mon {

	struct NodeState {

		// the node this state is describing
		com::rank_t rank;

		// the time this state was recorded
		std::uint64_t time;

		// the state of this node
		bool online = false;

		// the current CPU load (snapshot)
		float cpu_load = 0.0f;

		// the current memory usage
		std::uint64_t memory_load = 0;

		// number of tasks processed per second
		float task_throughput = 0.0f;

		// number of weighted tasks processed per second
		float weighted_task_throughput = 0.0f;

		// received network throughput
		std::uint64_t network_in = 0;

		// transmitted network throughput
		std::uint64_t network_out = 0;

		// percentage of time being idle
		float idle_rate = 0.0f;

		// the data owned by this node
		data::DataItemRegions ownership;

		// -- printing --

		friend std::ostream& operator<<(std::ostream&,const NodeState&);

		void toJSON(std::ostream&) const;

		// -- serialization --

		void store(allscale::utils::ArchiveWriter&) const;

		static NodeState load(allscale::utils::ArchiveReader&);

	};

	/**
	 * Installs the dash board service on the given network.
	 */
	void installDashbordService(com::Network&);

	/**
	 * Shuts down and removes the dash board service
	 */
	void shutdownDashbordService(com::Network&);

	/**
	 * Retrieves the current state of all nodes in the given network.
	 */
	std::vector<NodeState> getSystemState(com::Network&);


} // end of namespace mon
} // end of namespace runtime
} // end of namespace allscale
