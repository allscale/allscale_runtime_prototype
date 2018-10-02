#pragma once

#include <ostream>
#include <vector>

#include "allscale/utils/serializer.h"

#include "allscale/runtime/com/network.h"
#include "allscale/runtime/data/data_item_region.h"
#include "allscale/runtime/hw/energy.h"
#include "allscale/runtime/hw/frequency.h"
#include "allscale/runtime/work/scheduler.h"

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

		// is this node active according to the current scheduler
		bool active = false;


		// -- CPU load --

		// the number of cores involved on this node
		int num_cores = 1;

		// the current CPU load (snapshot)
		float cpu_load = 0.0f;


		// -- CPU core frequency --

		hw::Frequency max_frequency = hw::Frequency::GHz(1);

		hw::Frequency cur_frequency = hw::Frequency::GHz(1);


		// -- memory state --

		// the current memory usage (in bytes)
		std::uint64_t memory_load = 0;

		// the total available physical memory (in bytes)
		std::uint64_t total_memory = 0;


		// -- worker state --

		// number of tasks processed per second
		float task_throughput = 0.0f;

		// number of weighted tasks processed per second
		float weighted_task_throughput = 0.0f;

		// percentage of time being idle
		float idle_rate = 0.0f;


		// -- network I/O --

		// received network throughput (in bytes/s)
		std::uint64_t network_in = 0;

		// transmitted network throughput (in bytes/s)
		std::uint64_t network_out = 0;


		// -- Data distribution --

		// the data owned by this node
		data::DataItemRegions ownership;


		// -- multi-objective metrics --

		// the number of productive cycles spend per second
		hw::cycles productive_cycles_per_second = 0;

		// the maximum power consumption per second
		hw::Power cur_power = 0;

		// the maximum energy consumptoin per second
		hw::Power max_power = 0;

		// cycles of progress / max available cycles \in [0..1]
		float speed = 0;

		// cycles of progress / current available cycles \in [0..1]
		float efficiency = 0;

		// current power usage / max power usage \in [0..1]
		float power = 0;


		// -- printing --

		friend std::ostream& operator<<(std::ostream&,const NodeState&);

		void toJSON(std::ostream&) const;

		// -- serialization --

		void store(allscale::utils::ArchiveWriter&) const;

		static NodeState load(allscale::utils::ArchiveReader&);

	};

	struct SystemState {

		// the time this state was recorded
		std::uint64_t time;


		// -- multi-objective metrics --

		// cycles of progress / max available cycles on all nodes \in [0..1]
		float speed = 0;

		// cycles of progress / current available cycles on active nodes \in [0..1]
		float efficiency = 0;

		// current power usage / max power usage on all nodes \in [0..1]
		float power = 0;

		// the overall system-wide score of the objective function
		float score = 0;

		// -- scheduler state --

		work::SchedulerType scheduler;

		// -- the node states --

		// the state of the nodes
		std::vector<NodeState> nodes;


		// -- printing --

		friend std::ostream& operator<<(std::ostream&,const SystemState&);

		void toJSON(std::ostream&) const;

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
	 * Retrieves the current state of all nodes and the overall system in the given network.
	 */
	SystemState getSystemState(com::Network&);


} // end of namespace mon
} // end of namespace runtime
} // end of namespace allscale
