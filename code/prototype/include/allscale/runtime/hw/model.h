#pragma once

#include <vector>
#include <pthread.h>

#include "allscale/runtime/com/node.h"

namespace allscale {
namespace runtime {
namespace hw {

	/**
	 * Configuration parameters of a given worker.
	 */
	struct WorkerConfig {

		// the core affinity mask to be enforced (may be all cores)
		cpu_set_t affinityMask;

	};

	/**
	 * Obtains the configuration of all workers to be maintained on the given rank.
	 */
	std::vector<WorkerConfig> getWorkerPoolConfig(com::rank_t);


} // end namespace hw
} // end namespace runtime
} // end namespace allscale
