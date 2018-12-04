#include "allscale/runtime/hw/model.h"

#include <cstdlib>
#include <thread>

namespace allscale {
namespace runtime {
namespace hw {

	const static char* ART_NUM_WORKERS = "ART_NUM_WORKERS";


	std::vector<WorkerConfig> getWorkerPoolConfig(com::rank_t) {
		std::vector<WorkerConfig> res;

		int numWorkers = 1;
		if (auto str = std::getenv(ART_NUM_WORKERS)) {
			numWorkers = std::atoi(str);
		}

		numWorkers = std::max(numWorkers,1);

		// create a mask allowing all workers to be active on all cores
		cpu_set_t allCores;
		CPU_ZERO(&allCores);
		for(unsigned i=0; i<std::thread::hardware_concurrency(); i++) {
			CPU_SET(i, &allCores);
		}

		// create configurations
		for(int i=0; i<numWorkers; i++) {
			res.push_back({ allCores });
		}

		// that's it
		return res;
	}


} // end namespace hw
} // end namespace runtime
} // end namespace allscale
