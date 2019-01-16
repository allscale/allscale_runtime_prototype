#include "allscale/runtime/hw/model.h"

#include <cstdlib>
#include <thread>

namespace allscale {
namespace runtime {
namespace hw {

	const static char* ART_NUM_WORKERS = "ART_NUM_WORKERS";
	const static char* ART_AFFINITY_FILL = "ART_AFFINITY_FILL";


	std::vector<WorkerConfig> getWorkerPoolConfig(com::rank_t rank) {
		std::vector<WorkerConfig> res;

		int numWorkers = 1;
		if (auto str = std::getenv(ART_NUM_WORKERS)) {
			numWorkers = std::atoi(str);
		}

		numWorkers = std::max(numWorkers,1);

		// bind affinity if requested
		if (std::getenv(ART_AFFINITY_FILL)) {

			// crate configurations
			for(int i=0; i<numWorkers; i++) {
				cpu_set_t cores;
				CPU_ZERO(&cores);
				CPU_SET(rank*numWorkers+i,&cores);
				res.push_back({ cores });
			}

		} else {


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

		}

		// that's it
		return res;
	}


} // end namespace hw
} // end namespace runtime
} // end namespace allscale
