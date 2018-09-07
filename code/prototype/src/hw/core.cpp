#include "allscale/runtime/hw/core.h"

#include <numeric>
#include <thread>

namespace allscale {
namespace runtime {
namespace hw {

	unsigned getNumberAvailableCores() {
		return std::thread::hardware_concurrency();
	}

	std::vector<Core> getAvailableCores() {
		int n = getNumberAvailableCores();
		std::vector<Core> res(n);
		std::iota(res.begin(),res.end(),0);
		return res;
	}

} // end of namespace hw
} // end of namespace runtime
} // end of namespace allscale
