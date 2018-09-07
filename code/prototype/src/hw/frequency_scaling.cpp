#include "allscale/runtime/hw/energy.h"

#include "allscale/utils/assert.h"

namespace allscale {
namespace runtime {
namespace hw {

	std::vector<Frequency> getFrequencyOptions(Core) {
		return {};	// currently no options are available
	}

	Frequency getFrequency(Core) {
		return 2.2_GHz;	// dummy value
	}

	bool setFrequency(Core, Frequency) {
		return false; // change not supported yet
	}

} // end of namespace hw
} // end of namespace runtime
} // end of namespace allscale
