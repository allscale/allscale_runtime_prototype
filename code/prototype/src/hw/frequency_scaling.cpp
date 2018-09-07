#include "allscale/runtime/hw/energy.h"

#include <algorithm>

#include "allscale/utils/assert.h"
#include "allscale/runtime/hw/core.h"

namespace allscale {
namespace runtime {
namespace hw {

	// a dummy implementation

	namespace dummy {

		const std::vector<Frequency> options = { 1.0_GHz, 1.8_GHz, 2.2_GHz, 2.4_GHz };

		std::vector<Frequency> initialSetup() {
			return std::vector<Frequency>(getNumberAvailableCores(),2.2_GHz);
		}

		std::vector<Frequency> currentFrequencies = initialSetup();

	}

	std::vector<Frequency> getFrequencyOptions(Core) {
		// dummy options
		return dummy::options;	// currently no options are available
	}

	Frequency getFrequency(Core c) {
		assert_lt(c,dummy::currentFrequencies.size());
		return dummy::currentFrequencies[c];
	}

	bool setFrequency(Core c, Frequency f) {
		assert_lt(c,dummy::currentFrequencies.size());
		assert_true(std::binary_search(dummy::options.begin(),dummy::options.end(),f));
		dummy::currentFrequencies[c] = f;
		return true; // update successful
	}

} // end of namespace hw
} // end of namespace runtime
} // end of namespace allscale
