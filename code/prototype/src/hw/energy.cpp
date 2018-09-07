#include "allscale/runtime/hw/energy.h"

#include "allscale/utils/assert.h"

namespace allscale {
namespace runtime {
namespace hw {


	Energy estimateEnergyUsage(Frequency f, cycles c) {
		// simply estimate 100 pJ per cycle
		// TODO: integrate frequency

		// estimation: something like  C * f * U^2 * cycles
		// where C is some hardware constant
		auto C = 100_pJ;

		// voltage U should be adapted to the frequency
		auto U = 0.8; // TODO: adapt to frequency

		// so we obtain
		return C * f.toHz() * (U*U) * c;
	}

	bool providesEnergyMeasurement(Core) {
		return false; // not implemented yet
	}

	Energy getEnergyConsumedOn(Core) {
		assert_not_implemented() << "No power measurement available yet";
		return 0_J;
	}


} // end of namespace hw
} // end of namespace runtime
} // end of namespace allscale
