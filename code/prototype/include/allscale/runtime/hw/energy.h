#pragma once

#include "allscale/utils/scalar.h"
#include "allscale/runtime/hw/core.h"
#include "allscale/runtime/hw/frequency.h"

namespace allscale {
namespace runtime {
namespace hw {

	// -- representation --

	/**
	 * A type to present energy. Energy is represented
	 * as a scalar value, storing the amount of joules.
	 */
	class Energy : public utils::Scalar<double,Energy> {

		using super = utils::Scalar<double,Energy>;

	public:

		Energy() = default;

		Energy(double value) : super(value) {}

		double toJoule() const {
			return getValue();
		}

		friend std::ostream& operator<<(std::ostream& out, const Energy& e) {
			return out << e.getValue() << " J";
		}

	};

	inline Energy operator "" _J(long double j) {
		return j;
	}

	inline Energy operator "" _J(unsigned long long j) {
		return j;
	}

	inline Energy operator "" _mJ(long double j) {
		return j * 1e-3;
	}

	inline Energy operator "" _mJ(unsigned long long j) {
		return j * 1e-3;
	}

	inline Energy operator "" _uJ(long double j) {
		return j * 1e-6;
	}

	inline Energy operator "" _uJ(unsigned long long j) {
		return j * 1e-6;
	}

	inline Energy operator "" _nJ(long double j) {
		return j * 1e-9;
	}

	inline Energy operator "" _nJ(unsigned long long j) {
		return j * 1e-9;
	}

	inline Energy operator "" _pJ(long double j) {
		return j * 1e-12;
	}

	inline Energy operator "" _pJ(unsigned long long j) {
		return j * 1e-12;
	}

	// -- functionality --

	/**
	 * Provides an estimate for the energy consumed when processing
	 * the given number of cycles with the given clock frequency.
	 */
	Energy estimateEnergyUsage(Frequency f, cycles c);

	/**
	 * Tests whether there are energy measurements available on the given core.
	 */
	bool providesEnergyMeasurement(Core c);

	/**
	 * Obtains a measurement of the energy consumed on the given core
	 * since some arbitrary point in the past. This will be used to
	 * observe the increments over time.
	 *
	 * @param c the core to obtain the measurement from.
	 */
	Energy getEnergyConsumedOn(Core c);

} // end of namespace hw
} // end of namespace runtime
} // end of namespace allscale
