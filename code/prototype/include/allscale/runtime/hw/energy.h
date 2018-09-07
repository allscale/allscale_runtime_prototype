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
	class Energy : public allscale::utils::Scalar<double,Energy> {

		using super = allscale::utils::Scalar<double,Energy>;

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

	/**
	 * A type to present power. Power is represented
	 * as a scalar value, storing the amount of watts.
	 */
	class Power : public allscale::utils::Scalar<double,Power> {

		using super = allscale::utils::Scalar<double,Power>;

	public:

		Power() = default;

		Power(double value) : super(value) {}

		double toWatt() const {
			return getValue();
		}

		friend std::ostream& operator<<(std::ostream& out, const Power& e) {
			return out << e.getValue() << " W";
		}

	};

	inline Power operator "" _W(long double w) {
		return w;
	}

	inline Power operator "" _w(unsigned long long w) {
		return w;
	}

	inline Power operator "" _mW(long double w) {
		return w * 1e-3;
	}

	inline Power operator "" _mW(unsigned long long w) {
		return w * 1e-3;
	}

	inline Power operator "" _uW(long double w) {
		return w * 1e-6;
	}

	inline Power operator "" _uW(unsigned long long w) {
		return w * 1e-6;
	}

	inline Power operator "" _nW(long double w) {
		return w * 1e-9;
	}

	inline Power operator "" _nW(unsigned long long w) {
		return w * 1e-9;
	}

	inline Power operator "" _pW(long double w) {
		return w * 1e-12;
	}

	inline Power operator "" _pW(unsigned long long w) {
		return w * 1e-12;
	}

	template<typename Rep, typename Ratio>
	Power operator/(const Energy& e, const std::chrono::duration<Rep,Ratio>& d) {
		return e.toJoule() / std::chrono::duration_cast<std::chrono::duration<double,std::ratio<1>>>(d).count();
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
