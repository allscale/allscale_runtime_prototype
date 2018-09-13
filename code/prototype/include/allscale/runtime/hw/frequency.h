#pragma once

#include <ostream>
#include <chrono>

#include "allscale/utils/scalar.h"

namespace allscale {
namespace runtime {
namespace hw {

	// define the cycle type
	using cycles = unsigned long long;

	/**
	 * A utility type to represent CPU clock frequency.
	 */
	class Frequency : public allscale::utils::Scalar<cycles, Frequency> {

		using super = allscale::utils::Scalar<cycles, Frequency>;

	public:

		Frequency() = default;

		Frequency(cycles f) : super(f) {};


		// --- factories ---

		static Frequency Hz(cycles f) {
			return f;
		}

		static Frequency kHz(cycles f) {
			return f * 1000;
		}

		static Frequency MHz(cycles f) {
			return f * 1000 * 1000;
		}

		static Frequency GHz(cycles f) {
			return f * 1000 * 1000 * 1000;
		}

		// - observer -

		cycles toHz() const {
			return getValue();
		}

		cycles tokHz() const {
			return getValue() / 1000;
		}

		// - interaction with time -

		template<typename Rep, typename Period>
		cycles operator*(const std::chrono::duration<Rep,Period>& duration) {
			return getValue() * std::chrono::duration_cast<std::chrono::duration<double,std::ratio<1>>>(duration).count();
		}

		// - printer support -

		friend std::ostream& operator<<(std::ostream& out, const Frequency& f) {
			return out << f.getValue() << " Hz";
		}
	};

	// -- Literals --

	inline Frequency operator "" _Hz(cycles f) {
		return Frequency::Hz(f);
	}

	inline Frequency operator "" _kHz(cycles f) {
		return Frequency::kHz(f);
	}

	inline Frequency operator "" _MHz(cycles f) {
		return Frequency::MHz(f);
	}

	inline Frequency operator "" _GHz(cycles f) {
		return Frequency::GHz(f);
	}


	inline Frequency operator "" _kHz(long double f) {
		return Frequency::kHz(f);
	}

	inline Frequency operator "" _MHz(long double f) {
		return Frequency::MHz(f);
	}

	inline Frequency operator "" _GHz(long double f) {
		return Frequency::GHz(f);
	}

	template<typename Rep, typename Period>
	cycles operator*(const std::chrono::duration<Rep,Period>& duration, Frequency f) {
		return f * duration;
	}

} // end of namespace hw
} // end of namespace runtime
} // end of namespace allscale
