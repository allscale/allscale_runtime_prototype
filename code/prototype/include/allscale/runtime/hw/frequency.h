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
	class Frequency : public allscale::utils::Scalar<unsigned long long, Frequency> {

		using super = allscale::utils::Scalar<unsigned long long, Frequency>;

	public:

		Frequency() = default;

		Frequency(unsigned long long f) : super(f) {};


		// --- factories ---

		static Frequency Hz(unsigned long long f) {
			return f;
		}

		static Frequency kHz(unsigned long long f) {
			return f * 1000;
		}

		static Frequency MHz(unsigned long long f) {
			return f * 1000 * 1000;
		}

		static Frequency GHz(unsigned long long f) {
			return f * 1000 * 1000 * 1000;
		}

		// - observer -

		unsigned long long toHz() const {
			return getValue();
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

	inline Frequency operator "" _Hz(unsigned long long f) {
		return Frequency::Hz(f);
	}

	inline Frequency operator "" _kHz(unsigned long long f) {
		return Frequency::kHz(f);
	}

	inline Frequency operator "" _MHz(unsigned long long f) {
		return Frequency::MHz(f);
	}

	inline Frequency operator "" _GHz(unsigned long long f) {
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
