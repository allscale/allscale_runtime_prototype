
#include "allscale/runtime/work/optimizer.h"

#include <cmath>
#include <cstdlib>

namespace allscale {
namespace runtime {
namespace work {

	TuningObjective::TuningObjective() : TuningObjective(0,0,0) {};

	TuningObjective::TuningObjective(float s,float e,float p)
		: speedExponent(s), efficiencyExponent(e), powerExponent(p) {}


	float TuningObjective::getScore(float speed, float efficiency, float power) const {
		// maximize speed and efficiency, minimize power
		return
			std::pow(speed,speedExponent) *
			std::pow(efficiency,efficiencyExponent) *
			std::pow(1-power,powerExponent);
	}

	std::ostream& operator<<(std::ostream& out, const TuningObjective& o) {
		return out << "t^" << o.speedExponent << "e^" << o.efficiencyExponent << "p^" << o.powerExponent;
	}

	namespace {

		TuningObjective getInitialObjective() {

			auto c_obj = std::getenv("ART_OBJECTIVE");

			if (!c_obj) {
				return TuningObjective::speed();
			}

			std::string obj = c_obj;
			if (obj == "speed") {
				return TuningObjective::speed();
			}

			if (obj == "efficiency") {
				return TuningObjective::efficiency();
			}

			if (obj == "power") {
				return TuningObjective::power();
			}

			if (obj == "se") {
				return TuningObjective(1,1,0);
			}

			if (obj == "sp") {
				return TuningObjective(1,0,1);
			}

			if (obj == "spe") {
				return TuningObjective(1,1,1);
			}

			std::cerr << "Unknown tuning objective: " << obj << " -- using default objective: speed\n";

			return TuningObjective::speed();
		}

	}


	TuningObjective objective = getInitialObjective();


	void setActiveTuningObjectiv(const TuningObjective& obj) {
		objective = obj;
	}

	TuningObjective getActiveTuningObjectiv() {
		return objective;
	}


} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale
