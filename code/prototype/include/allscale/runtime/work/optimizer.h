/*
 * An interface for the dynamic multi-objective optimizer.
 */

#pragma once

#include "allscale/utils/serializer.h"

namespace allscale {
namespace runtime {
namespace work {


	// ---------------------------------------------------------------
	//					        Optimizer
	// ---------------------------------------------------------------

	/**
	 * A class to model user-defined tuning objectives.
	 *
	 * Objectives are defined as
	 *
	 * 	score = t^m * e^n * (1-p)^k
	 *
	 * where t, e, p \in [0..1] is the current system speed, efficiency
	 * and power dissipation. The exponents m, n, and k can be user defined.
	 *
	 * The optimizer aims to maximize the overall score.
	 */
	class TuningObjective : public allscale::utils::trivially_serializable {

		float speedExponent;

		float efficiencyExponent;

		float powerExponent;

	public:

		TuningObjective();

		TuningObjective(float,float,float);

		// --- factories ---

		static TuningObjective speed() {
			return { 1, 0, 0 };
		}

		static TuningObjective efficiency() {
			return { 0, 1, 0 };
		}

		static TuningObjective power() {
			return { 0, 0, 1 };
		}

		// --- observers ---

		float getSpeedExponent() const {
			return speedExponent;
		}

		float getEfficiencyExponent() const {
			return efficiencyExponent;
		}

		float getPowerExponent() const {
			return powerExponent;
		}

		// --- mutators ---

		void setSpeedExponent(float x) {
			assert_le(0,x);
			speedExponent = x;
		}

		void setEfficiencyExponent(float x) {
			assert_le(0,x);
			efficiencyExponent = x;
		}

		void setPowerExponent(float x) {
			assert_le(0,x);
			powerExponent = x;
		}

		/**
		 * Evaluates the given state based on the presented objectives
		 * and returns a
		 */
		float getScore(float speed, float efficiency, float power) const;

		// -- operators --

		bool operator==(const TuningObjective& other) const {
			return this == &other || (speedExponent == other.speedExponent && efficiencyExponent == other.efficiencyExponent && powerExponent == other.powerExponent);
		}

		bool operator!=(const TuningObjective& other) const {
			return !(*this == other);
		}

		// provides print support
		friend std::ostream& operator<<(std::ostream& out, const TuningObjective&);

	};

	/**
	 * Updates the active tuning objective to be optimized for by the dynamic
	 * optimizer.
	 */
	void setActiveTuningObjectiv(const TuningObjective& obj);

	/**
	 * Obtains the current objective optimized for by the dynamic optimizer.
	 */
	TuningObjective getActiveTuningObjectiv();


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
