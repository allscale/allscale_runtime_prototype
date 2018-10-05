#pragma once

#include <ostream>

#include "allscale/utils/serializer.h"

#include "allscale/runtime/hw/frequency.h"
#include "allscale/runtime/work/node_mask.h"

namespace allscale {
namespace runtime {
namespace work {

	// --- General Interface ---

	/**
	 * A configuration in the optimization space.
	 */
	struct Configuration {

		// the current selection of nodes being active
		NodeMask nodes;

		// the current clock frequency active on all nodes
		hw::Frequency frequency;

		// -- serialization --

		void store(allscale::utils::ArchiveWriter& out) const;

		static Configuration load(allscale::utils::ArchiveReader& in);

		friend std::ostream& operator<<(std::ostream&, const Configuration&);
	};

	struct State : public allscale::utils::trivially_serializable {

		// the measured system speed in [0..1]
		float speed = 0.0;

		// the measured system efficiency in [0..1]
		float efficiency = 0.0;

		// the measured system power consumption in [0..1]
		float power = 0.0;

		// the overall score of the current system state according to the tuning objective, in [0..1]
		float score = 0.0;

		friend std::ostream& operator<<(std::ostream&, const State&);
	};


	/**
	 * An abstract base class for tuning strategies as it is utilized by the
	 * strategic scheduler. Its role is to obtain system configurations leading
	 * to high overall scores according to the user specified preferences.
	 */
	class Tuner {
	public:

		// a default destructor since it is a abstract base class
		virtual ~Tuner() {}

		/**
		 * The main interface, requesting the next configuration to be explored
		 * by the scheduler based on the given system state collected during the
		 * last scheduling interval.
		 *
		 * @param currentConfig the configuration active during the last scheduling interval
		 * @param currentState the observed system state during the last scheduling interval
		 * @return the configuration to be utilized for the next interval
		 */
		virtual Configuration next(const Configuration& currentConfig, const State& currentState) =0;

	};



	// --- Implementations ---

	/**
	 * Simple proof-of-concept tuning schema utilizing a simple gradient descent like approach.
	 */
	class SimpleGradientDescent : public Tuner {

		// best known option so far
		Configuration best;

		// score of best option so far
		float best_score = 0;

		// list of options to explore
		std::vector<Configuration> explore;

	public:

		SimpleGradientDescent(const Configuration& initial) : best(initial) {};

		Configuration next(const Configuration& current, const State& state) override;

	};

	/**
	 * Simple implementation of a coordinate descent tuning algorithm.
	 *
	 * See: https://en.wikipedia.org/wiki/Coordinate_descent
	 */
	class SimpleCoordinateDescent : public Tuner {

		// the dimensions searched
		enum Dimension {
			NumNodes = 0,
			Frequency = 1
		};

		// the direction
		enum Direction {
			Up = 0, Down = 1
		};


		// the dimension currently search along
		Dimension dim = NumNodes;

		// the direction currently search along the selected dimension
		Direction direction = Down;	// 0 .. increase, 1 .. decrease

		// the best configuration seen so far
		Configuration best;

		// the best score seen
		float best_score = 0;

	public:

		SimpleCoordinateDescent(const Configuration& initial) : best(initial) {};

		Configuration next(const Configuration& current, const State& state) override;

	private:

		void nextDirection();

	};


	// Other possible ideas:
	//  - pattern search - https://en.wikipedia.org/wiki/Pattern_search_(optimization)
	//  - Nelder-Mead - https://en.wikipedia.org/wiki/Nelder%E2%80%93Mead_method



} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale
