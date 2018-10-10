#pragma once

#include <ostream>

#include "allscale/utils/serializer.h"

#include "allscale/runtime/hw/frequency.h"
#include "allscale/runtime/work/node_mask.h"
#include "allscale/runtime/work/optimizer.h"

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

		// -- operator --

		bool operator==(const Configuration& other) const {
			return nodes == other.nodes && frequency == other.frequency;
		}

		bool operator!=(const Configuration& other) const {
			return !(*this == other);
		}

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
	 * A tuner utilizing another tuner for obtaining a good solution and retaining that
	 * solution until performance degrates, triggering a search-restart
	 */
	class IntervalTuner : public Tuner {

		enum Mode {
			Exploring,	// < exploring the configuration space
			Exploiting	// < exploiting the best solution
		};

		// the objective handled the last time - to notice changes
		TuningObjective objective;

		Mode mode = Exploring;

		// the best known configuration
		Configuration best;

		// the cores of the best configuration
		float best_score = 0;

		// a nested tuner
		std::unique_ptr<Tuner> tuner;

		// the number of times the result could not be improved lately
		int no_improvent_counter = 0;

		// when there hasn't been an improvement for 10 time steps => use the current best
		const int no_improvement_limit = 10;

		// the acceptable performance degradation
		const float degredatin = 0.9;

	public:

		IntervalTuner(const Configuration& initial, std::unique_ptr<Tuner>&& tuner)
			: best(initial), tuner(std::move(tuner)) {}

		Configuration next(const Configuration& current, const State& state) override;

	};


	/**
	 * Simple proof-of-concept tuning schema utilizing a simple gradient descent like approach.
	 */
	class SimpleGradientDescent : public Tuner {

		// the objective handled the last time - to notice changes
		TuningObjective objective;

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

		// the objective handled the last time - to notice changes
		TuningObjective objective;

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
