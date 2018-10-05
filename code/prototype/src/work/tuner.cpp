#include "allscale/runtime/work/tuner.h"

#include <algorithm>
#include <iostream>
#include <iomanip>

#include "allscale/utils/optional.h"
#include "allscale/runtime/hw/frequency_scaling.h"

namespace allscale {
namespace runtime {
namespace work {

	using allscale::utils::optional;

	// --- General Interface ---

	void Configuration::store(allscale::utils::ArchiveWriter& out) const {
		out.write(nodes);
		out.write(frequency);
	}

	Configuration Configuration::load(allscale::utils::ArchiveReader& in) {
		auto nodes = in.read<NodeMask>();
		auto freq = in.read<hw::Frequency>();
		return { std::move(nodes), freq };
	}

	std::ostream& operator<<(std::ostream& out, const Configuration& c) {
		return out << c.nodes << "@" << c.frequency;
	}

	std::ostream& operator<<(std::ostream& out, const State& s) {
		return out << "System state:"
				<<  " spd=" << std::setprecision(2) << s.speed
				<< ", eff=" << std::setprecision(2) << s.efficiency
				<< ", pow=" << std::setprecision(2) << s.power
				<< " => score: " << std::setprecision(2) << s.score;
	}

	// --- Implementations ---

	namespace {

		optional<NodeMask> inc(const NodeMask& mask) {
			if (mask.count() == mask.totalNodes()) return {};
			return NodeMask(mask).addNode();
		}

		optional<NodeMask> dec(const NodeMask& mask) {
			if (mask.count() == 1) return {};
			return NodeMask(mask).removeLast();
		}

		optional<hw::Frequency> inc(const hw::Frequency& freq) {
			auto options = hw::getFrequencyOptions(0);
			auto cur = std::find(options.begin(), options.end(), freq);
			assert_true(cur != options.end());
			auto pos = cur - options.begin();

			if (std::size_t(pos) == options.size()-1) return {};
			return options[pos+1];
		}

		optional<hw::Frequency> dec(const hw::Frequency& freq) {
			auto options = hw::getFrequencyOptions(0);
			auto cur = std::find(options.begin(), options.end(), freq);
			assert_true(cur != options.end());
			auto pos = cur - options.begin();

			if (pos == 0) return {};
			return options[pos-1];
		}
	}


	Configuration SimpleGradientDescent::next(const Configuration& current, const State& state) {

		// record current solution
		if (state.score > best_score) {
			best = current;
			best_score = state.score;
		}

		// pick next state

		if (explore.empty()) {

			// nothing left to explore => generate new points
			std::cout << "\t\tPrevious best option " << best << " with score " << best_score << "\n";

			// get nearby frequencies
			std::vector<optional<hw::Frequency>> frequencies;
			frequencies.push_back(dec(best.frequency));
			frequencies.push_back(best.frequency);
			frequencies.push_back(inc(best.frequency));

			// get nearby node numbers
			std::vector<optional<NodeMask>> nodes;
			nodes.push_back(dec(best.nodes));
			nodes.push_back(best.nodes);
			nodes.push_back(inc(best.nodes));

			// create new options
			for(const auto& a : nodes) {
				for(const auto& b : frequencies) {
					if (bool(a) && bool(b)) {
						std::cout << "\t\tAdding option " << *a << " @ " << *b << "\n";
						explore.push_back({*a,*b});
					}
				}
			}

			// reset best options
			best_score = 0;
		}

		// if there are still no options, there is nothing to do
		if (explore.empty()) return current;

		// take next option and evaluate
		auto next = explore.back();
		explore.pop_back();
		return next;
	}


	Configuration SimpleCoordinateDescent::next(const Configuration& current, const State& state) {

		// make sure there is a configuration space
		assert_true(inc(current.nodes) || dec(current.nodes) || inc(current.frequency) || dec(current.frequency));

		// decide whether this causes a direction change
		if (state.score < best_score) {
			nextDirection();
		}

		// remember best score
		if (state.score > best_score) {
			best_score = state.score;
			best = current;
		}

		// compute next configuration
		Configuration res = best;

		std::cout << "\tCurrent state: " << current << " with score " << state.score << "\n";
		std::cout << "\tCurrent best:  " << best << " with score " << best_score << "\n";

		while(true) {
			if (dim == NumNodes) {
				if (optional<NodeMask> n = (direction == Up) ? inc(res.nodes) : dec(res.nodes)) {
					res.nodes = *n;
					return res;
				}
			} else {
				if (optional<hw::Frequency> n = (direction == Up) ? inc(res.frequency) : dec(res.frequency)) {
					res.frequency = *n;
					return res;
				}
			}
			nextDirection();
		}

		// done
		return res;
	}

	void SimpleCoordinateDescent::nextDirection() {
		// swap direction
		direction = Direction(1 - direction);

		// swap dimension if necessary
		if (direction == Up) {
			dim = Dimension(1 - dim);
		}

		// print a status message
		std::cout << "New search direction: " << (dim == NumNodes ? "#nodes" : "frequency") << " " << (direction == Up ? "up" : "down") << "\n";
	}


} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale

