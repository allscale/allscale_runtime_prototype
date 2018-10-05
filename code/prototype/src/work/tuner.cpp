#include "allscale/runtime/work/tuner.h"

#include <algorithm>
#include <iostream>
#include <iomanip>

#include "allscale/runtime/hw/frequency_scaling.h"

namespace allscale {
namespace runtime {
namespace work {

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


	Configuration SimpleHillClimbing::next(const Configuration& current, const State& state) {

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
			auto options = hw::getFrequencyOptions(0);
			auto cur = std::find(options.begin(), options.end(), best.frequency);
			assert_true(cur != options.end());
			auto pos = cur - options.begin();

			std::vector<hw::Frequency> frequencies;
			if (pos != 0) frequencies.push_back(options[pos-1]);
			frequencies.push_back(options[pos]);
			if (pos+1<int(options.size())) frequencies.push_back(options[pos+1]);

			// get nearby node numbers
			std::vector<NodeMask> nodes;
			if (best.nodes.count() > 1) nodes.push_back(NodeMask(best.nodes).removeLast());
			nodes.push_back(best.nodes);
			if (best.nodes.count() < best.nodes.totalNodes()) nodes.push_back(NodeMask(best.nodes).addNode());


			// create new options
			for(const auto& a : nodes) {
				for(const auto& b : frequencies) {
					std::cout << "\t\tAdding option " << a << " @ " << b << "\n";
					explore.push_back({a,b});
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


} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale

