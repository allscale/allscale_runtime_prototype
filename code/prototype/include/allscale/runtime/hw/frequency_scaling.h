#pragma once

#include <vector>

#include "allscale/runtime/hw/core.h"
#include "allscale/runtime/hw/frequency.h"

namespace allscale {
namespace runtime {
namespace hw {

	/**
	 * Obtains the list of frequency options for the given core.
	 *
	 * Consecutive invocations for the same core within the same process must
	 * return the same set of options.
	 *
	 * @param core the core for which to obtain the list of options
	 * @return the list of possible frequency options, containing at least one option and ordered in size
	 */
	std::vector<Frequency> getFrequencyOptions(Core core);

	/**
	 * Obtains the current operation frequency of the specified core.
	 */
	Frequency getFrequency(Core core);

	/**
	 * Updates the frequency on the given core. Returns true on success,
	 * false otherwise.
	 *
	 * @param core the core id for which the frequency should be altered
	 * @param f the frequency to be set, must be an element of the option vector.
	 * @param true if successful, false otherwise
	 */
	bool setFrequency(Core core, Frequency);

	/**
	 * Resets the state of a core to allow it to use its entire frequency range.
	 * Should be called before exit on every adjusted core.
	 */
	void resetFrequency(Core core);

#ifdef USE_LINUX_CPUFREQ
	// internal testing details
	namespace testing {
		// internally used to unit test caching
		extern thread_local int getFrequencyOptions_num_file_accesses;
	}
#endif

} // end of namespace hw
} // end of namespace runtime
} // end of namespace allscale
