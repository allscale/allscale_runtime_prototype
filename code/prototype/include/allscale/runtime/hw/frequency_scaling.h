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
	 * @param core the core which's frequency should be altered
	 * @param f the frequency to be set, must be an element of the option vector.
	 * @param true if successfull, false otherwise
	 */
	bool setFrequency(Core core, Frequency);


} // end of namespace hw
} // end of namespace runtime
} // end of namespace allscale
