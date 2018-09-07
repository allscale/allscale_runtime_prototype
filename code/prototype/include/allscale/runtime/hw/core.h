#pragma once

#include <vector>

namespace allscale {
namespace runtime {
namespace hw {

	/**
	 * The type to identify cores.
	 */
	using Core = unsigned;

	/**
	 * Obtains the number of available cores.
	 */
	unsigned getNumberAvailableCores();

	/**
	 * Obtains a list of the available cores on the local system.
	 */
	std::vector<Core> getAvailableCores();

} // end namespace hw
} // end namespace runtime
} // end namespace allscale
