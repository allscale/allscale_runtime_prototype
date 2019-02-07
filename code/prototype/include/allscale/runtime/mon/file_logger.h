#pragma once

#include <ostream>
#include <vector>

#include "allscale/utils/serializer.h"

#include "allscale/runtime/com/network.h"

namespace allscale {
namespace runtime {
namespace mon {

	/**
	 * The file logger is a service periodically collecting system load information
	 * during the execution, and storing it into a file for post-processing.
	 */

	/**
	 * Installs the file logging service on the given network.
	 */
	void installFileLoggerService(com::Network&);

	/**
	 * Shuts down the file logging service within the given network.
	 */
	void shutdownFileLoggerService(com::Network&);

} // end of namespace mon
} // end of namespace runtime
} // end of namespace allscale
