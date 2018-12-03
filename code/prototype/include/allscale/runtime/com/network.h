/*
 * network.h
 *
 *  Created on: Jul 23, 2018
 *      Author: herbert
 */

#pragma once

// chose the implementation:

#ifdef ENABLE_MPI
#include "allscale/runtime/com/mpi/network.h"
#else
#include "allscale/runtime/com/sim/network.h"
#endif

namespace allscale {
namespace runtime {
namespace com {

	#ifdef ENABLE_MPI

		using Network = mpi::Network;

		template<typename T>
		using RemoteCallResult = mpi::RemoteCallResult<T>;

	#else

		using Network = sim::Network;

		template<typename T>
		using RemoteCallResult = sim::RemoteCallResult<T>;

	#endif

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
