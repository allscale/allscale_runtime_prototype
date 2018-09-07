/*
 * An interface to the inter-node load balancer.
 */

#pragma once

#include "allscale/runtime/com/network.h"
#include "allscale/runtime/work/task.h"

namespace allscale {
namespace runtime {
namespace work {


	// ---------------------------------------------------------------
	//					   Balancer Interface
	// ---------------------------------------------------------------


	/**
	 * Allows to test whether the local node is active or in stand-by.
	 *
	 * Note: for simplicty, this function is defined in the scheduler.cpp
	 */
	bool isLocalNodeActive();


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
