/*
 * The prototype implementation of the scheduler interface.
 *
 *  Created on: Jul 24, 2018
 *      Author: herbert
 */

#pragma once

#include "allscale/runtime/com/network.h"
#include "allscale/runtime/work/task.h"

namespace allscale {
namespace runtime {
namespace work {


	// ---------------------------------------------------------------
	//					   Scheduler Interface
	// ---------------------------------------------------------------


	/**
	 * Requests a task to be scheduled somewhere in the system -- simple as that.
	 */
	void schedule(TaskPtr&&);

	/**
	 * Determines whether the given task should be split.
	 */
	bool shouldSplit(const TaskPtr&);

	// -- setup --

	// start up scheduler service within the provided network
	void installSchedulerService(com::Network&);


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
