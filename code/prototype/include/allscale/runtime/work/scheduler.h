/*
 * The prototype implementation of the scheduler interface.
 *
 *  Created on: Jul 24, 2018
 *      Author: herbert
 */

#pragma once

#include "allscale/runtime/com/node.h"
#include "allscale/runtime/work/task.h"

namespace allscale {
namespace runtime {
namespace work {

	/**
	 * Determines whether the given task should be split.
	 */
	bool shouldSplit(const TaskPtr&);

	/**
	 * An initial, simple scheduling function.
	 */
	com::rank_t getScheduleTarget(com::rank_t localRank, const TaskPtr&);



} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
