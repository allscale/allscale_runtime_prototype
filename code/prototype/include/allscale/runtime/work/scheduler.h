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

	// -- management interface --

	/**
	 * The different types of schedulers supported.
	 */
	enum class SchedulerType {
		Uniform,		// < a scheduler assigning equal number of tasks to all nodes
		Balanced,		// < a scheduler actively balancing load between nodes
		Tuned,			// < the balanced scheduler + adaptation of #nodes and frequency
		Random			// < a scheduler assigning tasks randomly to nodes
	};

	// allow types to be printed
	std::ostream& operator<<(std::ostream&,const SchedulerType&);

	/**
	 * Obtains the currently active type of scheduler.
	 */
	SchedulerType getCurrentSchedulerType();

	/**
	 * Updates the currently active scheduler type.
	 */
	void setCurrentSchedulerType(SchedulerType);

	/**
	 * Toggles the active state of the given node.
	 */
	void toggleActiveState(com::rank_t);


	// -- internal interface --

	/**
	 * Requests a task to be scheduled somewhere in the system -- simple as that.
	 */
	void schedule(TaskPtr&&);

	/**
	 * Determines whether the given task should be split.
	 */
	bool shouldSplit(const TaskPtr&);

	/**
	 * Requests the scheduler to estimate the location of the given task. This
	 * is a local operation, not including remote operations. However, the result
	 * might be wrong if e.g. random scheduling is used, or scheduling policies
	 * have changed. Its main purpose is to get an estimate for task dependency
	 * resolution.
	 */
	com::rank_t estimateLocationOf(const TaskID& task);

	// -- setup --

	// start up scheduler service within the provided network
	void installSchedulerService(com::Network&);


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
