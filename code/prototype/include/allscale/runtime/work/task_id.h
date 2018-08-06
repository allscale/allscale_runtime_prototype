/*
 * The prototype version of a task ID.
 *
 *  Created on: Aug 1, 2018
 *      Author: herbert
 */

#pragma once

#include "allscale/utils/assert.h"

#include "allscale/api/core/impl/reference/task_id.h"

namespace allscale {
namespace runtime {
namespace work {

	// we are reusing the reference implementations task path and task ID
	using TaskPath = allscale::api::core::impl::reference::TaskPath;
	using TaskID = allscale::api::core::impl::reference::TaskID;

	// obtains a new, fresh id
	TaskID getFreshId();

	// obtains a child ID for a newly spawned task (only valid if triggered during the execution of a parent task)
	TaskID getNewChildId();

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
