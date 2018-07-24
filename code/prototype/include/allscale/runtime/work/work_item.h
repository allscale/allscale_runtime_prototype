/*
 * The prototype version of a task.
 *
 *  Created on: Jul 24, 2018
 *      Author: herbert
 */

#pragma once

#include "allscale/utils/functional_utils.h"

namespace allscale {
namespace runtime {
namespace work {

	template <
		typename Result,				// the result type of this work item
		typename Name,					// a struct producing the name of this work item
		typename SerializationFlag,		// this one is ignored
		typename SplitVariant,			// the split variant implementation
		typename ProcessVariant,		// the process variant implementation
		typename CanSplitTest			// the can-split test
	>
	struct work_item_description
	{
		using result_type = Result;
		using split_variant = SplitVariant;
		using process_variant = ProcessVariant;
		using can_spit_test = CanSplitTest;
	};

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
