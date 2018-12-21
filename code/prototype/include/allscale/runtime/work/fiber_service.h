#pragma once

#include "allscale/utils/fibers.h"

#include "allscale/runtime/com/node.h"
#include "allscale/runtime/com/network.h"

namespace allscale {
namespace runtime {
namespace work {


	class FiberContextService {

		allscale::utils::FiberContext context;

	public:

		FiberContextService(com::Node&) {}

		auto& getContext() {
			return context;
		}

	};

	void installFiberContextService(com::Network&);


} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale
