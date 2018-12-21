
#include <allscale/runtime/work/fiber_service.h>
#include "allscale/runtime/com/network.h"

namespace allscale {
namespace runtime {
namespace work {

	void installFiberContextService(com::Network& net) {
		net.installServiceOnNodes<FiberContextService>();
	}

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
