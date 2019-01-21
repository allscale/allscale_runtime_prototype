
#include "allscale/runtime/work/treeture.h"

#include "allscale/runtime/com/network.h"

namespace allscale {
namespace runtime {
namespace work {

	void installTreetureStateService(com::Network& net) {
		net.installServiceOnNodes<TreetureStateService>();
	}

	treeture<void> treeture_parallel(treeture<void>&& a, treeture<void>&& b) {
		// wait eagerly
		a.wait();
		b.wait();
		return treeture<void>(true);
	}


} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale
