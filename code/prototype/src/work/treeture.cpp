
#include "allscale/runtime/work/treeture.h"

namespace allscale {
namespace runtime {
namespace work {

	treeture<void> treeture_parallel(treeture<void>&& a, treeture<void>&& b) {
		// wait eagerly
		a.wait();
		b.wait();
		return treeture<void>();
	}


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
