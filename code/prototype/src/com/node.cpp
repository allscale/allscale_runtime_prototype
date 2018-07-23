
#include "allscale/runtime/com/node.h"
#include "allscale/runtime/com/network.h"

namespace allscale {
namespace runtime {
namespace com {

	int Node::ping(
			int x) const {
		return x+1;
	}

	thread_local Node* Node::tp_local_node = nullptr;

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
