
#include "allscale/runtime/com/node.h"
#include "allscale/runtime/com/network.h"

namespace allscale {
namespace runtime {
namespace com {

	int Node::ping(
			int x) const {
		return x+1;
	}

	__attribute__ ((noinline))
	Node*& Node::tp_local_node() {
		static thread_local Node* node = nullptr;
		return node;
	}

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
