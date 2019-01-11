
#include "allscale/runtime/com/node.h"
#include "allscale/runtime/com/network.h"

namespace allscale {
namespace runtime {
namespace com {

	int Node::ping(
			int x) const {
		return x+1;
	}

	static thread_local Node* tl_current_node = nullptr;

	__attribute__ ((noinline))
	Node* Node::getLocalNodeInternal() {
		return tl_current_node;
	}

	void Node::setLocalNode(Node* node) {
		tl_current_node = node;
	}

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
