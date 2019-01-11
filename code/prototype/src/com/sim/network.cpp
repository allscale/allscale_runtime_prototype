#include "allscale/runtime/com/sim/network.h"

#include "allscale/runtime/work/fiber_service.h"

namespace allscale {
namespace runtime {
namespace com {
namespace sim {

	namespace detail {

		allscale::utils::FiberContext& getFiberContextOn(com::Node& node) {
			return node.getLocalService<work::FiberContextService>().getContext();
		}

	}

	Network::Network(size_t size) {
		nodes.reserve(size);
		for(size_t i=0; i<size; i++) {
			nodes.emplace_back(std::make_unique<Node>(i));

			// start network statistic service on nodes
			nodes.back()->startService<NetworkStatisticService>();
		}
	}

	std::unique_ptr<Network> Network::create() {
		// get the number of nodes
		int num_nodes = 4;	// < by default we use four nodes
		if (auto val = std::getenv("ART_NUM_NODES")) {
			num_nodes = std::atoi(val);
			if (num_nodes < 1) num_nodes = 1;
		}
		return create(num_nodes);
	}

	std::unique_ptr<Network> Network::create(size_t num_nodes) {
		return std::make_unique<Network>(num_nodes);
	}

	static thread_local Network* tl_current_network;


	// obtains the enclosing network instance
	Network& Network::getNetwork() {
		assert_true(tl_current_network) << "No local network!";
		return *tl_current_network;
	}

	void Network::setLocalNetwork() const {
		assert_false(tl_current_network);
		tl_current_network = const_cast<Network*>(this);
	}

	void Network::resetLocalNetwork() const {
		assert_true(tl_current_network);
		tl_current_network = nullptr;
	}

	NetworkStatistics Network::getStatistics() {
		NetworkStatistics res(numNodes());
		for(rank_t cur = 0; cur < numNodes(); cur++) {
			nodes[cur]->run([&](Node&){
				res[cur] = getRemoteProcedure(cur,&NetworkStatisticService::getNodeStats)().get();
			});
		}
		return res;
	}

	void Network::resetStatistics() {
		for(const auto& cur : nodes) {
			cur->getService<NetworkStatisticService>().resetNodeStats();
		}
	}

	NodeStatistics& Network::getNodeStats(rank_t rank) {
		assert_lt(rank,numNodes());
		return nodes[rank]->getService<NetworkStatisticService>().getLocalNodeStats();
	}

} // end of namespace sim
} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
