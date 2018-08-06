#include <gtest/gtest.h>


#include "allscale/utils/string_utils.h"
#include "allscale/api/user/data/grid.h"

#include "allscale/runtime/com/network.h"
#include "allscale/runtime/com/hierarchy.h"
#include "allscale/runtime/data/data_item_manager.h"

using namespace allscale::utils;
using namespace allscale::api::core;
using namespace allscale::api::user::data;

namespace allscale {
namespace runtime {
namespace data {

	// TODO: simulate the migration of data ownership

	TEST(DataItemMigration, TwoPoint) {

		using data_item = Grid<int,1>;
		using region_t = typename data_item::region_type;
		using ref_t = DataItemReference<Grid<int,1>>;

		// create a network
		auto network = com::Network::create();
		auto& net = *network;
		com::HierarchicalOverlayNetwork hierarchy(net);

		if (net.numNodes() < 2) {
			std::cout << "WARNING: can not run this test with less than two nodes.";
			return;
		}

		// install services
		data::installDataItemManagerService(net);
		hierarchy.installServiceOnNodes<DataItemIndexService>();

		// sync before processing

		ref_t A(0);
		auto printDistribution = [&]{
			// check distribution
			net.runOnAll([&](com::Node& node){
				auto& mgr = node.getService<DataItemManagerService>();
				std::cout << "Node " << node.getRank() << ": " << mgr.getExclusiveRegion(A) << "\n";
			});
			std::cout << "\n";
		};


		// create a data item
		net.runOn(0,[&](com::Node&){
			A = DataItemManager::create<data_item>(10);
		});

		printDistribution();

		// allocate data at node 0
		region_t region_left(0,5);
		net.runOn(0,[&](com::Node& node){
			auto& mgr = node.getService<DataItemManagerService>();
			mgr.resizeExclusive(A,region_left);

			// insert some data
			mgr.get(A)[3] = 12;
		});

		printDistribution();

		// migrate data to node 3
		region_t fragment(2,4);
		net.runOn(3,[&](com::Node& node){
			auto& mgr = node.getService<DataItemManagerService>();
			mgr.acquireOwnership(A,fragment);

			// check value of available data
			EXPECT_EQ(12,mgr.get(A)[3]);
		});

		printDistribution();

	}


} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
