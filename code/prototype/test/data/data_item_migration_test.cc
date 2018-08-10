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


	TEST(DISABLED_DataItemMigration, TwoPoint) {

		using data_item = Grid<int,1>;
		using region_t = typename data_item::region_type;
		using ref_t = DataItemReference<Grid<int,1>>;

		// the data item reference to be manipulated
		ref_t A(0);

		// the region to be allocated
		region_t region_left(0,5);

		// the region to be transfered
		region_t fragment(2,4);

		DataItemRegions regions;
		regions.add(A,region_left);


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
		net.sync();

		auto printDistribution = [&]{
			// check distribution
			net.runOnAll([&](com::Node& node){
				auto& mgr = node.getService<DataItemManagerService>();
				std::cout << "Node " << node.getRank() << ": " << mgr.getExclusiveRegion(A) << "\n";
			});
			std::cout << "\n";

			// print distribution knowledge
			com::HierarchicalOverlayNetwork hierarchy(net);

			auto extractRegionFrom = [&](const DataItemRegions& regions)->region_t {
				auto res = regions.getRegion(A);
				return (res) ? *res : region_t();
			};

			hierarchy.runOnAll([&](com::Node& node, com::HierarchyAddress addr){
				auto& diis = node.getService<com::HierarchyService<DataItemIndexService>>().get(addr.getLayer());
				std::cout << addr << ": F=" << extractRegionFrom(diis.getAvailableData())
						<< ", L=" << extractRegionFrom(diis.getAvailableDataLeft())
						<< ", R=" << extractRegionFrom(diis.getAvailableDataRight()) << "\n";
			});
			std::cout << "\n\n";
		};


		// create a data item
		net.runOn(0,[&](com::Node&){
			A = DataItemManager::create<data_item>(10);
		});

		printDistribution();

		net.runOn(2,[&](com::Node&){
			auto& diis = com::HierarchicalOverlayNetwork::getLocalService<DataItemIndexService>();
			auto loc = diis.locate(regions);
			EXPECT_EQ("Locations()",toString(loc));
		});

		// allocate data at node 0
		net.runOn(0,[&](com::Node& node){

			// insert ownership in information service
			for(int i=2; i>=0; i--) {
				auto& diis = node.getService<com::HierarchyService<DataItemIndexService>>().get(com::layer_t(i));
				diis.addRegions(regions);
				if (i != 0) {
					diis.addRegionsLeft(regions);
				}
			}

			// get data fragment
			auto& mgr = node.getService<DataItemManagerService>();

			// should now already have the correct size
			EXPECT_EQ(region_left,mgr.getExclusiveRegion(A));

			// insert some data
			mgr.get(A)[3] = 12;
		});

		printDistribution();

		net.runOn(2,[&](com::Node&){
			auto& diis = com::HierarchicalOverlayNetwork::getLocalService<DataItemIndexService>();
			auto loc = diis.locate(regions);
			EXPECT_EQ("",toString(loc));
		});

		// migrate data to node 3
		net.runOn(3,[&](com::Node& node){
			auto& mgr = node.getService<DataItemManagerService>();
			mgr.acquire(A,fragment);

			// check value of available data
			EXPECT_EQ(12,mgr.get(A)[3]);
		});

		printDistribution();

		net.runOn(2,[&](com::Node&){
			auto& diis = com::HierarchicalOverlayNetwork::getLocalService<DataItemIndexService>();
			auto loc = diis.locate(regions);
			EXPECT_EQ("",toString(loc));
		});

	}


} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
