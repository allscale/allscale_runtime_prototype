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


	TEST(DataItemMigration, SingleSource) {

		using data_item = Grid<int,1>;
		using region_t = typename data_item::region_type;
		using ref_t = DataItemReference<Grid<int,1>>;

		// the data item reference to be manipulated
		ref_t A(0);

		// the region to be allocated
		region_t region_left(0,5);

		// the region to be transfered
		region_t fragment(2,4);


		// create a network
		auto network = com::Network::create();
		auto& net = *network;
		com::HierarchicalOverlayNetwork hierarchy(net);

		if (net.numNodes() < 4) {
			std::cout << "WARNING: can not run this test with less than four nodes.";
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

		DataItemRegions regions;
		regions.add(A,region_left);

		printDistribution();

		net.runOn(2,[&](com::Node&){
			auto& diis = com::HierarchicalOverlayNetwork::getLocalService<DataItemIndexService>();
			auto loc = diis.locate(regions);

			DataItemLocationInfos should;
			EXPECT_EQ(should,loc);
		});

		// allocate data at node 0
		net.runOn(0,[&](com::Node& node){

			// insert ownership in information service
			for(int i=2; i>=0; i--) {
				auto& diis = node.getService<com::HierarchyService<DataItemIndexService>>().get(com::layer_t(i));
				if (i!=0) {
					diis.addAllowanceLeft(regions,regions);
				} else {
					diis.addAllowanceLocal(regions);
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

			DataItemLocationInfos should;
			should.add(A,region_left,0);
			EXPECT_EQ(should,loc);
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

			DataItemLocationInfos should;
			should.add(A,region_t::difference(region_left,fragment),0);
			should.add(A,fragment,3);
			EXPECT_EQ(should,loc);
		});

	}


	TEST(DataItemMigration, MultiSource) {

		using data_item = Grid<int,1>;
		using region_t = typename data_item::region_type;
		using ref_t = DataItemReference<Grid<int,1>>;

		// the data item reference to be manipulated
		ref_t A(0);

		// the region to be allocated
		region_t region_left(0,5);		// < on node 0
		region_t region_right(5,10);	// < on node 1
		region_t region_full(0,10);		// < full range

		// the region to be transfered
		region_t fragment(2,7);			// < received by node 3


		// create a network
		auto network = com::Network::create();
		auto& net = *network;
		com::HierarchicalOverlayNetwork hierarchy(net);

		if (net.numNodes() < 4) {
			std::cout << "WARNING: can not run this test with less than four nodes.";
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

		DataItemRegions regions_left;
		regions_left.add(A,region_left);

		DataItemRegions regions_right;
		regions_right.add(A,region_right);

		DataItemRegions regions_full;
		regions_full.add(A,region_full);

		printDistribution();

		net.runOn(2,[&](com::Node&){
			auto& diis = com::HierarchicalOverlayNetwork::getLocalService<DataItemIndexService>();
			auto loc = diis.locate(regions_full);

			DataItemLocationInfos should;
			EXPECT_EQ(should,loc);
		});

		// allocate data at node 0
		net.runOn(0,[&](com::Node& node){

			// insert ownership in information service
			for(int i=2; i>=0; i--) {
				auto& diis = node.getService<com::HierarchyService<DataItemIndexService>>().get(com::layer_t(i));
				if (i > 1) {
					diis.addAllowanceLeft(regions_full,regions_full);
				} if (i == 1) {
					diis.addAllowanceLeft(regions_full,regions_left);
					diis.addAllowanceRight({},regions_right);
				} else {
					diis.addAllowanceLocal(regions_left);
				}
			}

			// get data fragment
			auto& mgr = node.getService<DataItemManagerService>();

			// should now already have the correct size
			EXPECT_EQ(region_left,mgr.getExclusiveRegion(A));

			// insert some data
			mgr.get(A)[3] = 12;
		});

		// allocate data at node 1
		net.runOn(1,[&](com::Node& node){

			// insert ownership in information service
			{
				auto& diis = node.getService<com::HierarchyService<DataItemIndexService>>().get(com::layer_t(0));
				diis.addAllowanceLocal(regions_right);
			}

			// get data fragment
			auto& mgr = node.getService<DataItemManagerService>();

			// should now already have the correct size
			EXPECT_EQ(region_right,mgr.getExclusiveRegion(A));

			// insert some data
			mgr.get(A)[6] = 18;
		});

		printDistribution();

		net.runOn(2,[&](com::Node&){
			auto& diis = com::HierarchicalOverlayNetwork::getLocalService<DataItemIndexService>();
			auto loc = diis.locate(regions_full);

			DataItemLocationInfos should;
			should.add(A,region_left,0);
			should.add(A,region_right,1);
			EXPECT_EQ(should,loc);
		});

		// migrate data to node 3
		net.runOn(3,[&](com::Node& node){
			auto& mgr = node.getService<DataItemManagerService>();
			mgr.acquire(A,fragment);

			// check value of available data
			EXPECT_EQ(12,mgr.get(A)[3]);
			EXPECT_EQ(18,mgr.get(A)[6]);
		});

		printDistribution();

		net.runOn(2,[&](com::Node&){
			auto& diis = com::HierarchicalOverlayNetwork::getLocalService<DataItemIndexService>();
			auto loc = diis.locate(regions_full);

			DataItemLocationInfos should;
			should.add(A,region_t::difference(region_left,fragment),0);
			should.add(A,region_t::difference(region_right,fragment),1);
			should.add(A,fragment,3);
			EXPECT_EQ(should,loc);
		});

	}

} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
