#include <gtest/gtest.h>

#include <set>

#include "allscale/runtime/com/hierarchy.h"

#include "allscale/utils/string_utils.h"

namespace allscale {
namespace runtime {
namespace com {


	TEST(HierarchyAddress, TypeTraits) {

		// some basic type traits
		EXPECT_TRUE(std::is_default_constructible<HierarchyAddress>::value);
		EXPECT_TRUE(std::is_destructible<HierarchyAddress>::value);

		EXPECT_TRUE(std::is_trivially_copy_constructible<HierarchyAddress>::value);
		EXPECT_TRUE(std::is_trivially_copy_assignable<HierarchyAddress>::value);

		EXPECT_TRUE(std::is_trivially_move_constructible<HierarchyAddress>::value);
		EXPECT_TRUE(std::is_trivially_move_assignable<HierarchyAddress>::value);


		// make sure the hierarchy address is serializable
		EXPECT_TRUE(allscale::utils::is_serializable<HierarchyAddress>::value);
		EXPECT_TRUE(allscale::utils::is_trivially_serializable<HierarchyAddress>::value);

	}


	TEST(HierarchyAddress, Create) {
		// test that the default version is valid
		HierarchyAddress addr;
	}

	TEST(HierarchyAddress, Print) {
		// test that the default version is valid
		HierarchyAddress addr;

		EXPECT_EQ("0:0",toString(addr));
	}

	TEST(HierarchyAddress, Root) {
		// test the root of various network sizes
		EXPECT_EQ("0:0",toString(HierarchyAddress::getRootOfNetworkSize( 1)));
		EXPECT_EQ("0:1",toString(HierarchyAddress::getRootOfNetworkSize( 2)));
		EXPECT_EQ("0:2",toString(HierarchyAddress::getRootOfNetworkSize( 3)));
		EXPECT_EQ("0:2",toString(HierarchyAddress::getRootOfNetworkSize( 4)));
		EXPECT_EQ("0:3",toString(HierarchyAddress::getRootOfNetworkSize( 5)));
		EXPECT_EQ("0:3",toString(HierarchyAddress::getRootOfNetworkSize( 6)));
		EXPECT_EQ("0:3",toString(HierarchyAddress::getRootOfNetworkSize( 7)));
		EXPECT_EQ("0:3",toString(HierarchyAddress::getRootOfNetworkSize( 8)));
		EXPECT_EQ("0:4",toString(HierarchyAddress::getRootOfNetworkSize( 9)));
		EXPECT_EQ("0:4",toString(HierarchyAddress::getRootOfNetworkSize(10)));
		EXPECT_EQ("0:4",toString(HierarchyAddress::getRootOfNetworkSize(11)));
		EXPECT_EQ("0:4",toString(HierarchyAddress::getRootOfNetworkSize(12)));
		EXPECT_EQ("0:4",toString(HierarchyAddress::getRootOfNetworkSize(13)));
		EXPECT_EQ("0:4",toString(HierarchyAddress::getRootOfNetworkSize(14)));
		EXPECT_EQ("0:4",toString(HierarchyAddress::getRootOfNetworkSize(15)));
		EXPECT_EQ("0:4",toString(HierarchyAddress::getRootOfNetworkSize(16)));
		EXPECT_EQ("0:5",toString(HierarchyAddress::getRootOfNetworkSize(17)));
	}

	TEST(HierarchyAddress, LayersOnNode) {

		// test the root of various network sizes
		EXPECT_EQ(1,HierarchyAddress::getLayersOn(0,1));
		EXPECT_EQ(2,HierarchyAddress::getLayersOn(0,2));
		EXPECT_EQ(3,HierarchyAddress::getLayersOn(0,3));
		EXPECT_EQ(3,HierarchyAddress::getLayersOn(0,4));
		EXPECT_EQ(4,HierarchyAddress::getLayersOn(0,5));
		EXPECT_EQ(4,HierarchyAddress::getLayersOn(0,6));
		EXPECT_EQ(4,HierarchyAddress::getLayersOn(0,7));
		EXPECT_EQ(4,HierarchyAddress::getLayersOn(0,8));
		EXPECT_EQ(5,HierarchyAddress::getLayersOn(0,9));

		// test other nodes
		EXPECT_EQ(4,HierarchyAddress::getLayersOn(0,8));
		EXPECT_EQ(1,HierarchyAddress::getLayersOn(1,8));
		EXPECT_EQ(2,HierarchyAddress::getLayersOn(2,8));
		EXPECT_EQ(1,HierarchyAddress::getLayersOn(3,8));
		EXPECT_EQ(3,HierarchyAddress::getLayersOn(4,8));
		EXPECT_EQ(1,HierarchyAddress::getLayersOn(5,8));
		EXPECT_EQ(2,HierarchyAddress::getLayersOn(6,8));
		EXPECT_EQ(1,HierarchyAddress::getLayersOn(7,8));

	}

	namespace {

		void collectAll(const HierarchyAddress& cur, std::set<HierarchyAddress>& res) {
			res.insert(cur);
			if (cur.isLeaf()) return;
			collectAll(cur.getLeftChild(),res);
			collectAll(cur.getRightChild(),res);
		}

		std::set<HierarchyAddress> getAllAddresses(rank_t size) {
			std::set<HierarchyAddress> all;
			collectAll(HierarchyAddress::getRootOfNetworkSize(size),all);

			// filter all beyond size
			std::set<HierarchyAddress> res;
			for(const auto& cur : all) {
				if (cur.getRank() < size) res.insert(cur);
			}

			// done
			return res;
		}

	}

	TEST(HierarchyAddress, Navigation) {

		int N = 8;

		// test the full navigation in an 8-wide tree
		auto root = HierarchyAddress::getRootOfNetworkSize(N);

		// collect all nodes
		std::set<HierarchyAddress> all;
		collectAll(root,all);

		// check the number of nodes
		EXPECT_EQ(15,all.size());

		int numLeafs = 0;
		int numInner = 0;
		int numLeftChildren = 0;
		int numRightChildren = 0;
		for(const auto& cur : all) {

			if (cur.isLeaf()) {
				numLeafs++;
			} else {
				EXPECT_TRUE(cur.isVirtualNode());
				numInner++;
			}

			// check family relations
			if (cur.isLeftChild()) {
				EXPECT_EQ(cur,cur.getParent().getLeftChild());
				numLeftChildren++;
			} else {
				EXPECT_TRUE(cur.isRightChild());
				EXPECT_EQ(cur,cur.getParent().getRightChild());
				numRightChildren++;
			}

			// check height
			EXPECT_LT(cur.getLayer(), HierarchyAddress::getLayersOn(cur.getRank(),N)) << cur;
		}

		// check the correct number of leafs and inner nodes
		EXPECT_EQ(8,numLeafs);
		EXPECT_EQ(7,numInner);

		// also: the correct number of left and right children
		EXPECT_EQ(8,numLeftChildren); // the root is a left child
		EXPECT_EQ(7,numRightChildren);

	}


	struct LayerService {

		HierarchyAddress myAddress;

		LayerService(Network&, const HierarchyAddress& addr) : myAddress(addr) {}

		HierarchyAddress whereAreYou() { return myAddress; }

	};

	TEST(HierarchicalOverlayNetwork, CommTest) {

		int N = 11;

		// create a network of 8 nodes
		auto network = Network::create(N);
		if (!network) {
			std::cout << "WARNING: could not create a network of N nodes!\n";
			return;
		}
		auto& net = *network;

		// create a overlay network
		HierarchicalOverlayNetwork hierarchy(net);

		// install the layer service
		hierarchy.installServiceOnNodes<LayerService>();

		// collect all virtual node addresses
		auto all = getAllAddresses(N);

		// perform remote calls to all services from node N/2
		net.runOn(N/2,[&](Node&){
			for(const auto& cur : all) {
				auto reply = hierarchy.getRemoteProcedure(cur,&LayerService::whereAreYou)().get();
				EXPECT_EQ(reply,cur);
			}
		});

		//std::cout << net.getStatistics();
	}


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
