#include <gtest/gtest.h>

#include "allscale/runtime/com/network.h"


namespace allscale {
namespace runtime {
namespace com {

	struct DummyService {

		int x;

		DummyService(Node&, int value=0) : x(value) {}

	};


	TEST(Node, Service) {

		// create a network (implicit node creation)
		auto network = Network::create(2);
		if (!network) {
			std::cout << "WARNING: could not get a network of size 2!\n";
			return;
		}

		auto& net = *network;

		// create a service
		net.runOn(0,[](Node& node){
			auto s = node.startService<DummyService>();
			EXPECT_EQ(0,s.x);
		});

		// retrieve the service
		net.runOn(0,[](Node& node){
			auto s = node.getService<DummyService>();
			EXPECT_EQ(0,s.x);
		});

		// create a service with initial value
		net.runOn(1,[](Node& node){
			auto s = node.startService<DummyService>(12);
			EXPECT_EQ(12,s.x);
		});

		// retrieve the service and check the value
		net.runOn(1,[](Node& node){
			auto s = node.getService<DummyService>();
			EXPECT_EQ(12,s.x);
		});

		// check that original service is not effected
		net.runOn(0,[](Node& node){
			auto s = node.getService<DummyService>();
			EXPECT_EQ(0,s.x);
		});
	}


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
