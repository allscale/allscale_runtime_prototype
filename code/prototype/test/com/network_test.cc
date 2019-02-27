#include <gtest/gtest.h>

#include "allscale/runtime/com/network.h"


namespace allscale {
namespace runtime {
namespace com {

	/**
	 * A simple ping service.
	 */
	struct PingService {

		PingService(com::Node&) {}

		int ping(int x) { return x + 1; };

	};


	TEST(Network, CreationAndSetup) {
		// create a network (implicit node creation)
		auto network = Network::create(2);
		if (!network) {
			std::cout << "WARNING: could not get a network of size 2!\n";
			return;
		}

		// install test service
		auto& net = *network;

		// install test service
		net.installServiceOnNodes<PingService>();

		net.sync();

		// run test RPC call
		net.runOn(0,[](Node& node){
			node.getFiberContext().process([]{
				auto& net = Network::getNetwork();
				auto f = net.getRemoteProcedure(0,&PingService::ping)(10);
				EXPECT_EQ(11,f.get());
				f = net.getRemoteProcedure(1,&PingService::ping)(11);
				EXPECT_EQ(12,f.get());
			});
		});

		// run test RPC call also in other direction
		net.runOn(1,[](Node& node){
			node.getFiberContext().process([]{
				auto& net = Network::getNetwork();
				auto f = net.getRemoteProcedure(0,&PingService::ping)(10);
				EXPECT_EQ(11,f.get());
				f = net.getRemoteProcedure(1,&PingService::ping)(11);
				EXPECT_EQ(12,f.get());
			});
		});

		net.sync();
	}

	// a simple POD
	struct POD : public allscale::utils::trivially_serializable {
		int x;
		POD() = default;
		POD(int x) : x(x) {}
	};

	// a type without default constructor
	struct NoDefaultCtor {
		int x;
		NoDefaultCtor(int x) : x(x) {}
		NoDefaultCtor(const NoDefaultCtor&) = default;
		NoDefaultCtor(NoDefaultCtor&&) = default;
		NoDefaultCtor& operator=(const NoDefaultCtor&) = default;
		NoDefaultCtor& operator=(NoDefaultCtor&&) = default;

		static NoDefaultCtor load(allscale::utils::ArchiveReader& in) {
			return in.read<int>();
		}

		void store(allscale::utils::ArchiveWriter& out) const {
			out.write(x);
		}
	};


	TEST(Network, TypeProperties) {

		EXPECT_TRUE(std::is_default_constructible<POD>::value);
		EXPECT_TRUE(std::is_copy_constructible<POD>::value);
		EXPECT_TRUE(std::is_move_constructible<POD>::value);
		EXPECT_TRUE(std::is_copy_assignable<POD>::value);
		EXPECT_TRUE(std::is_move_assignable<POD>::value);
		EXPECT_TRUE(allscale::utils::is_trivially_serializable<POD>::value);

		EXPECT_FALSE(std::is_default_constructible<NoDefaultCtor>::value);
		EXPECT_TRUE(std::is_copy_constructible<NoDefaultCtor>::value);
		EXPECT_TRUE(std::is_move_constructible<NoDefaultCtor>::value);
		EXPECT_TRUE(std::is_copy_assignable<NoDefaultCtor>::value);
		EXPECT_TRUE(std::is_move_assignable<NoDefaultCtor>::value);
		EXPECT_TRUE(allscale::utils::is_serializable<NoDefaultCtor>::value);
	}

	/**
	 * An RPC service to test restrictions on argument type properties.
	 */
	struct RPCTestService {

		RPCTestService(com::Node&) {}

		int pingA1(int x)       { return x + 1; };
		int pingA2(int x) const { return x + 1; };

		POD pingB1(const POD& p)       { return POD(p.x + 1); }
		POD pingB2(const POD& p) const { return POD(p.x + 1); }

		NoDefaultCtor pingC1(const NoDefaultCtor& p)       { return NoDefaultCtor(p.x + 1); }
		NoDefaultCtor pingC2(const NoDefaultCtor& p) const { return NoDefaultCtor(p.x + 1); }

	};


	TEST(Network, RPC) {
		// create a network (implicit node creation)
		auto network = Network::create(2);
		if (!network) {
			std::cout << "WARNING: could not get a network of size 2!\n";
			return;
		}

		// install test service
		auto& net = *network;

		// install test service
		net.installServiceOnNodes<RPCTestService>();

		// wait for completion
		net.sync();

		auto run = [&](auto op) {
			net.runOn(0,[&](Node& node){
				node.getFiberContext().process([&]{
					op();
				});
			});
		};

		// run test RPC call -- using primitive type
		run([]{
			auto& net = Network::getNetwork();
			auto f = net.getRemoteProcedure(0,&RPCTestService::pingA1)(10);
			EXPECT_EQ(11,f.get());
			auto g = net.getRemoteProcedure(1,&RPCTestService::pingA1)(11);
			EXPECT_EQ(12,g.get());
		});

		run([]{
			auto& net = Network::getNetwork();
			auto f = net.getRemoteProcedure(0,&RPCTestService::pingA2)(10);
			EXPECT_EQ(11,f.get());
			auto g = net.getRemoteProcedure(1,&RPCTestService::pingA2)(11);
			EXPECT_EQ(12,g.get());
		});

		// run test RPC call -- using POD
		run([]{
			auto& net = Network::getNetwork();
			auto f = net.getRemoteProcedure(0,&RPCTestService::pingB1)(10);
			EXPECT_EQ(11,f.get().x);
			auto g = net.getRemoteProcedure(1,&RPCTestService::pingB1)(11);
			EXPECT_EQ(12,g.get().x);
		});

		run([]{
			auto& net = Network::getNetwork();
			auto f = net.getRemoteProcedure(0,&RPCTestService::pingB2)(10);
			EXPECT_EQ(11,f.get().x);
			auto g = net.getRemoteProcedure(1,&RPCTestService::pingB2)(11);
			EXPECT_EQ(12,g.get().x);
		});

		// run test RPC call -- using NoDefaultCtor type
		run([]{
			auto& net = Network::getNetwork();
			auto f = net.getRemoteProcedure(0,&RPCTestService::pingC1)(10);
			EXPECT_EQ(11,f.get().x);
			auto g = net.getRemoteProcedure(1,&RPCTestService::pingC1)(11);
			EXPECT_EQ(12,g.get().x);
		});

		run([]{
			auto& net = Network::getNetwork();
			auto f = net.getRemoteProcedure(0,&RPCTestService::pingC2)(10);
			EXPECT_EQ(11,f.get().x);
			auto g = net.getRemoteProcedure(1,&RPCTestService::pingC2)(11);
			EXPECT_EQ(12,g.get().x);
		});

		// wait for completion
		net.sync();
	}

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
