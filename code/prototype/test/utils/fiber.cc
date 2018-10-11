#include <gtest/gtest.h>

#include "allscale/utils/fiber.h"

#include <type_traits>


namespace allscale {
namespace utils {


	TEST(Fiber, Basic) {

		EXPECT_FALSE(std::is_copy_constructible<FiberPool>::value);
		EXPECT_FALSE(std::is_move_constructible<FiberPool>::value);

		EXPECT_FALSE(std::is_copy_assignable<FiberPool>::value);
		EXPECT_FALSE(std::is_move_assignable<FiberPool>::value);

		// test that it can be constructed and destructed
		FiberPool pool;

	}

	TEST(Fiber, SimpleTask) {

		FiberPool pool;

		int x = 0;
		auto res = pool.start([&]{ x = 1; });
		EXPECT_FALSE(res);
		EXPECT_EQ(1,x);

		res = pool.start([&]{ x = 2; });
		EXPECT_FALSE(res);
		EXPECT_EQ(2,x);

	}

	TEST(Fiber, Suspension) {

		FiberPool pool;

		int x = 0;
		auto res = pool.start([&]{
			x = 1;
			FiberPool::suspend();
			x = 2;
		});

		EXPECT_TRUE(res);
		EXPECT_EQ(1,x);

		bool done = pool.resume(*res);

		EXPECT_FALSE(done);
		EXPECT_EQ(2,x);
	}

	TEST(Fiber, MultiSuspension) {

		FiberPool pool;

		int x = 0;
		auto res = pool.start([&]{
			x = 1;
			FiberPool::suspend();
			x = 2;
			FiberPool::suspend();
			x = 3;
		});

		EXPECT_TRUE(res);
		EXPECT_EQ(1,x);

		bool alive = pool.resume(*res);

		EXPECT_TRUE(alive);
		EXPECT_EQ(2,x);

		alive = pool.resume(*res);

		EXPECT_FALSE(alive);
		EXPECT_EQ(3,x);
	}


	TEST(Fiber, Interleave) {

		FiberPool pool;

		int x = 0;
		auto fA = pool.start([&]{
			x = 1;
			FiberPool::suspend();
			x = 2;
			FiberPool::suspend();
			x = 3;
		});

		EXPECT_TRUE(fA);
		EXPECT_EQ(1,x);

		auto fB = pool.start([&]{
			x = 11;
			FiberPool::suspend();
			x = 22;
			FiberPool::suspend();
			x = 33;
		});

		EXPECT_TRUE(fB);
		EXPECT_EQ(11,x);

		EXPECT_TRUE(pool.resume(*fA));
		EXPECT_EQ(2,x);

		EXPECT_TRUE(pool.resume(*fB));
		EXPECT_EQ(22,x);

		EXPECT_FALSE(pool.resume(*fB));
		EXPECT_EQ(33,x);

		EXPECT_FALSE(pool.resume(*fA));
		EXPECT_EQ(3,x);

	}

} // end of namespace utils
} // end of namespace allscale
