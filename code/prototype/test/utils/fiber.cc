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

		EXPECT_FALSE(isFiberContext());

		int x = 0;
		auto res = pool.start([&]{
			EXPECT_TRUE(isFiberContext());
			x = 1;
		});
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
			EXPECT_TRUE(isFiberContext());
			x = 1;
			FiberPool::suspend();
			EXPECT_TRUE(isFiberContext());
			x = 2;
		});

		EXPECT_TRUE(res);
		EXPECT_EQ(1,x);

		bool done = FiberPool::resume(*res);

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

		bool alive = FiberPool::resume(*res);

		EXPECT_TRUE(alive);
		EXPECT_EQ(2,x);

		alive = FiberPool::resume(*res);

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

		EXPECT_TRUE(FiberPool::resume(*fA));
		EXPECT_EQ(2,x);

		EXPECT_TRUE(FiberPool::resume(*fB));
		EXPECT_EQ(22,x);

		EXPECT_FALSE(FiberPool::resume(*fB));
		EXPECT_EQ(33,x);

		EXPECT_FALSE(FiberPool::resume(*fA));
		EXPECT_EQ(3,x);

	}

	TEST(FiberMutex, SimpleLock) {

		FiberPool pool;

		FiberMutex lock;

		int x = 0;
		auto fA = pool.start([&]{
			x = 1;
			lock.lock();
			x = 2;
			lock.unlock();
			x = 3;
		});

		EXPECT_FALSE(fA);
		EXPECT_EQ(3,x);

	}

	TEST(FiberMutex, BlockedLock) {

		FiberPool pool;

		FiberMutex lock;
		lock.lock();

		int x = 0;
		auto fA = pool.start([&]{
			x = 1;
			lock.lock();
			x = 2;
			lock.unlock();
			x = 3;
		});

		EXPECT_TRUE(fA);
		EXPECT_EQ(1,x);

		// unlock the fiber (this leads to the completion of the task)
		lock.unlock();
		EXPECT_EQ(3,x);

	}


	TEST(FiberMutex, BlockedLockMultipleFibers) {

		FiberPool pool;

		FiberMutex lock;
		lock.lock();

		int x = 0;
		auto fA = pool.start([&]{
			x = 1;
			lock.lock();
			x = 2;
			lock.unlock();
			x = 3;
		});

		EXPECT_TRUE(fA);
		EXPECT_EQ(1,x);

		int y = 0;
		auto fB = pool.start([&]{
			y = 1;
			lock.lock();
			y = 2;
			lock.unlock();
			y = 3;
		});

		EXPECT_TRUE(fB);
		EXPECT_EQ(1,y);

		// unlock the fiber (this leads to the completion of both tasks)
		lock.unlock();
		EXPECT_EQ(3,x);
		EXPECT_EQ(3,y);

	}


} // end of namespace utils
} // end of namespace allscale
