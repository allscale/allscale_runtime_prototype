#include <gtest/gtest.h>

#include "allscale/utils/fibers.h"

namespace allscale {
namespace utils {
namespace fiber {

	TEST(Fiber, Basic) {

	}

	TEST(Fiber, SimpleTask) {

		FiberContext ctxt;

		EXPECT_FALSE(getCurrentFiber());

		int x = 0;
		ctxt.start([&]{
			EXPECT_TRUE(getCurrentFiber());
			x = 1;
		});
		EXPECT_EQ(1,x);

		EXPECT_FALSE(getCurrentFiber());

		ctxt.start([&]{ x = 2; });
		EXPECT_EQ(2,x);

	}

	TEST(Fiber, Suspension) {

		FiberContext ctxt;

		auto event = ctxt.eventRegister.create();

		int x = 0;
		ctxt.start([&]{
			EXPECT_TRUE(getCurrentFiber());
			x = 1;
			suspend(event);
			EXPECT_TRUE(getCurrentFiber());
			x = 2;
		});

		EXPECT_EQ(1,x);

		// onlock the fiber
		ctxt.eventRegister.trigger(event);

		// give the fiber context a chance to process the unlocked fiber
		ctxt.yield();

		EXPECT_EQ(2,x);
	}


	TEST(Fiber, MultiSuspension) {

		FiberContext ctxt;

		auto evtA = ctxt.eventRegister.create();
		auto evtB = ctxt.eventRegister.create();

		int x = 0;
		ctxt.start([&]{
			x = 1;
			suspend(evtA);
			x = 2;
			suspend(evtB);
			x = 3;
		});

		EXPECT_EQ(1,x);

		ctxt.eventRegister.trigger(evtA);
		ctxt.yield();

		EXPECT_EQ(2,x);

		ctxt.eventRegister.trigger(evtB);
		ctxt.yield();

		EXPECT_EQ(3,x);
	}


	TEST(Fiber, Interleave) {

		FiberContext ctxt;

		auto evtA = ctxt.eventRegister.create();
		auto evtB = ctxt.eventRegister.create();
		auto evtC = ctxt.eventRegister.create();
		auto evtD = ctxt.eventRegister.create();

		auto trigger = [&](EventId evt) {
			ctxt.eventRegister.trigger(evt);
			ctxt.yield();
		};

		int x = 0;
		ctxt.start([&]{
			x = 1;
			suspend(evtA);
			x = 2;
			suspend(evtB);
			x = 3;
		});

		EXPECT_EQ(1,x);

		ctxt.start([&]{
			x = 11;
			suspend(evtC);
			x = 22;
			suspend(evtD);
			x = 33;
		});

		EXPECT_EQ(11,x);

		trigger(evtA);
		EXPECT_EQ(2,x);

		trigger(evtC);
		EXPECT_EQ(22,x);

		trigger(evtD);
		EXPECT_EQ(33,x);

		trigger(evtB);
		EXPECT_EQ(3,x);

	}


	TEST(FiberMutex, SimpleLock) {

		FiberContext ctxt;

		Mutex lock;

		int x = 0;
		ctxt.start([&]{
			x = 1;
			lock.lock();
			x = 2;
			lock.unlock();
			x = 3;
		});

		EXPECT_EQ(3,x);

	}

	TEST(FiberMutex, BlockedLock) {

		FiberContext ctxt;

		Mutex lock;
		lock.lock();

		int x = 0;
		ctxt.start([&]{
			x = 1;
			lock.lock();
			x = 2;
			lock.unlock();
			x = 3;
		});

		EXPECT_EQ(1,x);

		// unlock the fiber (this leads to the completion of the task)
		lock.unlock();
		ctxt.yield();
		EXPECT_EQ(3,x);

	}


	TEST(FiberMutex, BlockedLockMultipleFibers) {

		FiberContext ctxt;

		Mutex lock;
		lock.lock();

		int x = 0;
		ctxt.start([&]{
			x = 1;
			lock.lock();
			x = 2;
			lock.unlock();
			x = 3;
		});

		EXPECT_EQ(1,x);

		int y = 0;
		ctxt.start([&]{
			y = 1;
			lock.lock();
			y = 2;
			lock.unlock();
			y = 3;
		});

		EXPECT_EQ(1,y);

		// unlock the fiber (this leads to the completion of both tasks)
		lock.unlock();
		while (ctxt.yield()) ;
		EXPECT_EQ(3,x);
		EXPECT_EQ(3,y);

	}

	TEST(FiberPriorities, Properties) {
		EXPECT_EQ(Priority::DEFAULT,Priority::MEDIUM);
	}

	TEST(FiberContext, Priorities) {

		FiberContext ctxt;

		Mutex lock;
		lock.lock();

		int x = 0;
		ctxt.start([&]{
			lock.lock();
			x = 1;
			lock.unlock();
		}, fiber::Priority::LOW);

		ctxt.start([&]{
			lock.lock();
			x = 2;
			lock.unlock();
		}, fiber::Priority::MEDIUM);

		ctxt.start([&]{
			lock.lock();
			x = 3;
			lock.unlock();
		}, fiber::Priority::HIGH);

		EXPECT_EQ(0,x);

		// free the lock
		lock.unlock();
		EXPECT_EQ(0,x);		// nothing happened so far

		// process one step
		EXPECT_TRUE(ctxt.yield());
		EXPECT_EQ(3,x);

		// process second step
		EXPECT_TRUE(ctxt.yield());
		EXPECT_EQ(2,x);

		// process third step
		EXPECT_TRUE(ctxt.yield());
		EXPECT_EQ(1,x);

		// this should be all
		EXPECT_FALSE(ctxt.yield());
	}


} // end of namespace fiber
} // end of namespace utils
} // end of namespace allscale
