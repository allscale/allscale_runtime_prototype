#include <gtest/gtest.h>

#include "allscale/utils/fiber/read_write_lock.h"

#include <type_traits>
#include <thread>


namespace allscale {
namespace utils {
namespace fiber {

	// ----- futures -----

	TEST(ReadWriteLock, Traits) {

		EXPECT_FALSE(std::is_trivially_default_constructible<ReadWriteLock>::value);
		EXPECT_TRUE(std::is_default_constructible<ReadWriteLock>::value);
		EXPECT_FALSE(std::is_copy_constructible<ReadWriteLock>::value);
		EXPECT_FALSE(std::is_move_constructible<ReadWriteLock>::value);
		EXPECT_FALSE(std::is_copy_assignable<ReadWriteLock>::value);
		EXPECT_FALSE(std::is_move_assignable<ReadWriteLock>::value);

		EXPECT_TRUE(std::is_destructible<ReadWriteLock>::value);

	}

	TEST(ReadGuard, Traits) {

		EXPECT_FALSE(std::is_default_constructible<ReadGuard>::value);
		EXPECT_FALSE(std::is_copy_constructible<ReadGuard>::value);
		EXPECT_FALSE(std::is_move_constructible<ReadGuard>::value);
		EXPECT_FALSE(std::is_copy_assignable<ReadGuard>::value);
		EXPECT_FALSE(std::is_move_assignable<ReadGuard>::value);

		EXPECT_TRUE(std::is_destructible<ReadGuard>::value);
	}

	TEST(WriteGuard, Traits) {

		EXPECT_FALSE(std::is_default_constructible<WriteGuard>::value);
		EXPECT_FALSE(std::is_copy_constructible<WriteGuard>::value);
		EXPECT_FALSE(std::is_move_constructible<WriteGuard>::value);
		EXPECT_FALSE(std::is_copy_assignable<WriteGuard>::value);
		EXPECT_FALSE(std::is_move_assignable<WriteGuard>::value);

		EXPECT_TRUE(std::is_destructible<WriteGuard>::value);
	}



	TEST(ReadWriteLock, SingleRead) {

		FiberContext ctxt;

		// state
		int x = 0;
		ReadWriteLock lock;

		int a = 12;
		ctxt.process([&]{
			lock.startRead();
			a = x;
			lock.endRead();
		});

		EXPECT_EQ(a,x);

	}

	TEST(ReadWriteLock, MultipleRead) {

		FiberContext ctxt;

		// state
		int x = 0;
		ReadWriteLock lock;

		int a = 12;
		ctxt.process([&]{
			// just test that this does not cause it to hang
			lock.startRead();
			lock.startRead();
			EXPECT_TRUE(lock.tryStartRead());
			a = x;
			lock.endRead();
			lock.endRead();
			lock.endRead();
		});

		EXPECT_EQ(a,x);

	}


	TEST(ReadWriteLock, SingleWrite) {

		FiberContext ctxt;

		// state
		int x = 0;
		ReadWriteLock lock;

		int a = 12;
		ctxt.process([&]{
			lock.startWrite();
			EXPECT_FALSE(lock.tryStartWrite());
			a = x;
			lock.endWrite();
		});

		EXPECT_EQ(a,x);

	}


	TEST(ReadWriteLock, MultipleConcurrentRead) {
		int N = 10;

		FiberContext ctxt;
		auto& eventReg = ctxt.getEventRegister();

		// state
		ReadWriteLock lock;

		std::atomic<int> a(0);
		std::atomic<int> b(0);
		std::atomic<int> c(0);
		std::atomic<int> d(0);

		auto barrier = eventReg.create();

		for(int i=0; i<N; i++) {
			ctxt.start([&]{
				a++;
				lock.startRead();
				b++;
				suspend(barrier);
				c++;
				lock.endRead();
				d++;
			});
		}

		// nothing should yet have happend
		EXPECT_EQ(N,a.load());
		EXPECT_EQ(N,b.load());
		EXPECT_EQ(0,c.load());
		EXPECT_EQ(0,d.load());

		// trigger barrier for all N readers
		eventReg.trigger(barrier);

		// wait for completion
		while(ctxt.yield()) ;

		// now all should be done
		EXPECT_EQ(N,a.load());
		EXPECT_EQ(N,b.load());
		EXPECT_EQ(N,c.load());
		EXPECT_EQ(N,d.load());

	}

	TEST(ReadWriteLock, MultipleConcurrentWrite) {
		int N = 10000;

		FiberContext ctxt;

		// state
		int x = 0;
		ReadWriteLock lock;
		std::atomic<int> counter(0);

		for(int i=0; i<N; i++) {
			ctxt.start([&]{
				suspend();	// < suspend here
				lock.startWrite();
				x = x + 1;
				lock.endWrite();
				counter++;
			});
		}

		// nothing should have happend yet
		EXPECT_EQ(0,x);

		auto process = [&]{
			while (counter.load() != N) {
				ctxt.yield();
			}
		};

		// process concurrently
		std::thread t1(process);
		std::thread t2(process);
		std::thread t3(process);

		t1.join();
		t2.join();
		t3.join();

		// the updates should be completed
		EXPECT_EQ(N,x);

	}


	TEST(ReadWriteLock, MultipleReadWrite) {

		ReadWriteLock lock;

		// read and write should be possible
		EXPECT_TRUE(lock.tryStartRead());
		lock.endRead();

		EXPECT_TRUE(lock.tryStartWrite());
		lock.endWrite();


		// starting multiple reads should be possible
		lock.startRead();
		EXPECT_TRUE(lock.tryStartRead());
		lock.endRead();
		lock.endRead();

		// a write should block reads and writes
		lock.startWrite();
		EXPECT_FALSE(lock.tryStartRead());
		EXPECT_FALSE(lock.tryStartWrite());
		lock.endWrite();
	}

	// TODO: stress test read/write

	TEST(ReadWriteLock, MultipleConcurrentReadWrite) {
		int N = 10000;

		FiberContext ctxt;

		// state
		int x = 0;
		int y = 0;
		ReadWriteLock lock;
		std::atomic<int> counter(0);

		for(int i=0; i<N; i++) {
			// start a reader
			ctxt.start([&]{
				suspend();	// < suspend here
				lock.startRead();
				EXPECT_EQ(x,y);
				lock.endRead();
				counter++;
			});

			// start a writer
			ctxt.start([&]{
				suspend();	// < suspend here
				lock.startWrite();
				EXPECT_EQ(x,y);
				x = x + 1;
				y = y + 1;
				lock.endWrite();
				counter++;
			});
		}

		// nothing should have happend yet
		EXPECT_EQ(0,x);

		auto process = [&]{
			while (counter.load() != 2 * N) {
				ctxt.yield();
			}
		};

		// process concurrently
		std::thread t1(process);
		std::thread t2(process);
		std::thread t3(process);

		t1.join();
		t2.join();
		t3.join();

		// the updates should be completed
		EXPECT_EQ(N,x);
		EXPECT_EQ(N,y);

	}

} // end of namespace fiber
} // end of namespace utils
} // end of namespace allscale
