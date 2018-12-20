#include <gtest/gtest.h>

#include "allscale/utils/fiber/future.h"

#include <type_traits>
#include <thread>


namespace allscale {
namespace utils {
namespace fiber {

	// ----- futures -----

	TEST(Future, Traits) {

		EXPECT_FALSE(std::is_copy_constructible<Promise<int>>::value);
		EXPECT_TRUE(std::is_move_constructible<Promise<int>>::value);
		EXPECT_FALSE(std::is_copy_assignable<Promise<int>>::value);
		EXPECT_TRUE(std::is_move_assignable<Promise<int>>::value);

		EXPECT_TRUE(std::is_default_constructible<Future<int>>::value);
		EXPECT_FALSE(std::is_copy_constructible<Future<int>>::value);
		EXPECT_TRUE(std::is_move_constructible<Future<int>>::value);
		EXPECT_FALSE(std::is_copy_assignable<Future<int>>::value);
		EXPECT_TRUE(std::is_move_assignable<Future<int>>::value);

	}

	TEST(Future, ReadyScalar) {
		Future<int> f(12);
		EXPECT_TRUE(f.valid());
		EXPECT_EQ(12,f.get());
	}

	TEST(Future, SequentialScalar) {
		FiberContext ctxt;
		Promise<int> p(ctxt);
		auto f = p.get_future();
		EXPECT_TRUE(f.valid());
		p.set_value(12);
		EXPECT_EQ(12,f.get());
	}

	TEST(Future, ParallelScalar) {
		FiberContext ctxt;

		Promise<int> p(ctxt);
		auto f = p.get_future();
		EXPECT_TRUE(f.valid());

		std::atomic<bool> done(false);
		auto consumer = std::thread([&]{
			ctxt.start([&]{
				EXPECT_EQ(12,f.get());
				done = true;
			});
			while(!done) ctxt.yield();
		});

		auto producer = std::thread([&]{
			p.set_value(12);
		});

		consumer.join();
		producer.join();
	}


	TEST(Future, ParallelList_threads) {
		const static int NUM_FUTURES = 1000;

		FiberContext ctxt;
		std::array<Promise<int>,NUM_FUTURES> ps;
		std::vector<Future<int>> fs;

		for(int i=0; i<NUM_FUTURES; i++) {
			ps[i] = Promise<int>(ctxt);
			fs.emplace_back(ps[i].get_future());
		}

		std::atomic<bool> done(false);
		auto consumer = std::thread([&]{
			ctxt.start([&]{
				for(int i=0; i<NUM_FUTURES; i++) {
					EXPECT_EQ(i,fs[i].get());
				}
				done = true;
			});
			while(!done) ctxt.yield();
		});

		auto producer = std::thread([&]{
			for(int i=0; i<NUM_FUTURES; i++) {
				ps[i].set_value(i);
			}
		});

		consumer.join();
		producer.join();
	}


	TEST(Future, SequentialList_fiber) {
		const static int NUM_FUTURES = 1000;

		std::array<Promise<int>,NUM_FUTURES> ps;
		std::vector<Future<int>> fs;

		FiberContext ctxt;
		for(int i=0; i<NUM_FUTURES; i++) {
			ps[i] = Promise<int>(ctxt);
			fs.emplace_back(ps[i].get_future());
		}

		int msg_counter = 0;

		// let the consumer be a fiber
		ctxt.start([&]{
			for(int i=0; i<NUM_FUTURES; i++) {
				EXPECT_EQ(i,fs[i].get());
				msg_counter++;
			}
		});

		// produce all the values (causes consumption as a side-effect)
		for(int i=0; i<NUM_FUTURES; i++) {
			ps[i].set_value(i);
		}

		ctxt.yield();

		EXPECT_EQ(NUM_FUTURES,msg_counter);
	}


	TEST(Future, ParallelList_fiber) {
		const static int NUM_FUTURES = 1000;

		std::array<Promise<int>,NUM_FUTURES> ps;
		std::vector<Future<int>> fs;

		FiberContext ctxt;
		for(int i=0; i<NUM_FUTURES; i++) {
			ps[i] = Promise<int>(ctxt);
			fs.emplace_back(ps[i].get_future());
		}

		int msg_counter = 0;

		// let the consumer be a fiber
		ctxt.start([&]{
			for(int i=0; i<NUM_FUTURES; i++) {

				if (i % 2) {
					EXPECT_TRUE(fs[i].valid());
					EXPECT_EQ(i,fs[i].get());
				} else {
					Future<int> f;
					EXPECT_FALSE(f.valid());
					f = std::move(fs[i]);
					EXPECT_FALSE(fs[i].valid());
					EXPECT_TRUE(f.valid());
					EXPECT_EQ(i,f.get());
				}

				msg_counter++;
			}
		});

		// the producer is a thread
		auto producer = std::thread([&]{
			for(int i=0; i<NUM_FUTURES; i++) {
				ps[i].set_value(i);
			}
		});

		// let producer finish (will also drive consumer)
		producer.join();

		ctxt.yield();

		EXPECT_EQ(NUM_FUTURES,msg_counter);
	}

} // end of namespace fiber
} // end of namespace utils
} // end of namespace allscale
