#include <gtest/gtest.h>

#include "allscale/runtime/utils/timer.h"

namespace allscale {
namespace runtime {
namespace utils {


	TEST(Timer,Basic_Start_Stop) {

		PeriodicExecutor exec;

		// just see whether it terminates

		{
			PeriodicExecutor exec;

			exec.runPeriodically([]{
				std::cout << "Hello World\n";
				return false;
			});

		}


	}


	TEST(Timer,FiveSteps) {
		using namespace std::chrono;
		using clock = high_resolution_clock;

		milliseconds interval(50);

		PeriodicExecutor exec;

		auto start = clock::now();
		int counter = 0;

		exec.runPeriodically([&]{
			counter++;

			auto delta = duration_cast<milliseconds>(clock::now() - start);
			EXPECT_LE(50*counter,delta.count());
			EXPECT_LT(delta.count(),50*(counter+1));

			return true;
		},interval);

		std::this_thread::sleep_for(interval * 5.5);

		EXPECT_EQ(5,counter);
	}



	TEST(Timer,ThreeSteps) {
		using namespace std::chrono;
		using clock = high_resolution_clock;

		milliseconds interval(50);

		PeriodicExecutor exec;

		auto start = clock::now();
		int counter = 0;

		exec.runPeriodically([&]{
			counter++;

			auto delta = duration_cast<milliseconds>(clock::now() - start);
			EXPECT_LE(50*counter,delta.count());
			EXPECT_LT(delta.count(),50*(counter+1));

			return counter < 3;
		},interval);

		std::this_thread::sleep_for(interval * 5.5);

		EXPECT_EQ(3,counter);
	}



	TEST(Timer,MultiTasks) {
		using namespace std::chrono;
		using clock = high_resolution_clock;


		PeriodicExecutor exec;

		auto start = clock::now();
		int counterA = 0;
		int counterB = 0;

		milliseconds intervalA(50);
		milliseconds intervalB(20);

		exec.runPeriodically([&]{
			counterA++;

			auto delta = duration_cast<milliseconds>(clock::now() - start);
			EXPECT_LE(50*counterA,delta.count());
			EXPECT_LT(delta.count(),50*(counterA+1));

			return true;
		},intervalA);

		exec.runPeriodically([&]{
			counterB++;

			auto delta = duration_cast<milliseconds>(clock::now() - start);
			EXPECT_LE(20*counterB,delta.count());
			EXPECT_LT(delta.count(),20*(counterB+1));

			return counterB < 3;
		},intervalB);

		std::this_thread::sleep_for(intervalA * 5.5);

		EXPECT_EQ(5,counterA);
		EXPECT_EQ(3,counterB);
	}

} // end namespace utils
} // end namespace runtime
} // end namespace allscale
