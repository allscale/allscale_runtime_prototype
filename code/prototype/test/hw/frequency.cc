#include <gtest/gtest.h>

#include "allscale/runtime/hw/frequency.h"
#include "allscale/runtime/hw/frequency_scaling.h"

#include <type_traits>
#include <thread>

#include "allscale/utils/string_utils.h"

using namespace std::literals::chrono_literals;

namespace allscale {
namespace runtime {
namespace hw {

	TEST(Frequency, Basic) {

		// check constructors
		EXPECT_TRUE(std::is_default_constructible<Frequency>::value);
		EXPECT_TRUE(std::is_copy_constructible<Frequency>::value);
		EXPECT_TRUE(std::is_move_constructible<Frequency>::value);

		EXPECT_TRUE(std::is_trivially_default_constructible<Frequency>::value);
		EXPECT_TRUE(std::is_trivially_copy_constructible<Frequency>::value);
		EXPECT_TRUE(std::is_trivially_move_constructible<Frequency>::value);

		EXPECT_TRUE(std::is_trivially_copy_assignable<Frequency>::value);
		EXPECT_TRUE(std::is_trivially_move_assignable<Frequency>::value);

		// check serializability
		EXPECT_TRUE(utils::is_trivially_serializable<Frequency>::value);
	}

	TEST(Frequency, Factories) {
		EXPECT_EQ(25000, Frequency::kHz(25).getValue());
		EXPECT_EQ(100000000, Frequency::MHz(100).getValue());
		EXPECT_EQ(50000000000, Frequency::GHz(50).getValue());
	}

	TEST(Frequency, Literals) {
		EXPECT_EQ("25000 Hz",toString(25_kHz));
		EXPECT_EQ("100000000 Hz",toString(100_MHz));
		EXPECT_EQ("50000000000 Hz",toString(50_GHz));
	}

	TEST(Frequency, Cycles) {

		auto f = 100_MHz;

		EXPECT_EQ(100000000,f * 1s);
		EXPECT_EQ(100,f * 1us);

		EXPECT_EQ(100000000, 1s * f);
		EXPECT_EQ(1, 10ns * f);

	}

#ifdef USE_LINUX_CPUFREQ

	TEST(Cpufreq, GetFrequencyOptions_Caching) {
		// note: we can't be sure when this test case is executed, so
		// the only thing we can check is that repeated calls only increment
		// the counter by 1 at most
		auto start_num = testing::getFrequencyOptions_num_file_accesses;
		getFrequencyOptions(0);
		getFrequencyOptions(0);
		getFrequencyOptions(0);
		EXPECT_LE(testing::getFrequencyOptions_num_file_accesses, start_num+1);
	}

	TEST(Cpufreq, GetFrequencyOptions) {
		auto frequencies = getFrequencyOptions(0);

		// sanity checks
		ASSERT_GT(frequencies.size(), 1);
		EXPECT_GT(frequencies[0], 1_MHz);
		EXPECT_LT(frequencies[0], 10_GHz);

		// check order
		auto prev_freq = frequencies.front();
		for(auto freq : frequencies) {
			EXPECT_LE(prev_freq, freq);
			prev_freq = freq;
		}

		// debug output
		std::cout << "Core 0 frequencies:\n";
		for(auto freq : frequencies) {
			std::cout << freq << "\n";
		}
	}

	TEST(Cpufreq, GetFrequency) {
		auto f = getFrequency(0);
		auto freqs = getFrequencyOptions(0);
		EXPECT_GE(f, freqs.front());
		EXPECT_LE(f, freqs.back());
		EXPECT_NE(std::find(freqs.cbegin(), freqs.cend(), f), freqs.cend());

		// debug output
		std::cout << "Core 0 cur_freq: " << f << "\n";
	}

	TEST(Cpufreq, SetFrequency) {
		using namespace std::literals;

		auto freqs = getFrequencyOptions(0);
		ASSERT_GE(freqs.size(), 2) << "Need at least 2 frequencies available for this test";

		// the sleep period required is system-dependent, but 500 ms should be enough for everyone

		EXPECT_TRUE(setFrequency(0, freqs.front()));
		std::this_thread::sleep_for(500ms);
		EXPECT_EQ(getFrequency(0), freqs.front());

		EXPECT_TRUE(setFrequency(0, freqs[1]));
		std::this_thread::sleep_for(500ms);
		EXPECT_EQ(getFrequency(0), freqs[1]);

		EXPECT_TRUE(setFrequency(0, freqs.front()));
		std::this_thread::sleep_for(500ms);
		EXPECT_EQ(getFrequency(0), freqs.front());

		resetFrequency(0);
	}

#endif

} // end of namespace hw
} // end of namespace runtime
} // end of namespace allscale
