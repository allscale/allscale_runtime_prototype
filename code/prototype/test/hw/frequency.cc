#include <gtest/gtest.h>

#include "allscale/runtime/hw/frequency.h"

#include <type_traits>

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

	TEST(Frequency, Literals) {
		EXPECT_EQ("25000 Hz",toString(25_kHz));
		EXPECT_EQ("100000000 Hz",toString(100_MHz));
	}

	TEST(Frequency, Cycles) {

		auto f = 100_MHz;

		EXPECT_EQ(100000000,f * 1s);
		EXPECT_EQ(100,f * 1us);

		EXPECT_EQ(100000000, 1s * f);
		EXPECT_EQ(1, 10ns * f);

	}

} // end of namespace hw
} // end of namespace runtime
} // end of namespace allscale
