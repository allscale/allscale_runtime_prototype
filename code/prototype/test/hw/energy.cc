#include <gtest/gtest.h>

#include "allscale/runtime/hw/energy.h"

#include <type_traits>

#include "allscale/utils/string_utils.h"

namespace allscale {
namespace runtime {
namespace hw {

	TEST(Energy, Basic) {

		// check constructors
		EXPECT_TRUE(std::is_default_constructible<Energy>::value);
		EXPECT_TRUE(std::is_copy_constructible<Energy>::value);
		EXPECT_TRUE(std::is_move_constructible<Energy>::value);

		EXPECT_TRUE(std::is_trivially_default_constructible<Energy>::value);
		EXPECT_TRUE(std::is_trivially_copy_constructible<Energy>::value);
		EXPECT_TRUE(std::is_trivially_move_constructible<Energy>::value);

		EXPECT_TRUE(std::is_trivially_copy_assignable<Energy>::value);
		EXPECT_TRUE(std::is_trivially_move_assignable<Energy>::value);

		// check serializability
		EXPECT_TRUE(utils::is_trivially_serializable<Energy>::value);
	}

	TEST(Energy, Literals) {
		EXPECT_EQ("2.5e-11 J",toString(25_pJ));
		EXPECT_EQ("0.0002 J",toString(0.2_mJ));
	}

	TEST(Energy, Estimates) {

		auto f = 100_MHz;
		auto res = estimateEnergyUsage(f,10);
		EXPECT_LE(0,res.toJoule());
	}

} // end of namespace hw
} // end of namespace runtime
} // end of namespace allscale
