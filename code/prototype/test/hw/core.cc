#include <gtest/gtest.h>

#include <set>
#include "allscale/runtime/hw/core.h"

namespace allscale {
namespace runtime {
namespace hw {

	TEST(Core, Basic) {

		// check number of cores
		EXPECT_LT(0,getNumberAvailableCores());

		// test that the list of available cores has the right size
		int num = getNumberAvailableCores();
		auto cores = getAvailableCores();
		EXPECT_EQ(num,cores.size());

		// test that there are no duplicates
		std::set<int> set(cores.begin(),cores.end());
		EXPECT_EQ(num,set.size());
	}

} // end of namespace hw
} // end of namespace runtime
} // end of namespace allscale
