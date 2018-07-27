#include <gtest/gtest.h>

#include "allscale/runtime/work/treeture.h"


namespace allscale {
namespace runtime {
namespace work {

	TEST(Treeture, DefaultTreeture) {

		treeture<int> x;
		EXPECT_TRUE(x.isDone());

	}

	TEST(Treeture, Serializable) {

		EXPECT_TRUE(allscale::utils::is_serializable<treeture<void>>::value);
		EXPECT_TRUE(allscale::utils::is_serializable<treeture<int>>::value);
	}

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
