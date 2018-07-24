#include <gtest/gtest.h>

#include "allscale/runtime/work/treeture.h"


namespace allscale {
namespace runtime {
namespace work {

	TEST(Treeture, DefaultTreeture) {

		treeture<int> x;
		EXPECT_TRUE(x.isDone());

	}

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
