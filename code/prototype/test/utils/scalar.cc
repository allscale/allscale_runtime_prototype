#include <gtest/gtest.h>

#include "allscale/utils/scalar.h"

namespace allscale {
namespace utils {


	TEST(Scalar, Basic_Math) {

		struct S : public Scalar<int, S> {
			S() = default;
			S(int v) : Scalar<int, S>(v) {};
		};

		S a = 5;
		S b = 2;
		S c = 7;
		EXPECT_EQ(a + b, c);
		EXPECT_EQ(a + b, 7);
		EXPECT_EQ(a + 2, 7);
		EXPECT_EQ(5 + b, 7);

		S d = 3;
		EXPECT_EQ(a - b, d);
		EXPECT_EQ(a - b, 3);
		EXPECT_EQ(a - 2, 3);
		EXPECT_EQ(5 - b, 3);

		a += 2;
		EXPECT_EQ(a, 7);
		EXPECT_EQ(a, c);
		a += a;
		EXPECT_EQ(a, 14);
		a -= c;
		EXPECT_EQ(a, 7);
		a -= 2;
		EXPECT_EQ(a, 5);
	}


} // end namespace utils
} // end namespace allscale
