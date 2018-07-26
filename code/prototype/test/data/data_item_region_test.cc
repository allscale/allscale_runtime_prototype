#include <gtest/gtest.h>

#include "allscale/runtime/data/data_item_region.h"

#include "allscale/api/user/data/grid.h"
#include "allscale/utils/string_utils.h"

using namespace allscale::utils;
using namespace allscale::api::core;
using namespace allscale::api::user::data;

namespace allscale {
namespace runtime {
namespace data {


	TEST(DataItemRegion, Serialization) {
		using region = DataItemRegion<Grid<float,2>>;
		EXPECT_TRUE(allscale::utils::is_serializable<region>::value);
	}


	TEST(DataItemRegions, Create) {
		// just create an instance
		DataItemRegions a;
		EXPECT_TRUE(a.empty());
	}

	TEST(DataItemRegions, Serialization) {
		// regions need to be serializable too
		EXPECT_TRUE(allscale::utils::is_serializable<DataItemRegions>::value);
	}

	TEST(DataItemRegions, Merge) {
		using data_item = Grid<float,1>;
		using ref_t = DataItemReference<data_item>;
		using region = DataItemRegion<data_item>;

		ref_t a(1);
		ref_t b(2);

		DataItemRegions aE;
		EXPECT_EQ("Regions()",toString(aE));

		DataItemRegions aA1;
		aA1.add(region(a,{10,20}));
		EXPECT_EQ("Regions(DI-1:{[[10] - [20])})",toString(aA1));

		DataItemRegions aA2;
		aA2.add(region(a,{15,25}));
		EXPECT_EQ("Regions(DI-1:{[[15] - [25])})",toString(aA2));

		DataItemRegions aB1;
		aB1.add(region(b,{15,25}));
		EXPECT_EQ("Regions(DI-2:{[[15] - [25])})",toString(aB1));

		// X + X must be X
		EXPECT_EQ(aE,merge(aE,aE));
		EXPECT_EQ(aA1,merge(aA1,aA1));
		EXPECT_EQ(aA2,merge(aA2,aA2));
		EXPECT_EQ(aB1,merge(aB1,aB1));

		// E + X must be X
		EXPECT_EQ(aE,merge(aE,aE));
		EXPECT_EQ(aA1,merge(aA1,aE));
		EXPECT_EQ(aA2,merge(aA2,aE));
		EXPECT_EQ(aB1,merge(aB1,aE));

		EXPECT_EQ(aA1,merge(aE,aA1));
		EXPECT_EQ(aA2,merge(aE,aA2));
		EXPECT_EQ(aB1,merge(aE,aB1));

		// some other results
		EXPECT_EQ("Regions(DI-1:{[[10] - [25])})",toString(merge(aA1,aA2)));
		EXPECT_EQ("Regions(DI-1:{[[10] - [20])},DI-2:{[[15] - [25])})",toString(merge(aA1,aB1)));
	}

	TEST(DataItemRegions, Intersect) {
		using data_item = Grid<float,1>;
		using ref_t = DataItemReference<data_item>;
		using region = DataItemRegion<data_item>;

		ref_t a(1);
		ref_t b(2);

		DataItemRegions aE;
		EXPECT_EQ("Regions()",toString(aE));

		DataItemRegions aA1;
		aA1.add(region(a,{10,20}));
		EXPECT_EQ("Regions(DI-1:{[[10] - [20])})",toString(aA1));

		DataItemRegions aA2;
		aA2.add(region(a,{15,25}));
		EXPECT_EQ("Regions(DI-1:{[[15] - [25])})",toString(aA2));

		DataItemRegions aB1;
		aB1.add(region(b,{15,25}));
		EXPECT_EQ("Regions(DI-2:{[[15] - [25])})",toString(aB1));

		// X * X must be X
		EXPECT_EQ(aE,intersect(aE,aE));
		EXPECT_EQ(aA1,intersect(aA1,aA1));
		EXPECT_EQ(aA2,intersect(aA2,aA2));
		EXPECT_EQ(aB1,intersect(aB1,aB1));

		// E * X must be E
		EXPECT_EQ(aE,intersect(aE,aE));
		EXPECT_EQ(aE,intersect(aA1,aE));
		EXPECT_EQ(aE,intersect(aA2,aE));
		EXPECT_EQ(aE,intersect(aB1,aE));

		EXPECT_EQ(aE,intersect(aE,aA1));
		EXPECT_EQ(aE,intersect(aE,aA2));
		EXPECT_EQ(aE,intersect(aE,aB1));

		// some other results
		EXPECT_EQ("Regions(DI-1:{[[15] - [20])})",toString(intersect(aA1,aA2)));
		EXPECT_EQ("Regions()",toString(intersect(aA1,aB1)));
	}

	TEST(DataItemRegions, Difference) {
		using data_item = Grid<float,1>;
		using ref_t = DataItemReference<data_item>;
		using region = DataItemRegion<data_item>;

		ref_t a(1);
		ref_t b(2);

		DataItemRegions aE;
		EXPECT_EQ("Regions()",toString(aE));

		DataItemRegions aA1;
		aA1.add(region(a,{10,20}));
		EXPECT_EQ("Regions(DI-1:{[[10] - [20])})",toString(aA1));

		DataItemRegions aA2;
		aA2.add(region(a,{15,25}));
		EXPECT_EQ("Regions(DI-1:{[[15] - [25])})",toString(aA2));

		DataItemRegions aB1;
		aB1.add(region(b,{15,25}));
		EXPECT_EQ("Regions(DI-2:{[[15] - [25])})",toString(aB1));

		// X\X must be empty
		auto empty = [](auto& cur) { return cur.empty(); };

		EXPECT_PRED1(empty,difference(aE,aE));
		EXPECT_PRED1(empty,difference(aA1,aA1));
		EXPECT_PRED1(empty,difference(aA2,aA2));
		EXPECT_PRED1(empty,difference(aB1,aB1));

		// some other results
		EXPECT_EQ("Regions(DI-1:{[[10] - [15])})",toString(difference(aA1,aA2)));
		EXPECT_EQ("Regions(DI-1:{[[10] - [20])})",toString(difference(aA1,aB1)));
	}

} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
