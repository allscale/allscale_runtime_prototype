#include <gtest/gtest.h>

#include "allscale/runtime/data/data_item_requirement.h"

#include "allscale/api/user/data/grid.h"
#include "allscale/utils/string_utils.h"

using namespace allscale::utils;
using namespace allscale::api::core;
using namespace allscale::api::user::data;

namespace allscale {
namespace runtime {
namespace data {



	TEST(DataItemRequirement, Create) {

		// just create an instance
		DataItemRequirements reqs;
		EXPECT_TRUE(reqs.empty());
	}


	TEST(DataItemRequirement, AddingRequirements) {

		using ref_t = DataItemReference<Grid<float,1>>;
		using region_t = Grid<float,1>::region_type;

		ref_t a(1);

		// just create an instance
		DataItemRequirements reqs;
		EXPECT_TRUE(reqs.empty()) << reqs;

		// insert an empty requirement
		reqs.add(createDataItemRequirement(a,region_t(),ReadOnly));
		EXPECT_TRUE(reqs.empty()) << reqs;

		// insert a real requirement
		reqs.add(createDataItemRequirement(a,region_t(0,10),ReadOnly));
		EXPECT_FALSE(reqs.empty()) << reqs;

		EXPECT_EQ("Requirements( RO: Regions(DI-1:{[[0] - [10])}), RW: Regions())",toString(reqs));

		// insert another requirement
		reqs.add(createDataItemRequirement(a,region_t(5,15),ReadOnly));
		EXPECT_FALSE(reqs.empty()) << reqs;

		EXPECT_EQ("Requirements( RO: Regions(DI-1:{[[0] - [15])}), RW: Regions())",toString(reqs));

		// insert a write requirement
		reqs.add(createDataItemRequirement(a,region_t(5,10),ReadWrite));
		EXPECT_FALSE(reqs.empty()) << reqs;

		EXPECT_EQ("Requirements( RO: Regions(DI-1:{[[0] - [15])}), RW: Regions(DI-1:{[[5] - [10])}))",toString(reqs));

	}

	TEST(DataItemRequirement, FromTuple) {

		using ref_t = DataItemReference<Grid<float,1>>;
		using region_t = Grid<float,1>::region_type;

		ref_t a(1);
		ref_t b(2);

		// test the empty tuple
		auto tupleE = std::make_tuple();
		auto reqE = DataItemRequirements::fromTuple(tupleE);
		EXPECT_TRUE(reqE.empty());

		// test a single requirement
		auto tuple1 = std::make_tuple(
				createDataItemRequirement(a,region_t(0,10),ReadOnly)
		);
		auto req1 = DataItemRequirements::fromTuple(tuple1);
		EXPECT_FALSE(req1.empty());

		EXPECT_EQ("Requirements( RO: Regions(DI-1:{[[0] - [10])}), RW: Regions())",toString(req1));


		// test multiple requirements
		auto tuple2 = std::make_tuple(
				createDataItemRequirement(a,region_t(0,10),ReadOnly),
				createDataItemRequirement(b,region_t(0,10),ReadWrite)
		);
		auto req2 = DataItemRequirements::fromTuple(tuple2);
		EXPECT_FALSE(req2.empty());

		EXPECT_EQ("Requirements( RO: Regions(DI-1:{[[0] - [10])}), RW: Regions(DI-2:{[[0] - [10])}))",toString(req2));

	}

} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
