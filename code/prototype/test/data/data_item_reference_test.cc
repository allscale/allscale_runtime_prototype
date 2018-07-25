#include <gtest/gtest.h>

#include "allscale/runtime/data/data_item_reference.h"

#include "allscale/api/user/data/grid.h"
#include "allscale/utils/string_utils.h"

using namespace allscale::utils;
using namespace allscale::api::core;
using namespace allscale::api::user::data;

namespace allscale {
namespace runtime {
namespace data {



	TEST(DataItemReference, TypeProperties) {

		using ref_type = DataItemReference<Grid<float,2>>;

		// check some basic properties
		EXPECT_FALSE(std::is_default_constructible<ref_type>::value);

		EXPECT_TRUE(std::is_copy_constructible<ref_type>::value);
		EXPECT_TRUE(std::is_move_constructible<ref_type>::value);

		EXPECT_TRUE(std::is_copy_assignable<ref_type>::value);
		EXPECT_TRUE(std::is_move_assignable<ref_type>::value);

		// also: should be serializable
		EXPECT_TRUE(allscale::utils::is_serializable<ref_type>::value);

	}

	TEST(DataItemReference, Printable) {

		using ref_type = DataItemReference<Grid<float,2>>;

		ref_type x = ref_type::getFresh();

		// just print it (value is not fixed)
		EXPECT_FALSE(toString(x).empty());
	}

	TEST(DataItemReference, Serializable) {

		using ref_type = DataItemReference<Grid<float,2>>;

		ref_type x = ref_type::getFresh();
		ref_type y = ref_type::getFresh();

		// make sure those are different
		EXPECT_EQ(x,x);
		EXPECT_EQ(y,y);
		EXPECT_NE(x,y);

		// copy both of them
		auto ax = serialize(x);
		auto ay = serialize(y);

		ref_type cx = deserialize<ref_type>(ax);
		ref_type cy = deserialize<ref_type>(ay);

		EXPECT_NE(&x,&cx);
		EXPECT_NE(&y,&cy);

		EXPECT_EQ(x,cx);
		EXPECT_EQ(y,cy);
	}


	TEST(DataItemReference, Comparable) {

		using ref_type = DataItemReference<Grid<float,2>>;

		ref_type x = ref_type::getFresh();
		ref_type y = ref_type::getFresh();

		EXPECT_NE(x,y);
		EXPECT_TRUE(x<y || y<x);
	}


} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
