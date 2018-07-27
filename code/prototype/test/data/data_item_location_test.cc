#include <gtest/gtest.h>

#include "allscale/runtime/data/data_item_location.h"

#include "allscale/api/user/data/grid.h"
#include "allscale/utils/string_utils.h"

using namespace allscale::utils;
using namespace allscale::api::core;
using namespace allscale::api::user::data;

namespace allscale {
namespace runtime {
namespace data {

	TEST(DataItemLocationInfos, Traits) {

		// check constructors
		EXPECT_TRUE(std::is_default_constructible<DataItemLocationInfos>::value);
		EXPECT_TRUE(std::is_copy_constructible<DataItemLocationInfos>::value);
		EXPECT_TRUE(std::is_move_constructible<DataItemLocationInfos>::value);

		// check serializability
		EXPECT_TRUE(allscale::utils::is_serializable<DataItemLocationInfos>::value);
	}

	TEST(DataItemLocationInfos, Create) {
		// just create an instance
		DataItemLocationInfos info;
		EXPECT_TRUE(info.empty());
	}


} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
