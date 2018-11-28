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


	TEST(DataItemLocationsInfos, Serialization) {
		// regions need to be serializable too
		EXPECT_TRUE(allscale::utils::is_serializable<DataItemLocationInfos>::value);

		using data_item = Grid<float,1>;
		using ref_t = DataItemReference<data_item>;

		// create a data item location info and store it in an archive
		allscale::utils::Archive a = allscale::utils::serialize(1);
		{
			DataItemLocationInfos infos;
			infos.add(ref_t(1),{0,10},2);
			EXPECT_EQ("Locations(2=>Regions(DI-1:{[[0] - [10])}))",toString(infos));
			a = allscale::utils::serialize(infos);
		}

		// now the original info is gone, only the copy in the archive survived
		// => restore this one
		{
			auto infos = allscale::utils::deserialize<DataItemLocationInfos>(a);
			EXPECT_EQ("Locations(2=>Regions(DI-1:{[[0] - [10])}))",toString(infos));
		}

	}

} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
