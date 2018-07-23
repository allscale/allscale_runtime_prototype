#include <gtest/gtest.h>

#include "allscale/runtime/data/data_fragment_manager.h"
#include "allscale/runtime/com/network.h"

#include "allscale/api/user/data/grid.h"
#include "allscale/utils/string_utils.h"

using namespace allscale::api::core;
using namespace allscale::api::user::data;

namespace allscale {
namespace runtime {
namespace data {

	TEST(DataFragmentManager, Creation) {

		// create a network
		com::Network network(9);

		using mgr_t = DataFragmentManager<Grid<int,2>>;
		mgr_t* mgr_pointer = nullptr;

		// create a data fragment manager in the first node (option: add a factory for this)
		network.runOn(0,[&](com::Node& node){
			// start up data Item Manager;
			GridSharedData<2> shared_data({20,20});
			mgr_pointer = &node.startService<mgr_t>(std::move(shared_data));
		});

		// check that manager could be extracted successfully
		EXPECT_TRUE(mgr_pointer);

		mgr_t& mgr = *mgr_pointer;

		auto grid = mgr.getDataItem();
		EXPECT_EQ("[20,20]",toString(grid.size()));
	}

} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
