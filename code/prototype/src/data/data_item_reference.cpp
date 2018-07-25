
#include "allscale/runtime/data/data_item_reference.h"

namespace allscale {
namespace runtime {
namespace data {

	DataItemID getFreshDataItemID() {
		static DataItemID counter = 0;
		return ++counter;
	}

} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
