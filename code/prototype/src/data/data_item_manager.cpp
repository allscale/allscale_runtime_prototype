
#include "allscale/runtime/data/data_item_manager.h"

namespace allscale {
namespace runtime {
namespace data {


	DataItemManagerService::DataItemManagerService(com::Node&) {}

	// a function to retrieve the local instance of this service
	DataItemManagerService& DataItemManagerService::getLocalService() {
		return com::Node::getLocalService<DataItemManagerService>();
	}


} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
