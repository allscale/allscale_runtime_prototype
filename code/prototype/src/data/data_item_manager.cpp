
#include "allscale/runtime/data/data_item_manager.h"

#include "allscale/runtime/log/logger.h"

namespace allscale {
namespace runtime {
namespace data {


	DataItemManagerService::DataItemManagerService(com::Node&) {}

	// a function to retrieve the local instance of this service
	DataItemManagerService& DataItemManagerService::getLocalService() {
		return com::Node::getLocalService<DataItemManagerService>();
	}


	void DataItemManagerService::allocate(const DataItemRequirements& reqs) {

		// do not wait for empty requirements
		if (reqs.empty()) return;

		DLOG << "Requested " << reqs;

		// the rest is still missing
//		assert_not_implemented();
	}


	void DataItemManagerService::release(const DataItemRequirements& reqs) {

		// no work required for empty requirements
		if (reqs.empty()) return;

		DLOG << "Releasing " << reqs;

		// the rest is still missing
//		assert_not_implemented();
	}


} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
