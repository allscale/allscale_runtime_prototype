
#include "allscale/runtime/data/data_item_manager.h"

#include "allscale/runtime/log/logger.h"

#include "allscale/runtime/com/hierarchy.h"
#include "allscale/runtime/data/data_item_index.h"

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

		// todo: retrieve read-only data

		// todo: record access locks

//		std::cout << "Running task with " << reqs << "\n";

		// for now, just test that data is available
		assert_pred2(
			data::isSubRegion,
			reqs.getWriteRequirements(),
			com::HierarchicalOverlayNetwork::getLocalService<DataItemIndexService>().getAvailableData()
		);
	}


	void DataItemManagerService::release(const DataItemRequirements& reqs) {

		// no work required for empty requirements
		if (reqs.empty()) return;

		// todo: release access locks

		// for now: check that data is still owned
		assert_pred2(
			data::isSubRegion,
			reqs.getWriteRequirements(),
			com::HierarchicalOverlayNetwork::getLocalService<DataItemIndexService>().getAvailableData()
		);

	}


} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
