
#include "allscale/runtime/data/data_item_manager.h"

#include "allscale/runtime/log/logger.h"

#include "allscale/runtime/com/hierarchy.h"
#include "allscale/runtime/data/data_item_index.h"

namespace allscale {
namespace runtime {
namespace data {


	// start up the treeture service within the given network
	void installDataItemManagerService(com::Network& network) {
		network.installServiceOnNodes<DataItemManagerService>();
	}


	DataItemManagerService::DataItemManagerService(com::Node& node)
		: network(com::Network::getNetwork()), rank(node.getRank()) {}

	// a function to retrieve the local instance of this service
	DataItemManagerService& DataItemManagerService::getLocalService() {
		return com::Node::getLocalService<DataItemManagerService>();
	}


	void DataItemManagerService::allocate(const DataItemRequirements& reqs) {

		// do not wait for empty requirements
		if (reqs.empty()) return;

		// todo: record access locks

		// get access to the local data item index service
		auto& diis = com::HierarchicalOverlayNetwork::getLocalService<DataItemIndexService>();

		// retrieve ownership of missing data
		auto missing = difference(reqs.getWriteRequirements(),diis.getAvailableData());
		if (!missing.empty()) {
			acquire(missing);
		}

		// now all write-requirements should be satisfied
		assert_pred2(
			data::isSubRegion,
			reqs.getWriteRequirements(),
			diis.getAvailableData()
		);

		// locate all read requirements
		auto locations = diis.locate(reqs.getReadRequirements());

		// import all read requirements
		retrieve(locations);

		// check that all read requirements are now covered
//		assert_pred2(
//			data::isSubRegion,
//			reqs.getReadRequirements(),
//			diis.getAvailableData()
//		);
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

		// also check read requirements
//		assert_pred2(
//			data::isSubRegion,
//			reqs.getReadRequirements(),
//			com::HierarchicalOverlayNetwork::getLocalService<DataItemIndexService>().getAvailableData()
//		);
	}

	/**
	 * Instructs the data item manager to retrieve all data listed in the given location summary.
	 */
	void DataItemManagerService::retrieve(const DataItemLocationInfos& infos) {
		for(const auto& cur : registers) {
			cur.second->retrieve(infos);
		}
	}

	void DataItemManagerService::acquire(const DataItemRegions& regions) {

		// if there is nothing to get, be done
		if (regions.empty()) return;

		// get access to the local data item index service
		auto& diis = com::HierarchicalOverlayNetwork::getLocalService<DataItemIndexService>();

		// forward this call to the index server
		diis.acquire(regions);

	}

	void DataItemManagerService::takeOwnership(const DataItemMigrationData& data) {
		// import data into actual data fragment managers ...
		for(const auto& cur : registers) {
			cur.second->takeOwnership(data);
		}
	}

	DataItemRegions DataItemManagerService::getExclusiveRegions() const {
		DataItemRegions res;
		for(const auto& cur : registers) {
			cur.second->addExclusiveRegions(res);
		}
		return res;
	}


} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
