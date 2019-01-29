
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
		: network(com::Network::getNetwork()),
		  rank(node.getRank()),
		  allocate_call_count(0),
		  release_call_count(0),
		  locate_call_count(0),
		  retrieve_call_count(0),
		  acquire_call_count(0)
	{}

	// a function to retrieve the local instance of this service
	DataItemManagerService& DataItemManagerService::getLocalService() {
		return com::Node::getLocalService<DataItemManagerService>();
	}


	void DataItemManagerService::allocate(const DataItemRequirements& reqs) {

		// increment counter
		allocate_call_count.fetch_add(1,std::memory_order_relaxed);

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

		// import all read requirements
		retrieve(reqs.getReadRequirements());

		// check that all read requirements are now covered
//		assert_pred2(
//			data::isSubRegion,
//			reqs.getReadRequirements(),
//			diis.getAvailableData()
//		);
	}


	void DataItemManagerService::release(const DataItemRequirements& reqs) {

		// increment counter
		release_call_count.fetch_add(1,std::memory_order_relaxed);

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
	 * Instructs the data item manager to retrieve all data listed in the given regions.
	 */
	void DataItemManagerService::retrieve(const DataItemRegions& requestedRegions) {

		// increment counter
		retrieve_call_count.fetch_add(1,std::memory_order_relaxed);

		// if there is nothing to cover, there is nothing to do
		if (requestedRegions.empty()) return;

		// crop to actual data item size
		auto regions = intersect(getFullRegions(),requestedRegions);

		// if there is now nothing to do, be done
		if (regions.empty()) return;

		// while the retievel was not successful ...
		while(true) {

			// -- locate data - if possible through the cache --

			// locate all read requirements
			auto entry = locationCache.lookup(regions);
			if (!entry || entry->empty()) {

				// get access to the local data item index service
				auto& diis = com::HierarchicalOverlayNetwork::getLocalService<DataItemIndexService>();

				// count locate call
				locate_call_count.fetch_add(1,std::memory_order_relaxed);

				// update locations in cache
				auto locations = diis.locate(regions);

				// add resolved data to the location cache
				bool valid = regions == locations.getCoveredRegions();
				entry = &locationCache.update(regions,locations,valid);
			}

			assert_true(entry);
			auto& locations = *entry;


			// -- retrieve the data, if not available, restart --

			std::vector<std::pair<const DataItemRegions*,allscale::runtime::com::RemoteCallResult<DataItemMigrationData>>> calls;
			calls.reserve(locations.getLocationInfo().size());


			// track whether all data has been found where expected
			bool allFine = true;

			// a utility to invalidate the utilized cache entry
			auto invalidateCache = [&]{
				// clear cache entry and restart retrieval (not most efficient)
				locationCache.clear(regions);

				// we have to restart
				allFine = false;
			};


			// start RPC calls to retrieve data from their source locations
			for(const auto& cur : locations.getLocationInfo()) {

				// handle local queries
				if (cur.first == rank) {

					// check that expected data is present
					bool ok = true;
					{
						// get a read permission for the exclusive region
						allscale::utils::fiber::ReadGuard g(exclusiveRegionsLock);
						ok = isSubRegion(cur.second,getExclusiveRegionsInternal());
					}

					if (!ok) {
						// invalidate cache and restart
						invalidateCache();
						break;
					}

					// can be skipped
					continue;
				}

				// retrieve data
				calls.emplace_back(
					&cur.second,
					network.getRemoteProcedure(cur.first,&DataItemManagerService::extractRegions)(cur.second)
				);

			}

			// if not all calls have been send => restart
			if (!allFine) continue;

			// wait for data to be retrieved
			std::vector<DataItemMigrationData> allData;
			allData.reserve(locations.getLocationInfo().size());

			for(auto& call : calls) {
				// retrieve data
				allData.emplace_back(call.second.get());

				// test that all data is included
				if (*call.first != allData.back().getCoveredRegions()) {
					invalidateCache();
					break;
				}
			}

			// if not all data could be collected => restart
			if (!allFine) continue;

			// integrate data locally
			for(auto& cur : registers) {
				for(const auto& data : allData) {
					cur.second->import(data);
				}
			}

			return;
		}

	}

	void DataItemManagerService::acquire(const DataItemRegions& regions) {

		// increment counter
		acquire_call_count.fetch_add(1,std::memory_order_relaxed);

		// if there is nothing to get, be done
		if (regions.empty()) return;

		// get access to the local data item index service
		auto& diis = com::HierarchicalOverlayNetwork::getLocalService<DataItemIndexService>();

		// forward this call to the index server
		diis.acquire(regions);

	}

	DataItemMigrationData DataItemManagerService::extractRegions(const DataItemRegions& regions) const {

		// start with nothing
		DataItemMigrationData res;

		// collect the data
		for(const auto& cur : registers) {
			cur.second->extract(regions,res);
		}

		// done
		return res;
	}

	void DataItemManagerService::addExclusive(const DataItemRegions& regions) {
		// resize to fit actual regions
		for(const auto& cur : registers) {
			cur.second->addExclusiveRegions(regions);
		}

		allscale::utils::fiber::WriteGuard g(exclusiveRegionsLock);
		exclusiveRegions.add(regions);
	}

	void DataItemManagerService::removeExclusive(const DataItemRegions& regions) {
		// resize to fit actual regions
		for(const auto& cur : registers) {
			cur.second->removeExclusiveRegions(regions);
		}

		allscale::utils::fiber::WriteGuard g(exclusiveRegionsLock);
		exclusiveRegions = difference(exclusiveRegions, regions);
	}


	void DataItemManagerService::takeOwnership(const DataItemMigrationData& data) {
		// import data into actual data fragment managers ...
		for(const auto& cur : registers) {
			cur.second->takeOwnership(data);
		}
	}


	DataItemRegions DataItemManagerService::getExclusiveRegions() const {
		allscale::utils::fiber::ReadGuard g(exclusiveRegionsLock);
		return exclusiveRegions;
	}

	const DataItemRegions& DataItemManagerService::getExclusiveRegionsInternal() const {
		allscale::utils::fiber::ReadGuard g(exclusiveRegionsLock);
		return exclusiveRegions;
	}


	DataItemManagerStatisticEntry DataItemManagerService::getLocalStatistic() const {
		DataItemManagerStatisticEntry res;

		res.locate_calls   = locate_call_count.load(std::memory_order_relaxed);
		res.retrieve_calls = retrieve_call_count.load(std::memory_order_relaxed);
		res.acquire_calls  = acquire_call_count.load(std::memory_order_relaxed);
		res.allocate_calls = allocate_call_count.load(std::memory_order_relaxed);
		res.release_calls  = release_call_count.load(std::memory_order_relaxed);

		return res;
	}


	DataItemManagerStatistic DataItemManagerService::getStatistics() const {
		// collect handles for remote calls
		std::vector<com::RemoteCallResult<DataItemManagerStatisticEntry>> calls;
		for(com::rank_t i = 0; i<network.numNodes(); i++) {
			calls.push_back(network.getRemoteProcedure(i,&DataItemManagerService::getLocalStatistic)());
		}
		// aggregate results
		DataItemManagerStatistic res;
		for(com::rank_t i = 0; i<network.numNodes(); i++) {
			res.add(i,calls[i].get());
		}
		return res;
	}


} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
