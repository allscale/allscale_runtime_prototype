/*
 * The prototype implementation of the data item interface.
 *
 *  Created on: Jul 23, 2018
 *      Author: herbert
 */

#pragma once

#include <map>
#include <unordered_map>
#include <memory>
#include <mutex>
#include <typeinfo>
#include <typeindex>

#include "allscale/utils/assert.h"

#include "allscale/runtime/com/node.h"
#include "allscale/runtime/com/network.h"
#include "allscale/runtime/com/hierarchy.h"

#include "allscale/runtime/data/data_item_reference.h"
#include "allscale/runtime/data/data_item_requirement.h"
#include "allscale/runtime/data/data_item_location.h"
#include "allscale/runtime/data/data_item_migration.h"
#include "allscale/runtime/data/data_item_statistic.h"

namespace allscale {
namespace runtime {
namespace data {

	// -- setup --

	// start up the treeture service within the given network
	void installDataItemManagerService(com::Network&);


	// --- forward declarations of data index interfaces ---

	class DataItemIndexService;

	template<typename DataItem>
	void notifyIndexOnCreation(const DataItemReference<DataItem>&);

	// -----------------------------------------------------


	/**
	 * A fragment manager is managing the locally maintained data of a single data item.
	 * It thus manages the fragment, its content, and the shared data.
	 */
	template<typename DataItem>
	class DataFragmentHandler {

		// Test that the passed data item type is valid.
		static_assert(allscale::api::core::is_data_item<DataItem>::value, "Can only be instantiated for data items!");

		// ------ some type definitions ------

		using shared_data_type = typename DataItem::shared_data_type;
		using region_type      = typename DataItem::region_type;
		using fragment_type    = typename DataItem::fragment_type;
		using facade_type      = typename DataItem::facade_type;


		// -- node-local management information --

		// the management data shared among all instances
		shared_data_type shared_data;

		// the locally maintained data fragment
		fragment_type fragment;

		// the locally maintained exclusive regions
		region_type exclusive;

		// the total reserved region (must be a super-set of exclusive)
		region_type reserved;

		// a lock for operation synchronization
		mutable allscale::utils::fiber::Mutex lock;

		// the kind of guard used for synchronization
		using guard = std::lock_guard<allscale::utils::fiber::Mutex>;

	public:

		DataFragmentHandler(const shared_data_type& shared_data)
			: shared_data(shared_data), fragment(this->shared_data) {

			// make sure that nothing is owned yet
			assert_true(exclusive.empty());
			assert_true(reserved.empty());
			assert_true(fragment.getCoveredRegion().empty());
		}

		// no copy but move
		DataFragmentHandler(const DataFragmentHandler&) = delete;
		DataFragmentHandler(DataFragmentHandler&&) = default;

		/**
		 * Obtains access to the managed fragment through a facade.
		 * The fragment itself is not exposed to avoid messing with covered regions.
		 */
		facade_type getDataItem() {
			return fragment.mask();
		}

	private:

		friend class DataItemManager;

		/**
		 * Provides access to the maintained fragment type to friend types.
		 */
		fragment_type& getFragment() {
			return fragment;
		}

	public:

		region_type getDataItemSize() const {
			return fragment.getTotalSize();
		}

		void resizeExclusive(const region_type& newSize) {

			// reserve the area
			reserve(newSize);

			// update the ownership
			guard g(lock);
			exclusive = newSize;
		}

		const region_type& getExclusiveRegion() const {
			return exclusive;
		}

		void reserve(const region_type& area) {
			// lock down this fragment
			guard g(lock);

			// test whether a change is necessary
			if (allscale::api::core::isSubRegion(area,reserved)) return;

			// grow reserved area
			reserved = region_type::merge(reserved,area);

			// resize fragment
			fragment.resize(reserved);
		}

		allscale::utils::optional<allscale::utils::Archive> extract(const region_type& region) const {
			allscale::utils::ArchiveWriter out;
			{
				// lock down this fragment
				guard g(lock);
				if (!allscale::api::core::isSubRegion(region,exclusive)) return {};	// do not extract non-eclusive content!
				fragment.extract(out,region_type::intersect(region,getDataItemSize()));
			}
			return std::move(out).toArchive();

		}

		void insert(allscale::utils::Archive& data) {
			// lock down this fragment
			guard g(lock);
			allscale::utils::ArchiveReader in(data);
			fragment.insert(in);
		}

	};


	/**
	 * The service running on each node for handling data items of various types.
	 */
	class DataItemManagerService {

		/**
		 * A common base type of all data item registers.
		 */
		class DataItemRegisterBase {
		public:
			virtual ~DataItemRegisterBase() {};
			virtual void import(const DataItemMigrationData&) =0;
			virtual void extract(const DataItemRegions&, DataItemMigrationData&) =0;
			virtual void takeOwnership(const DataItemMigrationData&) =0;
			virtual void addExclusiveRegions(DataItemRegions&) const =0;
		};

		/**
		 * A register for data items of a specific type.
		 */
		template<typename DataItem>
		class DataItemRegister : public DataItemRegisterBase {

			using reference_type = DataItemReference<DataItem>;
			using region_type = typename DataItem::region_type;
			using shared_data_type = typename DataItem::shared_data_type;
			using facade_type = typename DataItem::facade_type;

			// the index of registered items
			std::unordered_map<DataItemReference<DataItem>,std::unique_ptr<DataFragmentHandler<DataItem>>> items;

			// the network this service is a part of
			com::Network& network;

			// the rank this service is running on
			com::rank_t rank;

		public:

			DataItemRegister(com::Network& network, com::rank_t rank)
				: network(network), rank(rank) {};

			DataItemRegister(const DataItemRegister&) = delete;
			DataItemRegister(DataItemRegister&&) = delete;

			// registers a new data item in the local registry
			void registerItem(const reference_type& ref, const shared_data_type& shared) {
				assert_not_pred1(contains,ref);
				items.emplace(ref,std::make_unique<DataFragmentHandler<DataItem>>(shared));
			}

			region_type getDataItemSize(const reference_type& ref) const {
				auto pos = items.find(ref);
				assert_true(pos != items.end());
				return pos->second->getDataItemSize();
			}

			void extract(const DataItemRegions& regions, DataItemMigrationData& res) override {
				regions.forAll<DataItem>([&](const DataItemRegion<DataItem>& cur){
					auto& ref = cur.getDataItemReference();
					auto& region = cur.getRegion();
					auto pos = items.find(ref);
					// one can not extract what one does not have
					if (pos == items.end()) return;
					// obtain data
					auto data = pos->second->extract(region);
					// if successful, add data to result
					if (bool(data)) res.add(ref,region,std::move(*data));
				});
			}

			allscale::utils::optional<allscale::utils::Archive> extract(const reference_type& ref, const region_type& region) {
				return get(ref).extract(region);
			}

			void insert(const reference_type& ref, allscale::utils::Archive& archive) {
				get(ref).insert(archive);
			}

			void import(const DataItemMigrationData& data) override {
				data.forEach<DataItem>([&](reference_type ref, const region_type& region, allscale::utils::Archive& archive){
					if (region.empty()) return;
					auto& fragment = get(ref);
					fragment.reserve(region);
					fragment.insert(archive);
				});
			}

			void takeOwnership(const DataItemMigrationData& data) override {
				data.forEach<DataItem>([&](reference_type ref, const region_type& region, allscale::utils::Archive& archive){
					if (region.empty()) return;
					auto& fragment = get(ref);
					assert_pred2(allscale::api::core::isSubRegion,region,fragment.getExclusiveRegion());
					fragment.insert(archive);
				});
			}

			void addExclusiveRegions(DataItemRegions& res) const override {
				for(const auto& cur : items) {
					auto region = region_type::intersect(cur.second->getExclusiveRegion(),cur.second->getDataItemSize());
					if (region.empty()) continue;
					res.add(cur.first,region);
				}
			}

			// obtains access to a selected fragment handler
			DataFragmentHandler<DataItem>& get(const reference_type& ref) {
				assert_pred1(contains,ref) << "Known Items: " << allscale::utils::join(",",items,[](std::ostream& out, const auto& entry) { out << entry.first; });
				return *items.find(ref)->second;
			}

		private:

			bool contains(const reference_type& ref) const {
				return items.find(ref) != items.end();
			}

		};

		// the maintained register of data item registers (type specific)
		std::unordered_map<std::type_index,std::unique_ptr<DataItemRegisterBase>> registers;

		// the network this service is a part of
		com::Network& network;

		// the rank this service is running on
		com::rank_t rank;

		// the list of all regions registered in the system
		DataItemRegions fullRegions;

		// a cache for resolved data locations
		mutable DataItemLocationCache locationCache;

		// -- statistics --

		std::atomic<uint64_t> allocate_call_count;
		std::atomic<uint64_t> release_call_count;
		std::atomic<uint64_t> locate_call_count;
		std::atomic<uint64_t> retrieve_call_count;
		std::atomic<uint64_t> acquire_call_count;

	public:

		DataItemManagerService(com::Node&);


		// a function to retrieve the local instance of this service
		static DataItemManagerService& getLocalService();

		/**
		 * Creates a new data item instance.
		 */
		template<typename DataItem, typename ... Args>
		DataItemReference<DataItem> create(Args&& ... args) {

			using shared_data_t = typename DataItem::shared_data_type;

			// create a new ID
			auto res = DataItemReference<DataItem>::getFresh();

			// create shared data object
			shared_data_t sharedInfo(std::forward<Args>(args)...);

			// register data item in network (also locally)
			com::Network::getNetwork().broadcastAndSync(&DataItemManagerService::registerDataItem<DataItem>)(res,sharedInfo);

			// return reference
			return res;
		}


		/**
		 * Obtains access to a data item maintained by this manager.
		 */
		template<typename DataItem>
		typename DataItem::facade_type get(const DataItemReference<DataItem>& ref) {
			return getRegister<DataItem>().get(ref).getDataItem();
		}

	private:

		friend class DataItemManager;

		/**
		 * Obtains access to the fragment handler managing the referenced data item.
		 */
		template<typename DataItem>
		DataFragmentHandler<DataItem>& getFragmentHandlerOf(const DataItemReference<DataItem>& ref) {
			return getRegister<DataItem>().get(ref);
		}

	public:

		/**
		 * Requests the allocation of the requested data item regions. Blocks until available.
		 */
		void allocate(const DataItemRequirements& reqs);

		/**
		 * Releases the specified data requirements.
		 * TODO: this could also be matched to an allocate call through some handler
		 */
		void release(const DataItemRequirements& reqs);

		/**
		 * Instructs the data item manager to refresh the given data regions locally.
		 */
		void retrieve(const DataItemRegions& regions);

		/**
		 * Acquires ownership on the stated requirements (where missing).
		 */
		void acquire(const DataItemRegions& regions);

		// --- protocol interface ---

		template<typename DataItem>
		void registerDataItem(DataItemReference<DataItem> ref, const typename DataItem::shared_data_type& shared_data) {
			// register shared data
			auto& reg = getRegister<DataItem>();
			reg.registerItem(ref,shared_data);

			// add new data item to full regions
			fullRegions.add(ref,reg.getDataItemSize(ref));

			// also inform index services
			notifyIndexOnCreation(ref);
		}

		/**
		 * Retrieves a serialized version of a data item stored at this locality.
		 */
		template<typename DataItem>
		allscale::utils::optional<allscale::utils::Archive> extract(DataItemReference<DataItem> ref, const typename DataItem::region_type& region) {
			return getRegister<DataItem>().extract(ref,region);
		}

		/**
		 * Retrieves a serialized version of the data stored at this locality.
		 */
		DataItemMigrationData extractRegions(const DataItemRegions& regions) const;

		/**
		 * Retrieves a serialized version of a data item stored at this locality.
		 */
		template<typename DataItem>
		void insert(DataItemReference<DataItem> ref, allscale::utils::Archive& archive) {
			return getRegister<DataItem>().insert(ref,archive);
		}

		// --- local interface ---

		template<typename DataItem>
		void resizeExclusive(const DataItemReference<DataItem>& ref, const typename DataItem::region_type& region) {
			getRegister<DataItem>().get(ref).resizeExclusive(region);
		}

		template<typename DataItem>
		void acquire(const DataItemReference<DataItem>& ref, const typename DataItem::region_type& region) {
			// channel through the type-erased interface
			DataItemRegions regions;
			regions.add(ref,region);
			acquire(regions);
		}

		void takeOwnership(const DataItemMigrationData& data);

		template<typename DataItem>
		typename DataItem::region_type getExclusiveRegion(const DataItemReference<DataItem>& ref) const {
			return getRegister<DataItem>().get(ref).getExclusiveRegion();
		}

		/**
		 * Obtains the full size of all data items registered in the system.
		 */
		const DataItemRegions& getFullRegions() const {
			return fullRegions;
		}

		/**
		 * Obtains the exclusive regions maintained locally.
		 */
		DataItemRegions getExclusiveRegions() const;

		/**
		 * Obtains a summary of the accesses statistics to this service instance.
		 */
		DataItemManagerStatisticEntry getLocalStatistic() const;

		/**
		 * Obtains a system wide summary of the access statistics of the data item manager service.
		 */
		DataItemManagerStatistic getStatistics() const;


	private:

		// obtains the register for a given data item type
		template<typename DataItem>
		DataItemRegister<DataItem>& getRegister() {
			auto pos = registers.find(typeid(DataItem));
			if (pos != registers.end()) {
				return *static_cast<DataItemRegister<DataItem>*>(pos->second.get());
			}
			auto instance = std::make_unique<DataItemRegister<DataItem>>(network,rank);
			auto& res = *instance;
			registers[typeid(DataItem)] = std::move(instance);
			return res;
		}

		template<typename DataItem>
		DataItemRegister<DataItem>& getRegister() const {
			auto pos = registers.find(typeid(DataItem));
			assert_true(pos != registers.end());
			return *static_cast<DataItemRegister<DataItem>*>(pos->second.get());
		}

	};


	/**
	 * The facade of the data item management.
	 */
	class DataItemManager {

		// a private constructor (just a utility class)
		DataItemManager() {}

	public:

		/**
		 * Creates a new data item instance.
		 */
		template<typename DataItem, typename ... Args>
		static DataItemReference<DataItem> create(Args&& ... args) {
			// forward this call to the local service
			return DataItemManagerService::getLocalService().create<DataItem>(std::forward<Args>(args)...);
		}

		/**
		 * Obtains access to a data item maintained by this manager.
		 */
		template<typename DataItem>
		static typename DataItem::facade_type get(const DataItemReference<DataItem>& ref) {

			// use cached fragment to obtain facade, if available
			if (ref.fragment) {
				return ref.fragment->mask();
			}

			// obtain reference to locally maintained fragment
			auto& fragment = DataItemManagerService::getLocalService().getFragmentHandlerOf(ref).getFragment();

			// cache lookup result
			ref.fragment = &fragment;

			// return mask
			return fragment.mask();
		}

	};


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale

// todo: merge those two files!
#include "allscale/runtime/data/data_item_index.h"
