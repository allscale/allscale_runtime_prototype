/*
 * The component aiding the management of data distributions on a
 * per-virtual node level in the overlay network.
 *
 *  Created on: Jul 26, 2018
 *      Author: herbert
 */

#pragma once

#include <map>
#include <memory>
#include <mutex>

#include "allscale/api/core/data.h"

#include "allscale/runtime/com/node.h"
#include "allscale/runtime/com/network.h"
#include "allscale/runtime/com/hierarchy.h"

#include "allscale/runtime/data/data_item_reference.h"
#include "allscale/runtime/data/data_item_manager.h"
#include "allscale/runtime/data/data_item_region.h"
#include "allscale/runtime/data/data_item_requirement.h"
#include "allscale/runtime/data/data_item_location.h"
#include "allscale/runtime/data/data_item_migration.h"

namespace allscale {
namespace runtime {
namespace data {

	/**
	 * The entity managing the distribution of a single data item
	 * on a virtual node in the hierarchical overlay network.
	 */
	template<typename DataItem>
	class DataItemIndexEntry {

		// Test that the passed data item type is valid.
		static_assert(allscale::api::core::is_data_item<DataItem>::value, "Can only be instantiated for data items!");

		// ------ some type definitions ------

		using shared_data_type = typename DataItem::shared_data_type;
		using region_type      = typename DataItem::region_type;
		using fragment_type    = typename DataItem::fragment_type;
		using facade_type      = typename DataItem::facade_type;


		// -- hierarchical network management information --

		// the area managed by this node on the corresponding level
		region_type full;

		// the area managed by the left child
		region_type left;

		// the area managed by the right child
		region_type right;

		// the network being a part of
		com::HierarchicalOverlayNetwork network;

		// the address this index entry is assigned to
		com::HierarchyAddress myAddr;

		// true if this is the root entry of the index, false otherwise
		bool isRoot;

		// the data item managing
		data::DataItemReference<DataItem> ref;

		// locks
		mutable std::mutex full_lock;
		mutable std::mutex left_lock;
		mutable std::mutex right_lock;

		using guard = std::lock_guard<std::mutex>;

	public:

		// create a new index entry
		DataItemIndexEntry(com::Network& net, const com::HierarchyAddress& addr, const data::DataItemReference<DataItem>& ref)
			: network(net), myAddr(addr), isRoot(myAddr == network.getRootAddress()), ref(ref) {}

		DataItemIndexEntry(const DataItemIndexEntry&) = delete;
		DataItemIndexEntry(DataItemIndexEntry&& other) = delete;

		// test whether the given requirement is covered by the current region of influence
		bool isCovered(const DataItemRequirement<DataItem>& req) const {
			assert_eq(ReadWrite, req.getMode());
			guard g(full_lock);
			return allscale::api::core::isSubRegion(req.getRegion(),full);
		}

		bool isCoveredByLeft(const DataItemRequirement<DataItem>& req) const {
			assert_eq(ReadWrite, req.getMode());
			guard g(left_lock);
			return allscale::api::core::isSubRegion(req.getRegion(),left);
		}

		bool isCoveredByRight(const DataItemRequirement<DataItem>& req) const {
			assert_eq(ReadWrite, req.getMode());
			guard g(right_lock);
			return allscale::api::core::isSubRegion(req.getRegion(),right);
		}

		void add(const DataItemRegion<DataItem>& a) {
			guard g(full_lock);
			full = allscale::api::core::merge(full,a.getRegion());

			// if this is not a leaf we are done
			if (!myAddr.isLeaf()) return;

			// grow corresponding fragment (keep them equally sized)
			com::Node::getLocalService<DataItemManagerService>().resizeExclusive(ref,full);
		}

		void addLeft(const DataItemRegion<DataItem>& a) {
			guard g(left_lock);
			left = allscale::api::core::merge(left,a.getRegion());
			check();
		}

		void addRight(const DataItemRegion<DataItem>& a) {
			guard g(right_lock);
			right = allscale::api::core::merge(right,a.getRegion());
			check();
		}


		void remove(const DataItemRegion<DataItem>& a) {
			guard g(full_lock);
			full = region_type::difference(full,a.getRegion());

			// if this is not a leaf we are done
			if (!myAddr.isLeaf()) return;

			// shrink corresponding fragment (keep them equally sized)
			com::Node::getLocalService<DataItemManagerService>().resizeExclusive(ref,full);
		}

		void removeLeft(const DataItemRegion<DataItem>& a) {
			guard g(left_lock);
			left = region_type::difference(left,a.getRegion());
			check();
		}

		void removeRight(const DataItemRegion<DataItem>& a) {
			guard g(right_lock);
			right = region_type::difference(right,a.getRegion());
			check();
		}


		void addTo(DataItemRegions& a) const {
			guard g(full_lock);
			a.add(DataItemRegion<DataItem>(ref,full));
		}

		void addLeftTo(DataItemRegions& a) const {
			guard g(left_lock);
			a.add(DataItemRegion<DataItem>(ref,left));
		}

		void addRightTo(DataItemRegions& a) const {
			guard g(right_lock);
			a.add(DataItemRegion<DataItem>(ref,right));
		}

		void addLocationInfo(const region_type& needed, DataItemLocationInfos& res) const {

			// this must only be called for leaf nodes
			assert_true(myAddr.isLeaf());

			// fill in local state ..

			// lock local state
			guard g(full_lock);

			// see whether there is something within the domain of this node
			auto match = region_type::intersect(needed,full);
			if (match.empty()) return; // does not seem so

			// we found something
			res.add(ref,match,myAddr.getRank());

		}

		void abandonOwnership(const region_type& needed, DataItemMigrationData& res) {

			// this must only be called for leaf nodes
			assert_true(myAddr.isLeaf());

			// fill in local data

			// lock local state
			guard g(full_lock);

			// see whether there is something within the domain of this node
			auto match = region_type::intersect(needed,full);
			if (match.empty()) return; // does not seem so

			// get the local data item manager
			auto& dim = com::Node::getLocalService<DataItemManagerService>();

			// we found something => extract it
			auto data = dim.extract(ref,match);
			res.add(ref,match,std::move(data));

			// remove ownership
			full = region_type::difference(full,match);

			// resize local data fragment (deletes local data)
			dim.resizeExclusive(ref,full);
		}

	private:

		// checks this entry for consistency
		void check() const {
			// check all invariants
			assert_pred2(allscale::api::core::isSubRegion,left,full);
			assert_pred2(allscale::api::core::isSubRegion,right,full);

			assert_decl(auto disjoint = [](const auto& a, const auto& b) {
				return region_type::intersect(a,b).empty();
			});
			assert_pred2(disjoint,left,right) << "Overlap: " << region_type::intersect(left,right);
		}

	};


	/**
	 * The hierarchical data item index service running on all virtual nodes.
	 */
	class DataItemIndexService {

		// the base of type-specific index entries
		class IndexBase {
		public:
			virtual ~IndexBase() {}

			virtual void addAvailable(DataItemRegions& res) const =0;

			virtual void addAvailableLeft(DataItemRegions& res) const =0;

			virtual void addAvailableRight(DataItemRegions& res) const =0;

			virtual void add(const DataItemRegions& regions) =0;

			virtual void addLeft(const DataItemRegions& regions) =0;

			virtual void addRight(const DataItemRegions& regions) =0;

			virtual void remove(const DataItemRegions& regions) =0;

			virtual void removeLeft(const DataItemRegions& regions) =0;

			virtual void removeRight(const DataItemRegions& regions) =0;

			virtual void addLocationInfo(const DataItemRegions& regions, DataItemLocationInfos& res) const =0;

			virtual void abandonOwnership(const DataItemRegions& regions, DataItemMigrationData& res) =0;
		};

		// a type specific index entry
		template<typename DataItem>
		class Index : public IndexBase {

			// the network being a part of
			com::Network& net;

			// the index of the maintained data item entries
			std::map<DataItemReference<DataItem>,std::unique_ptr<DataItemIndexEntry<DataItem>>> indices;

			// the address of the node this index is installed on
			com::HierarchyAddress myAddress;

		public:

			Index(com::Network& net, const com::HierarchyAddress& addr) : net(net), myAddress(addr) {}

			DataItemIndexEntry<DataItem>& get(const DataItemReference<DataItem>& ref) {
				auto pos = indices.find(ref);
				if (pos != indices.end()) return *pos->second;
				return *indices.emplace(ref,std::make_unique<DataItemIndexEntry<DataItem>>(net,myAddress,ref)).first->second;
			}

			void addAvailable(DataItemRegions& res) const override {
				for(const auto& cur : indices) {
					cur.second->addTo(res);
				}
			}

			void addAvailableLeft(DataItemRegions& res) const override {
				for(const auto& cur : indices) {
					cur.second->addLeftTo(res);
				}
			}

			void addAvailableRight(DataItemRegions& res) const override {
				for(const auto& cur : indices) {
					cur.second->addRightTo(res);
				}
			}

			void add(const DataItemRegions& regions) override {
				regions.forAll<DataItem>([&](const DataItemRegion<DataItem>& r){
					get(r.getDataItemReference()).add(r);
				});
			}

			void addLeft(const DataItemRegions& regions) override {
				regions.forAll<DataItem>([&](const DataItemRegion<DataItem>& r){
					get(r.getDataItemReference()).addLeft(r);
				});
			}

			void addRight(const DataItemRegions& regions) override {
				regions.forAll<DataItem>([&](const DataItemRegion<DataItem>& r){
					get(r.getDataItemReference()).addRight(r);
				});
			}

			void remove(const DataItemRegions& regions) override {
				regions.forAll<DataItem>([&](const DataItemRegion<DataItem>& r){
					get(r.getDataItemReference()).remove(r);
				});
			}

			void removeLeft(const DataItemRegions& regions) override {
				regions.forAll<DataItem>([&](const DataItemRegion<DataItem>& r){
					get(r.getDataItemReference()).removeLeft(r);
				});
			}

			void removeRight(const DataItemRegions& regions) override {
				regions.forAll<DataItem>([&](const DataItemRegion<DataItem>& r){
					get(r.getDataItemReference()).removeRight(r);
				});
			}

			virtual void addLocationInfo(const DataItemRegions& regions, DataItemLocationInfos& res) const {
				assert_true(myAddress.isLeaf());
				regions.forAll<DataItem>([&](const DataItemRegion<DataItem>& r){
					auto pos = indices.find(r.getDataItemReference());
					if (pos == indices.end()) return;
					pos->second->addLocationInfo(r.getRegion(),res);
				});
			}

			virtual void abandonOwnership(const DataItemRegions& regions, DataItemMigrationData& res) {
				assert_true(myAddress.isLeaf());
				regions.forAll<DataItem>([&](const DataItemRegion<DataItem>& r) {
					auto pos = indices.find(r.getDataItemReference());
					if (pos == indices.end()) return;
					pos->second->abandonOwnership(r.getRegion(),res);
				});
			}

		};

		// the network being based on
		com::HierarchicalOverlayNetwork network;

		// the address this service is installed on
		com::HierarchyAddress myAddress;

		// flag to indicate that this is the root service
		bool isRoot;

		// the set of all maintained indices
		std::map<std::type_index,std::unique_ptr<IndexBase>> indices;

		// a cache for location information
		mutable DataItemLocationCache locationCache;

	public:

		// creates a new instance of this service running on the given address
		DataItemIndexService(com::Network& net, const com::HierarchyAddress& address)
			: network(net), myAddress(address), isRoot(myAddress == network.getRootAddress()) {}

		template<typename DataItem>
		void registerDataItem(const DataItemReference<DataItem>& ref) {
			get(ref); // the index structure is created as a side-effect
		}

		template<typename DataItem>
		DataItemIndexEntry<DataItem>& get(const DataItemReference<DataItem>& ref) {
			return getIndex<DataItem>().get(ref);
		}

		// tests whether the given requirements are covered by this node
		bool coversWriteRequirements(const DataItemRequirements&);

		// computes the data regions available on this node (according to the ownership)
		DataItemRegions getAvailableData() const;

		// computes the data regions available in the left sub tree
		DataItemRegions getAvailableDataLeft() const;

		// computes the data regions available in the right sub tree
		DataItemRegions getAvailableDataRight() const;

		// computes the set of required regions to cover the given requirements
		DataItemRegions getMissingRegions(const DataItemRequirements&);

		// computes the set of required regions to schedule in the left sub-tree
		DataItemRegions getMissingRegionsLeft(const DataItemRequirements&);

		// computes the set of required regions to schedule in the right sub-tree
		DataItemRegions getMissingRegionsRight(const DataItemRequirements&);

		// adds the provided regions to this node
		void addRegions(const DataItemRegions&);

		// adds the provided regions to the coverage of the left sub tree
		void addRegionsLeft(const DataItemRegions&);

		// adds the provided regions to the coverage of the right sub tree
		void addRegionsRight(const DataItemRegions&);

		// removes the provided regions to this node
		void removeRegions(const DataItemRegions&);

		// removes the provided regions to the coverage of the left sub tree
		void removeRegionsLeft(const DataItemRegions&);

		// removes the provided regions to the coverage of the right sub tree
		void removeRegionsRight(const DataItemRegions&);

		/**
		 * Requests information on the location of data in the given regions.
		 */
		DataItemLocationInfos locate(const DataItemRegions& regions);

		/**
		 * Retrieves the current state and ownership for the specified regions.
		 */
		DataItemMigrationData acquire(const DataItemRegions& regions);

		/**
		 * Retrieves the current state of the specified regions and transferring ownership
		 * to the given call site.
		 */
		DataItemMigrationData acquireOwnership(com::HierarchyAddress caller, const DataItemRegions& regions);

	private:

		// retrieves a type specific index maintained in this service
		template<typename DataItem>
		Index<DataItem>& getIndex() {
			auto& ptr = indices[typeid(DataItem)];
			if (!ptr) ptr = std::make_unique<Index<DataItem>>(network.getNetwork(),myAddress);
			return static_cast<Index<DataItem>&>(*ptr);
		}

	};

	template<typename DataItem>
	void notifyIndexOnCreation(const DataItemReference<DataItem>& ref) {
		com::HierarchicalOverlayNetwork::forAllLocal<DataItemIndexService>([&](DataItemIndexService& s){
			s.registerDataItem(ref);
		});
	}


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
