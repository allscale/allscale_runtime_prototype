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

#include "allscale/api/core/data.h"

#include "allscale/runtime/com/node.h"
#include "allscale/runtime/com/network.h"
#include "allscale/runtime/com/hierarchy.h"

#include "allscale/runtime/data/data_item_reference.h"
#include "allscale/runtime/data/data_item_manager.h"
#include "allscale/runtime/data/data_item_region.h"
#include "allscale/runtime/data/data_item_requirement.h"

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

		// the address this index entry is assigned to
		com::HierarchyAddress myAddr;

		// true if this is the root entry of the index, false otherwise
		bool isRoot;

		// the data item managing
		data::DataItemReference<DataItem> ref;

	public:

		// create a new index entry
		DataItemIndexEntry(com::Network& net, const com::HierarchyAddress& addr, const data::DataItemReference<DataItem>& ref)
			: myAddr(addr), isRoot(myAddr == com::HierarchicalOverlayNetwork(net).getRootAddress()), ref(ref) {}


		// test whether the given requirement is covered by the current region of influence
		bool isCovered(const DataItemRequirement<DataItem>& req) const {
			assert_eq(ReadWrite, req.getMode());
			return allscale::api::core::isSubRegion(req.getRegion(),full);
		}

		bool isCoveredByLeft(const DataItemRequirement<DataItem>& req) const {
			assert_eq(ReadWrite, req.getMode());
			return allscale::api::core::isSubRegion(req.getRegion(),left);
		}

		bool isCoveredByRight(const DataItemRequirement<DataItem>& req) const {
			assert_eq(ReadWrite, req.getMode());
			return allscale::api::core::isSubRegion(req.getRegion(),right);
		}

		void add(const DataItemRegion<DataItem>& a) {
			full = allscale::api::core::merge(full,a.getRegion());

			// if this is not a leaf we are done
			if (!myAddr.isLeaf()) return;

			// grow corresponding fragment (keep them equally sized)
			com::Node::getLocalService<DataItemManagerService>().resizeExclusive(ref,full);
		}

		void addLeft(const DataItemRegion<DataItem>& a) {
			left = allscale::api::core::merge(left,a.getRegion());
			assert_true(check()) << "Invariant violated!";
		}

		void addRight(const DataItemRegion<DataItem>& a) {
			right = allscale::api::core::merge(right,a.getRegion());
			assert_true(check()) << "Invariant violated!";
		}

		void addTo(DataItemRegions& a) const {
			a.add(DataItemRegion<DataItem>(ref,full));
		}

		void addLeftTo(DataItemRegions& a) const {
			a.add(DataItemRegion<DataItem>(ref,left));
		}

		void addRightTo(DataItemRegions& a) const {
			a.add(DataItemRegion<DataItem>(ref,right));
		}

	private:

		// checks this entry for consistency
		bool check() const {
			// check all the invariants on the areas in this info entry
			return allscale::api::core::isSubRegion(left,full) &&
					allscale::api::core::isSubRegion(right,full) &&
				   region_type::intersect(left,right).empty();
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

		};

		// a type specific index entry
		template<typename DataItem>
		class Index : public IndexBase {

			// the network being a part of
			com::Network& net;

			// the index of the maintained data item entries
			std::map<DataItemReference<DataItem>,DataItemIndexEntry<DataItem>> indices;

			// the address of the node this index is installed on
			com::HierarchyAddress myAddress;

		public:

			Index(com::Network& net, const com::HierarchyAddress& addr) : net(net), myAddress(addr) {}

			DataItemIndexEntry<DataItem>& get(const DataItemReference<DataItem>& ref) {
				auto pos = indices.find(ref);
				if (pos != indices.end()) return pos->second;
				return indices.emplace(ref,DataItemIndexEntry<DataItem>(net,myAddress,ref)).first->second;
			}

			void addAvailable(DataItemRegions& res) const override {
				for(const auto& cur : indices) {
					cur.second.addTo(res);
				}
			}

			void addAvailableLeft(DataItemRegions& res) const override {
				for(const auto& cur : indices) {
					cur.second.addLeftTo(res);
				}
			}

			void addAvailableRight(DataItemRegions& res) const override {
				for(const auto& cur : indices) {
					cur.second.addRightTo(res);
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

		};

		// the network being based on
		com::Network& network;

		// the address this service is installed on
		com::HierarchyAddress myAddress;

		// the set of all maintained indices
		std::map<std::type_index,std::unique_ptr<IndexBase>> indices;

	public:

		// creates a new instance of this service running on the given address
		DataItemIndexService(com::Network& net, const com::HierarchyAddress& address)
			: network(net), myAddress(address) {}

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

		// computes the data regions available on this node
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

	private:

		// retrieves a type specific index maintained in this service
		template<typename DataItem>
		Index<DataItem>& getIndex() {
			auto& ptr = indices[typeid(DataItem)];
			if (!ptr) ptr = std::make_unique<Index<DataItem>>(network,myAddress);
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