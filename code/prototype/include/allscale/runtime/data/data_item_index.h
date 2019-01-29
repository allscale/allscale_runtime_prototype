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

#include "allscale/utils/fibers.h"
#include "allscale/utils/fiber/read_write_lock.h"

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
	 * The hierarchical data item index service running on all virtual nodes.
	 */
	class DataItemIndexService {

		// the network being based on
		com::HierarchicalOverlayNetwork network;

		// the address this service is installed on
		com::HierarchyAddress myAddress;

		// flag to indicate that this is the root service
		bool isRoot;

		// a summary of the regions managed by this hierarchical node
		DataItemRegions managedRegions;

		// a summary of the regions managed by the nodes left child
		DataItemRegions managedRegionsLeft;

		// a summary of the regions managed by the nodes right child
		DataItemRegions managedRegionsRight;

		// a cache for location information
		mutable DataItemLocationCache locationCache;

		// a lock to synchronize transfers on this instance
		mutable allscale::utils::fiber::ReadWriteLock lock;

		// the guard type to utilize
		using read_guard = allscale::utils::fiber::ReadGuard;
		using write_guard = allscale::utils::fiber::WriteGuard;

	public:

		// creates a new instance of this service running on the given address
		DataItemIndexService(com::Network& net, const com::HierarchyAddress& address)
			: network(net), myAddress(address), isRoot(myAddress == network.getRootAddress()) {}

		// tests whether the given regions are covered by this node
		bool covers(const DataItemRegions&) const;

		// tests whether the given region contains data managed by this node, yet not allocated to left or right
		DataItemRegions getManagedUnallocatedRegion(const DataItemRegions&) const;

		// computes the data regions available on this node (according to the ownership)
		DataItemRegions getAvailableData() const;

		// computes the data regions available in the left sub tree
		DataItemRegions getAvailableDataLeft() const;

		// computes the data regions available in the right sub tree
		DataItemRegions getAvailableDataRight() const;

		// computes the set of required regions to cover the given requirements
		DataItemRegions getMissingRegions(const DataItemRegions&) const;

		// computes the set of required regions to schedule in the left sub-tree
		DataItemRegions getMissingRegionsLeft(const DataItemRegions&) const;

		// computes the set of required regions to schedule in the right sub-tree
		DataItemRegions getMissingRegionsRight(const DataItemRegions&) const;

	private:

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

	public:

		// -- data item allowances handling --

		/**
		 * Register an additional local allowance (only supported for leaf nodes).
		 */
		void addAllowanceLocal(const DataItemRegions&);

		/**
		 * Register an additional allowance for the left sub-tree.
		 *
		 * @return the regions added to left
		 */
		DataItemRegions addAllowanceLeft(const DataItemRegions& full, const DataItemRegions& required);

		/**
		 * Register an additional allowance for the right sub-tree.
		 *
		 * @return the regions added to right
		 */
		DataItemRegions addAllowanceRight(const DataItemRegions& full, const DataItemRegions& required);


		// -- data item location protocol --

		/**
		 * The locate protocol has 3 phases:
		 *  - phase 1: walk from origin to node T in hierarchy owning everything that is requested (no locking along path)
		 *  - phase 2: from node T, lookup location recursively depth-first, locking nodes along the active paths
		 *  - phase 3: return result to origin
		 */

		/**
		 * Requests information on the location of data in the given regions.
		 * This is the entry point, as well as the implementation of phase 1 and 3.
		 */
		DataItemLocationInfos locate(const DataItemRegions& regions, int id = -1);

		/**
		 * This recursively resolves the location of the given data regions. Implements
		 * phase 2 of the lookup procedure.
		 */
		DataItemLocationInfos resolveLocations(const DataItemRegions& regions, int id);



		// -- data item acquire protocol --

		/**
		 * The acquire protocol has 4 phases:
		 *   - phase 1: walk from target to node T in hierarchy owning everything that is required (no locking along path)
		 *   - phase 2: lock node T
		 *   - phase 3: from node T, acquire required data recursively, lock in depth-first search manner
		 *   - phase 4: gradually move ownership from T to target, locking step-by-step
		 */

		/**
		 * Retrieves the current state and ownership for the specified regions.
		 * This is the entry point for e.g. the data item manager.
		 */
		void acquire(const DataItemRegions& regions);

		/**
		 * This function handles phase 1,2, and 4.
		 */
		DataItemMigrationData acquireOwnershipFor(const DataItemRegions& regions,com::HierarchyAddress child);

		/**
		 * A utility required for realizing the locking scheme of phase 4.
		 *
		 * @return always true; the return value is required to not make it a fire-and-forget asynchronous remote call
		 */
		bool lockForOwnershipTransfer();

		/**
		 * This function implements the recursive bottom-up collection of region data (phase 3) and
		 * removes ownership by doing so.
		 */
		DataItemMigrationData abandonOwnership(const DataItemRegions&);

	private:

		// a internal utility function implementing common functionality of acquireOwnershipFor and abandonOwnership
		DataItemMigrationData collectOwnershipFromChildren(const DataItemRegions&);

		// -- internal, non-lock protected members --

		bool coversInternal(const DataItemRegions&) const;

		DataItemRegions getManagedUnallocatedRegionInternal(const DataItemRegions&) const;

		const DataItemRegions& getAvailableDataInternal() const;

		const DataItemRegions& getAvailableDataLeftInternal() const;

		const DataItemRegions& getAvailableDataRightInternal() const;

		DataItemRegions getMissingRegionsInternal(const DataItemRegions&) const;

		DataItemRegions getMissingRegionsLeftInternal(const DataItemRegions&) const;

		DataItemRegions getMissingRegionsRightInternal(const DataItemRegions&) const;

		void addRegionsInternal(const DataItemRegions&);

		void addRegionsLeftInternal(const DataItemRegions&);

		void addRegionsRightInternal(const DataItemRegions&);

		void removeRegionsInternal(const DataItemRegions&);

		void removeRegionsLeftInternal(const DataItemRegions&);

		void removeRegionsRightInternal(const DataItemRegions&);

	public:

		void dumpState(const std::string& prefix) const;

	};


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
