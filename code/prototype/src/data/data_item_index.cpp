
#include "allscale/runtime/data/data_item_index.h"

#include <atomic>

namespace allscale {
namespace runtime {
namespace data {

	constexpr bool DEBUG = false;

	namespace {

		/**
		 * A utility to test whether a lock is locked.
		 */
		template<typename Lock>
		bool isLocked(Lock& lock) {
			auto res = lock.try_lock();
			if (res) lock.unlock();
			return !res;
		}

	}


	// tests whether the given requirements are covered by this node
	bool DataItemIndexService::covers(const DataItemRegions& region) const {
		// the requirements are covered if the needed regions are empty
		return getMissingRegions(region).empty();
	}

	bool DataItemIndexService::coversInternal(const DataItemRegions& region) const {
		// the requirements are covered if the needed regions are empty
		return getMissingRegionsInternal(region).empty();
	}

	DataItemRegions DataItemIndexService::getManagedUnallocatedRegion(const DataItemRegions& region) const {
		// quick response
		if (myAddress.isLeaf() || region.empty()) return {};

		// protect and forward to internal
		guard g(lock);
		return getManagedUnallocatedRegionInternal(region);
	}

	DataItemRegions DataItemIndexService::getManagedUnallocatedRegionInternal(const DataItemRegions& region) const {
		assert_true(isLocked(lock));

		// a leaf has no managed regions
		if (myAddress.isLeaf()) return {};

		// if the region is empty, there is nothing to do
		if (region.empty()) return {};

		// compute difference between left / right and full
		auto allocated = merge(getAvailableDataLeftInternal(),getAvailableDataRightInternal());
		auto unallocated = difference(getAvailableDataInternal(),allocated);
		return intersect(region,unallocated);
	}

	// computes the set of required regions to cover the given requirements
	DataItemRegions DataItemIndexService::getMissingRegions(const DataItemRegions& needed) const {
		// if nothing is needed => we are done
		if (needed.empty()) return {};

		guard g(lock);
		return getMissingRegionsInternal(needed);
	}

	DataItemRegions DataItemIndexService::getMissingRegionsInternal(const DataItemRegions& needed) const {
		assert_true(isLocked(lock));

		// if nothing is needed => we are done
		if (needed.empty()) return {};

		// see what is available
		auto avail = getAvailableDataInternal();

		// return the difference
		return difference(needed,avail);
	}

	// computes the set of required regions to schedule in the left sub-tree
	DataItemRegions DataItemIndexService::getMissingRegionsLeft(const DataItemRegions& needed) const {
		// if nothing is needed => we are done
		if (needed.empty()) return {};

		guard g(lock);
		return getMissingRegionsLeftInternal(needed);
	}

	DataItemRegions DataItemIndexService::getMissingRegionsLeftInternal(const DataItemRegions& needed) const {
		assert_true(isLocked(lock));

		// if nothing is needed => we are done
		if (needed.empty()) return {};

		// see what is available
		auto avail = getAvailableDataLeftInternal();

		// return the difference
		return difference(needed,avail);
	}

	// computes the set of required regions to schedule in the right sub-tree
	DataItemRegions DataItemIndexService::getMissingRegionsRight(const DataItemRegions& needed) const {
		assert_true(isLocked(lock));

		// if nothing is needed => we are done
		if (needed.empty()) return {};

		guard g(lock);
		return getMissingRegionsRightInternal(needed);
	}

	DataItemRegions DataItemIndexService::getMissingRegionsRightInternal(const DataItemRegions& needed) const {
		assert_true(isLocked(lock));

		// if nothing is needed => we are done
		if (needed.empty()) return {};

		// see what is available
		auto avail = getAvailableDataRightInternal();

		// return the difference
		return difference(needed,avail);
	}


	// adds the provided regions to this node
	void DataItemIndexService::addRegions(const DataItemRegions& regions) {
		// if empty => all done
		if (regions.empty()) return;

		// adding regions
		guard g(lock);
		addRegionsInternal(regions);
	}

	void DataItemIndexService::addRegionsInternal(const DataItemRegions& regions) {
		assert_true(isLocked(lock));

		// if empty => all done
		if (regions.empty()) return;

		// adding regions
		for(const auto& cur : indices) {
			cur.second->add(regions);
		}
	}

	// adds the provided regions to the coverage of the left sub tree
	void DataItemIndexService::addRegionsLeft(const DataItemRegions& regions) {
		// if empty => all done
		if (regions.empty()) return;

		// adding regions
		guard g(lock);
		addRegionsLeftInternal(regions);
	}

	void DataItemIndexService::addRegionsLeftInternal(const DataItemRegions& regions) {
		assert_true(isLocked(lock));

		// if empty => all done
		if (regions.empty()) return;

		// adding regions
		for(const auto& cur : indices) {
			cur.second->addLeft(regions);
		}
	}

	// adds the provided regions to the coverage of the right sub tree
	void DataItemIndexService::addRegionsRight(const DataItemRegions& regions) {
		// if empty => all done
		if (regions.empty()) return;

		// adding regions
		guard g(lock);
		addRegionsRightInternal(regions);
	}

	void DataItemIndexService::addRegionsRightInternal(const DataItemRegions& regions) {
		assert_true(isLocked(lock));

		// if empty => all done
		if (regions.empty()) return;

		// adding regions
		for(const auto& cur : indices) {
			cur.second->addRight(regions);
		}
	}


	// removes the provided regions to this node
	void DataItemIndexService::removeRegions(const DataItemRegions& regions) {
		// if empty => all done
		if (regions.empty()) return;

		// remove regions
		guard g(lock);
		removeRegionsInternal(regions);
	}

	void DataItemIndexService::removeRegionsInternal(const DataItemRegions& regions) {
		assert_true(isLocked(lock));

		// if empty => all done
		if (regions.empty()) return;

		// remove regions
		for(const auto& cur : indices) {
			cur.second->remove(regions);
		}
	}

	// removes the provided regions to the coverage of the left sub tree
	void DataItemIndexService::removeRegionsLeft(const DataItemRegions& regions) {
		// if empty => all done
		if (regions.empty()) return;

		// remove regions
		guard g(lock);
		removeRegionsLeftInternal(regions);
	}

	void DataItemIndexService::removeRegionsLeftInternal(const DataItemRegions& regions) {
		assert_true(isLocked(lock));

		// if empty => all done
		if (regions.empty()) return;

		// remove regions
		for(const auto& cur : indices) {
			cur.second->removeLeft(regions);
		}
	}

	// removes the provided regions to the coverage of the right sub tree
	void DataItemIndexService::removeRegionsRight(const DataItemRegions& regions) {
		// if empty => all done
		if (regions.empty()) return;

		// adding regions
		guard g(lock);
		removeRegionsRightInternal(regions);
	}

	void DataItemIndexService::removeRegionsRightInternal(const DataItemRegions& regions) {
		assert_true(isLocked(lock));

		// if empty => all done
		if (regions.empty()) return;

		// adding regions
		for(const auto& cur : indices) {
			cur.second->removeRight(regions);
		}
	}


	// computes the data regions available on this node
	DataItemRegions DataItemIndexService::getAvailableData() const {
		guard g(lock);
		return getAvailableDataInternal();
	}

	DataItemRegions DataItemIndexService::getAvailableDataInternal() const {
		assert_true(isLocked(lock));

		DataItemRegions res;
		for(const auto& cur : indices) {
			cur.second->addAvailable(res);
		}
		return res;
	}

	// computes the data regions available in the left sub tree
	DataItemRegions DataItemIndexService::getAvailableDataLeft() const {
		guard g(lock);
		return getAvailableDataLeftInternal();
	}

	DataItemRegions DataItemIndexService::getAvailableDataLeftInternal() const {
		assert_true(isLocked(lock));

		DataItemRegions res;
		for(const auto& cur : indices) {
			cur.second->addAvailableLeft(res);
		}
		return res;
	}

	// computes the data regions available in the right sub tree
	DataItemRegions DataItemIndexService::getAvailableDataRight() const {
		guard g(lock);
		return getAvailableDataRightInternal();
	}

	DataItemRegions DataItemIndexService::getAvailableDataRightInternal() const {
		assert_true(isLocked(lock));

		DataItemRegions res;
		for(const auto& cur : indices) {
			cur.second->addAvailableRight(res);
		}
		return res;
	}


	void DataItemIndexService::addAllowanceLocal(const DataItemRegions& regions) {

		// check that there is something to do
		if (regions.empty()) return;

		// just add to full region info
		guard g(lock);
		addRegionsInternal(regions);
	}

	DataItemRegions DataItemIndexService::addAllowanceLeft(const DataItemRegions& full, const DataItemRegions& required) {
		// only supported for inner nodes
		assert_false(myAddress.isLeaf());

		// check whether there is anything to do
		if (full.empty() && required.empty()) return {};

		// lock the local state
		guard g(lock);

		// extend the local ownership
		addRegionsInternal(full);

		// compute missing left
		auto missing = getMissingRegionsLeftInternal(required);

		// if there is nothing missing, we are done
		if (missing.empty()) return {};

		// cut down to what this process is allowed
		missing = difference(intersect(missing,getAvailableDataInternal()),getAvailableDataRightInternal());

		// add missing to left
		addRegionsLeftInternal(missing);

		// inform the user about what has been added
		return missing;
	}

	DataItemRegions DataItemIndexService::addAllowanceRight(const DataItemRegions& full, const DataItemRegions& required) {
		// only supported for inner nodes
		assert_false(myAddress.isLeaf());

		// check whether there is anything to do
		if (full.empty() && required.empty()) return {};

		// lock the local state
		guard g(lock);

		// extend the local ownership
		addRegionsInternal(full);

		// compute missing left
		auto missing = getMissingRegionsRightInternal(required);

		// if there is nothing missing, we are done
		if (missing.empty()) return {};

		// cut down to what this process is allowed
		missing = difference(intersect(missing,getAvailableDataInternal()),getAvailableDataLeftInternal());

		// add missing to right
		addRegionsRightInternal(missing);

		// inform the user about what has been added
		return missing;
	}



	DataItemLocationInfos DataItemIndexService::locate(const DataItemRegions& regions, int id) {

		static std::atomic<int> counter(0 + (1<<20) * myAddress.getRank());

		if (id < 0) {
			id = counter++;
			if (DEBUG) std::cout << myAddress << ": start locating procedure " << id << " - lock state: " << isLocked(lock) << " ..\n";
		} else {
			if (DEBUG) std::cout << myAddress << ": processing locating procedure " << id << " - lock state: " << isLocked(lock) << " ..\n";
		}



		// see whether there is something to do at all
		if (regions.empty()) return {};

		// Phase 1: find node owning all required data

		if (!isRoot && !isSubRegion(regions,getAvailableData())) {

			if (DEBUG) std::cout << myAddress << ": Resolve location " << id << " - forward to " << myAddress.getParent() << " - " << isLocked(lock) << "\n";

			// forward call to parent
			auto res = network.getRemoteProcedure(myAddress.getParent(),&DataItemIndexService::locate)(regions,id).get();

			if (DEBUG) std::cout << myAddress << ": Resolve location " << id << " - retrieved from " << myAddress.getParent() << "\n";
			return res;
		}

		// Phase 2 + 3: this is the owner of everything required => resolve details and return result
		return resolveLocations(regions,id);
	}

	DataItemLocationInfos DataItemIndexService::resolveLocations(const DataItemRegions& regions, int id) {

		if (DEBUG) std::cout << myAddress << ": Resolve location " << id << " - start - " << isLocked(lock) << "\n";

		// lock this node (to avoid concurrent modifications)
		guard g(lock);

		if (DEBUG) std::cout << myAddress << ": Resolve location " << id << " - locked\n";

		// make sure this one is responsible for the requested region
		if (!isRoot) assert_pred2(isSubRegion,regions,getAvailableDataInternal());


		DataItemLocationInfos res;


		// -- add data from leaf node --

		if (myAddress.isLeaf()) {

			// add local information
			for(const auto& cur : indices) {
				cur.second->addLocationInfo(regions,res);
			}

			// make sure everything has been located
			assert_eq(regions,res.getCoveredRegions())
				<< "Available: " << getAvailableDataInternal() << "\n"
				<< "Located: " << res << "\n"
				<< "Missing: " << difference(regions,res.getCoveredRegions());

			if (DEBUG) std::cout << myAddress << ": Resolved location " << id << " - leaf done\n";

			// done
			return res;

		}

		// -- process inner nodes --

		auto remaining = regions;

		// for inner nodes, query sub-trees
		{
			// start with left
			auto part = intersect(remaining,getAvailableDataLeftInternal());
			if (!part.empty()) {

				if (DEBUG) std::cout << myAddress << ": Resolve location " << id << " - asking left ..\n";

				// query sub-tree
				auto subInfo = network.getRemoteProcedure(myAddress.getLeftChild(),&DataItemIndexService::resolveLocations)(part,id).get();

				if (DEBUG) std::cout << myAddress << ": Resolve location " << id << " - left done\n";

				// consistency check
				assert_eq(part,subInfo.getCoveredRegions());

				// add to result
				res.addAll(subInfo);

				// reduce remaining
				remaining = difference(remaining,part);

			}

		}

		// and if necessary also the right sub-tree
		if (!remaining.empty()) {

			auto part = intersect(remaining,getAvailableDataRightInternal());
			if (!part.empty()) {

				if (DEBUG) std::cout << myAddress << ": Resolve location " << id << " - asking right ..\n";

				// query sub-tree
				auto subInfo = network.getRemoteProcedure(myAddress.getRightChild(),&DataItemIndexService::resolveLocations)(part,id).get();

				if (DEBUG) std::cout << myAddress << ": Resolve location " << id << " - right done\n";

				// consistency check
				assert_eq(part,subInfo.getCoveredRegions());

				// add to result
				res.addAll(subInfo);

				// reduce remaining
				remaining = difference(remaining,part);

			}
		}

		// check that all data could be retrieved
		if (!isRoot) {
			assert_true(remaining.empty());
			assert_eq(regions,res.getCoveredRegions());
		}

		if (DEBUG) std::cout << myAddress << ": Resolve location - inner done\n";

		// done
		return res;
	}


	void DataItemIndexService::acquire(const DataItemRegions& regions) {

//		std::cout << "Acquire called ..\n";

		// this entry point is only to be called on the leaf level
		assert_true(myAddress.isLeaf());

		// compute the missing regions
		auto missing = difference(regions,getAvailableData());

		// if there is nothing missing, there is nothing to do
		if (missing.empty()) return;

		// if this is the root node, there is no place to retrieve data from
		if (isRoot) {
			// client code is requesting unallocated data
			assert_pred2(isDisjoint,getAvailableData(),regions);

			// create default-initialized data through allowances
			addRegions(regions);

			// done
			return;
		}

		// send request to parent
		auto res = network.getRemoteProcedure(myAddress.getParent(), &DataItemIndexService::acquireOwnershipFor)(missing,myAddress).get();

		// this node should be locked now
		assert_true(isLocked(lock));

		// make sure the requested data is complete
		assert_eq(regions,res.getCoveredRegions());

		// register ownership
		addRegionsInternal(regions);

		// insert data locally
		com::Node::getLocalService<DataItemManagerService>().takeOwnership(res);

		// free the local lock
		lock.unlock();

	}

	DataItemMigrationData DataItemIndexService::acquireOwnershipFor(const DataItemRegions& regions, com::HierarchyAddress child) {

		// this must not be called for a leaf node
		assert_false(myAddress.isLeaf());

		// get left and right child addresses
		auto leftChild = myAddress.getLeftChild();
		auto rightChild = myAddress.getRightChild();

		// make sure the given child is really a child of this node
		assert_true(child == leftChild || child == rightChild)
			<< "Node " << child << " is not a child of " << myAddress;

		// Phase 1: walk toward node covering all required regions
		lock.lock();	// lock this node for the next test
		if (!isRoot && !isSubRegion(regions,getAvailableDataInternal())) {

			// to avoid dead locks, abandon local lock while descending in tree; decent will aquire it again
			lock.unlock();

			// forward to parent
			auto data = network.getRemoteProcedure(myAddress.getParent(), &DataItemIndexService::acquireOwnershipFor)(regions,myAddress).get();

			// the parent should have requested the lock
			assert_true(isLocked(lock));

			// this should cover all requested data
			assert_eq(regions,data.getCoveredRegions());

			// -- Phase 4: forward data to calling child --

			// update ownership data
			addRegionsInternal(regions);

			if (child == leftChild) {
				removeRegionsRightInternal(regions);
				addRegionsLeftInternal(regions);
			} else {
				removeRegionsLeftInternal(regions);
				addRegionsRightInternal(regions);
			}

			// lock child node
			network.getRemoteProcedure(child, &DataItemIndexService::lockForOwnershipTransfer)().get();

			// free the local lock
			lock.unlock();

			// forward data and ownership
			return data;
		}

		// Phase 2: this is the top-node, lock it
		// we preserve the acquired lock from above

		// Phase 3: recursively collect transfer data
		auto res = collectOwnershipFromChildren(regions);
		assert_eq(regions,res.getCoveredRegions());

		// Phase 4: transfer ownership to calling child

		// update region information
		if (child == rightChild) {
			removeRegionsLeftInternal(regions);
			addRegionsRightInternal(regions);
		} else {
			removeRegionsRightInternal(regions);
			addRegionsLeftInternal(regions);
		}

		// make sure new management knowledge is consistent (for the requested part)
		assert_eq(
				intersect(getAvailableDataInternal(),regions),
				intersect(merge(getAvailableDataLeftInternal(),getAvailableDataRightInternal()),regions)
			) << "Node:    " << myAddress << "\n"
			  << "Regions: " << regions << "\n"
			  << "Available: " << getAvailableDataInternal() << "\n"
			  << "Left:      " << getAvailableDataLeftInternal() << "\n"
			  << "Right:     " << getAvailableDataRightInternal() << "\n";

		// lock child node for ownership transfer
		network.getRemoteProcedure(child, &DataItemIndexService::lockForOwnershipTransfer)().get();

		// give up the local lock
		lock.unlock();

		// return data migration information, thereby abandoning the local lock
		return res;
	}

	/**
	 * A utility required for realizing the locking scheme of phase 4.
	 */
	bool DataItemIndexService::lockForOwnershipTransfer() {
		// simply take the lock
		lock.lock();
		return true;	// the return value is needed to not make this a fire-and-forget remote procedure call
	}

	/**
	 * This function implements the recursive bottom-up collection of region data (phase 3) and
	 * removes ownership by doing so.
	 */
	DataItemMigrationData DataItemIndexService::abandonOwnership(const DataItemRegions& regions) {

		// lock this node (save since we are walking top-down)
		guard g(lock);

		// make sure a owned part of the tree is requested
		assert_pred2(isSubRegion,regions,getAvailableDataInternal())
			<< "Local address: " << myAddress << "\n";


		// -- handle leafs  --

		if (myAddress.isLeaf()) {

			// initialize results
			DataItemMigrationData res;

			// add local information
			for(const auto& cur : indices) {
				cur.second->abandonOwnership(regions,res);
			}

			// test that retrieved data is complete
			assert_eq(regions,res.getCoveredRegions());

			// ownership should be reduced impicitly
			assert_pred2(isDisjoint,regions,getAvailableDataInternal())
				<< "Extracted: " << res.getCoveredRegions() << "\n"
				<< "Remaining: " << getAvailableDataInternal() << "\n";


			// done
			return res;
		}

		// -- inner nodes --
		auto res = collectOwnershipFromChildren(regions);

		// remove ownership of this node
		removeRegionsInternal(regions);

		// done
		return res;
	}

	DataItemMigrationData DataItemIndexService::collectOwnershipFromChildren(const DataItemRegions& regions) {

		// make sure there accesses is exclusive
		assert_true(isLocked(lock));

		// make sure the requested region is owned by this node
		assert_pred2(isSubRegion,regions,getAvailableDataInternal());

		auto missing = regions;
		DataItemMigrationData res;

		// left child
		{
			auto part = intersect(missing,getAvailableDataLeftInternal());
			if (!part.empty()) {

				// retract ownership and retrieve data
				auto data = network.getRemoteProcedure(myAddress.getLeftChild(), &DataItemIndexService::abandonOwnership)(part).get();

				// make sure it is what was expected
				assert_eq(part,data.getCoveredRegions());

				// update management information
				removeRegionsLeftInternal(part);

				// update remaining missing area
				missing = difference(missing,part);

				// add to result
				res.addAll(data);
			}
		}

		// right child
		if (!missing.empty()) {
			auto part = intersect(missing,getAvailableDataRightInternal());
			if (!part.empty()) {

				// retract ownership and retrieve data
				auto data = network.getRemoteProcedure(myAddress.getRightChild(), &DataItemIndexService::abandonOwnership)(part).get();

				// make sure it is what was expected
				assert_eq(part,data.getCoveredRegions());

				// update management information
				removeRegionsRightInternal(part);

				// update remaining missing area
				missing = difference(missing,part);

				// add to result
				res.addAll(data);
			}
		}

		// if there is something left, it is managed by this node, but it has not been allocated yet
		if (!missing.empty()) {

			// make sure this is correct, the data is indeed missing
			assert_pred2(isSubRegion,missing,getAvailableDataInternal());

			assert_true(intersect(getAvailableDataLeftInternal(),missing).empty())
				<< "Available Left: " << getAvailableDataLeftInternal() << "\n"
				<< "Missing Region: " << missing << "\n"
				<< "Intersected:    " << intersect(getAvailableDataLeftInternal(),missing) << "\n";

			assert_true(intersect(getAvailableDataRightInternal(),missing).empty())
				<< "Available Left: " << getAvailableDataRightInternal() << "\n"
				<< "Missing Region: " << missing << "\n"
				<< "Intersected:    " << intersect(getAvailableDataRightInternal(),missing) << "\n";

			// add the missing data to the result as something that can be default initialized
			res.addDefaultInitRegions(missing);

		}

		// done
		return res;
	}


	void DataItemIndexService::dumpState(const std::string& prefix) const {
		std::cout << prefix << "DataItemIndexService@" << myAddress << " - lock state: " << isLocked(lock) << "\n";
	}

} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
