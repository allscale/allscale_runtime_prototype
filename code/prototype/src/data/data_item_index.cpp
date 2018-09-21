
#include "allscale/runtime/data/data_item_index.h"

namespace allscale {
namespace runtime {
namespace data {


	// tests whether the given requirements are covered by this node
	bool DataItemIndexService::covers(const DataItemRegions& region) const {
		// the requirements are covered if the needed regions are empty
		return getMissingRegions(region).empty();
	}

	DataItemRegions DataItemIndexService::getManagedUnallocatedRegion(const DataItemRegions& region) const {

		// a leaf has no managed regions
		if (myAddress.isLeaf()) return {};

		// if the region is empty, there is nothing to do
		if (region.empty()) return {};

		// compute difference between left / right and full
		guard g(lock);
		auto allocated = merge(getAvailableDataLeft(),getAvailableDataRight());
		auto unallocated = difference(getAvailableData(),allocated);
		return intersect(region,unallocated);
	}

	// computes the set of required regions to cover the given requirements
	DataItemRegions DataItemIndexService::getMissingRegions(const DataItemRegions& needed) const {

		// if nothing is needed => we are done
		if (needed.empty()) return {};

		// see what is available
		auto avail = getAvailableData();

		// return the difference
		return difference(needed,avail);
	}

	// computes the set of required regions to schedule in the left sub-tree
	DataItemRegions DataItemIndexService::getMissingRegionsLeft(const DataItemRegions& needed) const {

		// if nothing is needed => we are done
		if (needed.empty()) return {};

		// see what is available
		auto avail = getAvailableDataLeft();

		// return the difference
		return difference(needed,avail);
	}

	// computes the set of required regions to schedule in the right sub-tree
	DataItemRegions DataItemIndexService::getMissingRegionsRight(const DataItemRegions& needed) const {

		// if nothing is needed => we are done
		if (needed.empty()) return {};

		// see what is available
		auto avail = getAvailableDataRight();

		// return the difference
		return difference(needed,avail);
	}


	// adds the provided regions to this node
	void DataItemIndexService::addRegions(const DataItemRegions& regions) {

		// if empty => all done
		if (regions.empty()) return;

		// adding regions
		guard g(lock);
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
		for(const auto& cur : indices) {
			cur.second->addRight(regions);
		}
	}


	// removes the provided regions to this node
	void DataItemIndexService::removeRegions(const DataItemRegions& regions) {

		// if empty => all done
		if (regions.empty()) return;

		// adding regions
		guard g(lock);
		for(const auto& cur : indices) {
			cur.second->remove(regions);
		}
	}

	// removes the provided regions to the coverage of the left sub tree
	void DataItemIndexService::removeRegionsLeft(const DataItemRegions& regions) {
		// if empty => all done
		if (regions.empty()) return;

		// adding regions
		guard g(lock);
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
		for(const auto& cur : indices) {
			cur.second->removeRight(regions);
		}
	}


	// computes the data regions available on this node
	DataItemRegions DataItemIndexService::getAvailableData() const {
		DataItemRegions res;
		guard g(lock);
		for(const auto& cur : indices) {
			cur.second->addAvailable(res);
		}
		return res;
	}

	// computes the data regions available in the left sub tree
	DataItemRegions DataItemIndexService::getAvailableDataLeft() const {
		DataItemRegions res;
		guard g(lock);
		for(const auto& cur : indices) {
			cur.second->addAvailableLeft(res);
		}
		return res;
	}

	// computes the data regions available in the right sub tree
	DataItemRegions DataItemIndexService::getAvailableDataRight() const {
		DataItemRegions res;
		guard g(lock);
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
		addRegions(regions);
	}

	DataItemRegions DataItemIndexService::addAllowanceLeft(const DataItemRegions& full, const DataItemRegions& required) {
		// only supported for inner nodes
		assert_false(myAddress.isLeaf());

		// check whether there is anything to do
		if (full.empty() && required.empty()) return {};

		// lock the local state
		guard g(lock);

		// extend the local ownership
		addRegions(full);

		// compute missing left
		auto missing = getMissingRegionsLeft(required);

		// if there is nothing missing, we are done
		if (missing.empty()) return {};

		// cut down to what this process is allowed
		missing = difference(intersect(missing,getAvailableData()),getAvailableDataRight());

		// add missing to left
		addRegionsLeft(missing);

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
		addRegions(full);

		// compute missing left
		auto missing = getMissingRegionsRight(required);

		// if there is nothing missing, we are done
		if (missing.empty()) return {};

		// cut down to what this process is allowed
		missing = difference(intersect(missing,getAvailableData()),getAvailableDataLeft());

		// add missing to right
		addRegionsRight(missing);

		// inform the user about what has been added
		return missing;
	}



	DataItemLocationInfos DataItemIndexService::locate(const DataItemRegions& regions) {

		// see whether there is something to do at all
		if (regions.empty()) return {};

		// Phase 1: find node owning all required data

		if (!isRoot && !isSubRegion(regions,getAvailableData())) {
			// forward call to parent
			return network.getRemoteProcedure(myAddress.getParent(),&DataItemIndexService::locate)(regions);
		}

		// Phase 2 + 3: this is the owner of everything required => resolve details and return result
		return resolveLocations(regions);
	}

	DataItemLocationInfos DataItemIndexService::resolveLocations(const DataItemRegions& regions) {

		// lock this node (to avoid concurrent modifications)
		guard g(lock);

		// make sure this one is responsible for the requested region
		if (!isRoot) assert_pred2(isSubRegion,regions,getAvailableData());


		DataItemLocationInfos res;


		// -- add data from leaf node --

		if (myAddress.isLeaf()) {

			// add local information
			for(const auto& cur : indices) {
				cur.second->addLocationInfo(regions,res);
			}

			// make sure everything has been located
			assert_eq(regions,res.getCoveredRegions())
				<< "Available: " << getAvailableData() << "\n"
				<< "Located: " << res << "\n"
				<< "Missing: " << difference(regions,res.getCoveredRegions());

			// done
			return res;

		}

		// -- process inner nodes --

		auto remaining = regions;

		// for inner nodes, query sub-trees
		{
			// start with left
			auto part = intersect(remaining,getAvailableDataLeft());
			if (!part.empty()) {

				// query sub-tree
				auto subInfo = network.getRemoteProcedure(myAddress.getLeftChild(),&DataItemIndexService::resolveLocations)(part);

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

			auto part = intersect(remaining,getAvailableDataRight());
			if (!part.empty()) {

				// query sub-tree
				auto subInfo = network.getRemoteProcedure(myAddress.getRightChild(),&DataItemIndexService::resolveLocations)(part);

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

		// done
		return res;
	}



	namespace {

		DataItemMigrationData createMigrationAllowance(const DataItemRegions&) {

			// if this assertion is failing, investigate cause, and if necessary create return allowances for requested regions
			assert_fail() << "This should not be necessary, all data should be somewhere!";

			assert_not_implemented();
			return {};
		}

	}



	DataItemMigrationData DataItemIndexService::acquire(const DataItemRegions& regions) {

		// this entry point is only to be called on the leaf level
		assert_true(myAddress.isLeaf());

		// if this is the root node, there is no place to retrieve data from
		if (isRoot) {
			// client code is requesting unallocated data
			assert_pred2(isDisjoint,getAvailableData(),regions);
			// create default-initialized data through allowances
			return createMigrationAllowance(regions);
		}

		// compute the missing regions
		auto missing = difference(regions,getAvailableData());

		// if there is nothing missing, there is nothing to do
		if (missing.empty()) return {};

		// send request to parent
		auto res = network.getRemoteProcedure(myAddress.getParent(), &DataItemIndexService::acquireOwnershipFor)(missing,myAddress);

		// this node should be locked now
//		assert_false(lock.owns_lock());

		// make sure the requested data is complete
		assert_eq(regions,res.getCoveredRegions());

		// register ownership
		addRegions(regions);

		// free the local lock
		lock.unlock();

		// done
		return res;
	}

	DataItemMigrationData DataItemIndexService::acquireOwnershipFor(const DataItemRegions& regions, com::HierarchyAddress child) {

		// this must not be called for a leaf node
		assert_false(myAddress.isLeaf());

		// get left and right child addresses
		auto leftChild = myAddress.getLeftChild();
		auto rightChild = myAddress.getRightChild();

		// make sure local management knowledge is consistent (for the requested part)
		{
			guard g(lock);
			assert_eq(
					intersect(getAvailableData(),regions),
					merge(intersect(getAvailableDataLeft(),regions),intersect(getAvailableDataRight(),regions))
				) << "Node:    " << myAddress << "\n"
				  << "Regions: " << regions << "\n"
				  << "Available: " << getAvailableData() << "\n"
				  << "Left:      " << getAvailableDataLeft() << "\n"
				  << "Right:     " << getAvailableDataRight() << "\n"
				  << "Merged:    " << merge(getAvailableDataLeft(),getAvailableDataRight()) << "\n"
				  << " -- intersected --\n"
				  << "Available: " << intersect(getAvailableData(),regions) << "\n"
				  << "Left:      " << intersect(getAvailableDataLeft(),regions) << "\n"
				  << "Right:     " << intersect(getAvailableDataRight(),regions) << "\n";
		}

		// make sure the given child is really a child of this node
		assert_true(child == leftChild || child == rightChild)
			<< "Node " << child << " is not a child of " << myAddress;

		// Phase 1: walk toward node covering all required regions
		lock.lock();	// lock this node for the next test
		if (!isRoot && !isSubRegion(regions,getAvailableData())) {

			// to avoid dead locks, abandon local lock while descending in tree; decent will aquire it again
			lock.unlock();

			// forward to parent
			auto data = network.getRemoteProcedure(myAddress.getParent(), &DataItemIndexService::acquireOwnershipFor)(regions,myAddress);

			// this should cover all requested data
			assert_eq(regions,data.getCoveredRegions());

			// the parent should have requested the lock
//			assert_false(lock.owns_lock()) << "Local: " << myAddress;

			// -- Phase 4: forward data to calling child --

			// update ownership data
			addRegions(regions);

			if (child == leftChild) {
				removeRegionsRight(regions);
				addRegionsLeft(regions);
			} else {
				removeRegionsLeft(regions);
				addRegionsRight(regions);
			}

			// lock child node
			network.getRemoteProcedure(child, &DataItemIndexService::lockForOwnershipTransfer)();

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
			removeRegionsLeft(regions);
			addRegionsRight(regions);
		} else {
			removeRegionsRight(regions);
			addRegionsLeft(regions);
		}

		// make sure new management knowledge is consistent (for the requested part)
		assert_eq(
				intersect(getAvailableData(),regions),
				intersect(merge(getAvailableDataLeft(),getAvailableDataRight()),regions)
			) << "Node:    " << myAddress << "\n"
			  << "Regions: " << regions << "\n"
			  << "Available: " << getAvailableData() << "\n"
			  << "Left:      " << getAvailableDataLeft() << "\n"
			  << "Right:     " << getAvailableDataRight() << "\n";

		// lock child node for ownership transfer
		network.getRemoteProcedure(child, &DataItemIndexService::lockForOwnershipTransfer)();

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
		assert_pred2(isSubRegion,regions,getAvailableData());

		// make sure local management knowledge is consistent
		if (!myAddress.isLeaf()) {
			assert_eq(
					intersect(getAvailableData(),regions),
					merge(intersect(getAvailableDataLeft(),regions),intersect(getAvailableDataRight(),regions))
				) << "Node:    " << myAddress << "\n"
				  << "Regions: " << regions << "\n"
				  << "Available: " << getAvailableData() << "\n"
				  << "Left:      " << getAvailableDataLeft() << "\n"
				  << "Right:     " << getAvailableDataRight() << "\n"
				  << "Merged:    " << merge(getAvailableDataLeft(),getAvailableDataRight()) << "\n"
				  << " -- intersected --\n"
				  << "Available: " << intersect(getAvailableData(),regions) << "\n"
				  << "Left:      " << intersect(getAvailableDataLeft(),regions) << "\n"
				  << "Right:     " << intersect(getAvailableDataRight(),regions) << "\n";
		}


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
			assert_pred2(isDisjoint,regions,getAvailableData())
				<< "Extracted: " << res.getCoveredRegions() << "\n"
				<< "Remaining: " << getAvailableData() << "\n";


			// done
			return res;
		}

		// -- inner nodes --
		auto res = collectOwnershipFromChildren(regions);

		// remove ownership of this node
		removeRegions(regions);

		// done
		return res;
	}

	DataItemMigrationData DataItemIndexService::collectOwnershipFromChildren(const DataItemRegions& regions) {

		// make sure the requested region is owned by this node
		assert_pred2(isSubRegion,regions,getAvailableData());

		auto missing = regions;
		DataItemMigrationData res;

		// left child
		{
			auto part = intersect(missing,getAvailableDataLeft());
			if (!part.empty()) {

				// retract ownership and retrieve data
				auto data = network.getRemoteProcedure(myAddress.getLeftChild(), &DataItemIndexService::abandonOwnership)(part);

				// make sure it is what was expected
				assert_eq(part,data.getCoveredRegions());

				// update management information
				removeRegionsLeft(part);

				// make sure left knowledge is consistent
				assert_eq(getAvailableDataLeft(),network.getRemoteProcedure(myAddress.getLeftChild(),&DataItemIndexService::getAvailableData)());

				// update remaining missing area
				missing = difference(missing,part);

				// add to result
				res.addAll(data);
			}
		}

		// right child
		if (!missing.empty()) {
			auto part = intersect(missing,getAvailableDataRight());
			if (!part.empty()) {

				// retract ownership and retrieve data
				auto data = network.getRemoteProcedure(myAddress.getRightChild(), &DataItemIndexService::abandonOwnership)(part);

				// make sure it is what was expected
				assert_eq(part,data.getCoveredRegions());

				// update management information
				removeRegionsRight(part);

				// make sure right knowledge is consistent
				assert_eq(getAvailableDataRight(),network.getRemoteProcedure(myAddress.getRightChild(),&DataItemIndexService::getAvailableData)());

				// update remaining missing area
				missing = difference(missing,part);

				// add to result
				res.addAll(data);
			}
		}

		// this should cover all
		assert_true(missing.empty())
			<< "Unable to locate: " << missing << "\n"
			<< "Owning: " << getAvailableData() << "\n"
			<< "Left:   " << getAvailableDataLeft() << "\n"
			<< "Right:  " << getAvailableDataRight() << "\n";

		// done
		return res;
	}


} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
