
#include "allscale/runtime/data/data_item_index.h"

namespace allscale {
namespace runtime {
namespace data {


	// tests whether the given requirements are covered by this node
	bool DataItemIndexService::coversWriteRequirements(const DataItemRequirements& requirements) {
		// the requirements are covered if the needed regions are empty
		return getMissingRegions(requirements).empty();
	}

	// computes the set of required regions to cover the given requirements
	DataItemRegions DataItemIndexService::getMissingRegions(const DataItemRequirements& requirements) {

		// obtain required regions
		auto needed = requirements.getWriteRequirements();

		// if nothing is needed => we are done
		if (needed.empty()) return {};

		// see what is available
		auto avail = getAvailableData();

		// return the difference
		return difference(needed,avail);
	}

	// computes the set of required regions to schedule in the left sub-tree
	DataItemRegions DataItemIndexService::getMissingRegionsLeft(const DataItemRequirements& requirements) {

		// obtain required regions
		auto needed = requirements.getWriteRequirements();

		// if nothing is needed => we are done
		if (needed.empty()) return {};

		// see what is available
		auto avail = getAvailableDataLeft();

		// return the difference
		return difference(needed,avail);
	}

	// computes the set of required regions to schedule in the right sub-tree
	DataItemRegions DataItemIndexService::getMissingRegionsRight(const DataItemRequirements& requirements) {

		// obtain required regions
		auto needed = requirements.getWriteRequirements();

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
		for(const auto& cur : indices) {
			cur.second->add(regions);
		}
	}

	// adds the provided regions to the coverage of the left sub tree
	void DataItemIndexService::addRegionsLeft(const DataItemRegions& regions) {
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
		for(const auto& cur : indices) {
			cur.second->addRight(regions);
		}
	}


	// removes the provided regions to this node
	void DataItemIndexService::removeRegions(const DataItemRegions& regions) {

		// if empty => all done
		if (regions.empty()) return;

		// adding regions
		for(const auto& cur : indices) {
			cur.second->remove(regions);
		}
	}

	// removes the provided regions to the coverage of the left sub tree
	void DataItemIndexService::removeRegionsLeft(const DataItemRegions& regions) {
		// if empty => all done
		if (regions.empty()) return;

		// adding regions
		for(const auto& cur : indices) {
			cur.second->removeLeft(regions);
		}
	}

	// removes the provided regions to the coverage of the right sub tree
	void DataItemIndexService::removeRegionsRight(const DataItemRegions& regions) {
		// if empty => all done
		if (regions.empty()) return;

		// adding regions
		for(const auto& cur : indices) {
			cur.second->removeRight(regions);
		}
	}


	// computes the data regions available on this node
	DataItemRegions DataItemIndexService::getAvailableData() const {
		DataItemRegions res;
		for(const auto& cur : indices) {
			cur.second->addAvailable(res);
		}
		return res;
	}

	// computes the data regions available in the left sub tree
	DataItemRegions DataItemIndexService::getAvailableDataLeft() const {
		DataItemRegions res;
		for(const auto& cur : indices) {
			cur.second->addAvailableLeft(res);
		}
		return res;
	}

	// computes the data regions available in the right sub tree
	DataItemRegions DataItemIndexService::getAvailableDataRight() const {
		DataItemRegions res;
		for(const auto& cur : indices) {
			cur.second->addAvailableRight(res);
		}
		return res;
	}

	DataItemLocationInfos DataItemIndexService::locate(const DataItemRegions& regions) {

		// this is algorithm 1 of the runtime paper

		// quick exit - if there is nothing requested
		if (regions.empty()) return {};

		// start by taking data from cache
		auto res = locationCache.lookup(regions);

		// compute set of missing information
		auto remaining = difference(regions,res.getCoveredRegions());

		// -- add data for local sub-tree  --

		if (myAddress.isLeaf()) {

			// add local information
			for(const auto& cur : indices) {
				cur.second->addLocationInfo(remaining,res);
			}

		} else {

			// for inner nodes, query sub-trees
			{
				// start with left
				auto part = intersect(remaining,getAvailableDataLeft());
				if (!part.empty()) {

					// query sub-tree
					auto subInfo = network.getRemoteProcedure(myAddress.getLeftChild(),&DataItemIndexService::locate)(part);

					// consistency check
					assert_eq(part,subInfo.getCoveredRegions());

					// add to result
					res.addAll(subInfo);

				}

				// reduce remaining
				remaining = difference(remaining,part);

			}

			// and if necessary also the right sub-tree
			if (!remaining.empty()) {

				auto part = intersect(remaining,getAvailableDataRight());
				if (!part.empty()) {

					// query sub-tree
					auto subInfo = network.getRemoteProcedure(myAddress.getRightChild(),&DataItemIndexService::locate)(part);

					// consistency check
					assert_eq(part,subInfo.getCoveredRegions());

					// add to result
					res.addAll(subInfo);

				}
			}
		}


		// -- complete query by escalating if necessary --

		// if this is the root, there is nowhere to escalate any more
		if (isRoot) return std::move(res);

		// get covered regions
		auto covered = res.getCoveredRegions();

		// get missing regions
		auto missing = difference(regions,covered);

		// see whether there is nothing to search for left
		if (missing.empty()) return std::move(res);

		// ask parent
		auto extra = network.getRemoteProcedure(myAddress.getParent(),&DataItemIndexService::locate)(missing);

		// merge partial results
		res.addAll(extra);

		// cache result
		locationCache.update(res);

		// done
		return std::move(res);
	}


	DataItemMigrationData DataItemIndexService::acquire(const DataItemRegions& regions) {
		// this entry point is only to be called on the leaf level
		assert_true(myAddress.isLeaf());

		// if this is the root node, there is no place to retrieve data from
		if (isRoot) {
			assert_not_implemented();
			// TODO: extend migration data to include entries without data, to be default constructed
			return {};
		}

		// compute the missing regions
		auto missing = difference(regions,getAvailableData());

		// send request to parent
		auto res = network.getRemoteProcedure(myAddress.getParent(), &DataItemIndexService::acquireOwnership)(myAddress,missing);

		// make sure the requested data is complete
		assert_eq(regions,res.getCoveredRegions());

		// register ownership
		addRegions(res.getCoveredRegions());

		// done
		return res;
	}

	DataItemMigrationData DataItemIndexService::acquireOwnership(com::HierarchyAddress caller, const DataItemRegions& regions) {

		// the caller must be related
		assert_true(caller == myAddress.getParent() || caller == myAddress.getLeftChild() || caller == myAddress.getRightChild())
			<< "MyAddress: " << myAddress << "\n"
			<< "Caller:    " << caller << "\n";


		// this is similar to the lookup operation, just that while returning ownership is transfered

		// start out empty
		DataItemMigrationData res;

		// quick exit - if there is nothing requested
		if (regions.empty()) return res;

		// keep track of the regions still to resolve
		auto remaining = regions;

		// -- add data for local sub-tree  --

		if (myAddress.isLeaf()) {

			// this must be called from the parent site
			assert_eq(caller,myAddress.getParent());

			// add local information
			for(const auto& cur : indices) {
				cur.second->abandonOwnership(remaining,res);
			}

			// ownership should be reduced impicitly
			assert_true(intersect(res.getCoveredRegions(),getAvailableData()).empty())
				<< "Extracted: " << res.getCoveredRegions() << "\n"
				<< "Remaining: " << getAvailableData() << "\n";

			return res;
		}

		// for inner nodes, query sub-trees
		{
			// start with left
			auto part = intersect(remaining,getAvailableDataLeft());
			if (!part.empty()) {

				// in this case the call should not be coming from the left side
				assert_ne(caller,myAddress.getLeftChild());

				// query sub-tree
				auto subData = network.getRemoteProcedure(myAddress.getLeftChild(),&DataItemIndexService::acquireOwnership)(myAddress,part);

				// consistency check
				assert_eq(part,subData.getCoveredRegions());

				// reduce left ownership
				removeRegionsLeft(subData.getCoveredRegions());

				// add to result
				res.addAll(subData);

			}

			// reduce remaining
			remaining = difference(remaining,part);

		}

		// and if necessary also the right sub-tree
		if (!remaining.empty()) {

			auto part = intersect(remaining,getAvailableDataRight());
			if (!part.empty()) {

				// in this case the call should not be coming from the right side
				assert_ne(caller,myAddress.getRightChild());

				// query sub-tree
				auto subData = network.getRemoteProcedure(myAddress.getRightChild(),&DataItemIndexService::acquireOwnership)(myAddress,part);

				// consistency check
				assert_eq(part,subData.getCoveredRegions());

				// add to result
				res.addAll(subData);

				// reduce right ownership
				removeRegionsRight(subData.getCoveredRegions());

			}
		}

		// if transfer of ownership is toward parent, remove local ownership
		if (caller == myAddress.getParent()) {
			removeRegions(res.getCoveredRegions());
		}


		// -- complete query by escalating if necessary --

		// get covered regions
		auto covered = res.getCoveredRegions();

		// get missing regions
		auto missing = difference(regions,covered);

		// if there is still something missing => escalate
		if (!missing.empty()) {

			// in this case the call must not have come from the parent
			assert_ne(caller,myAddress.getParent());

			// if this is the root, there is nowhere to escalate any more
			if (isRoot) {
				assert_not_implemented();
				// TODO: do the same as above, in the acquire function
				return res;
			}

			// ask parent
			auto extra = network.getRemoteProcedure(myAddress.getParent(),&DataItemIndexService::acquireOwnership)(myAddress,missing);

			// add to local ownership
			addRegions(extra.getCoveredRegions());

			// merge partial results
			res.addAll(extra);

		}

		// add new ownerships to target node
		if (caller == myAddress.getLeftChild()) {
			addRegionsLeft(res.getCoveredRegions());
		} else if (caller == myAddress.getRightChild()) {
			addRegionsRight(res.getCoveredRegions());
		}

		// done
		return res;
	}

} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
