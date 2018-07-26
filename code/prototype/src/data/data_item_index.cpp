
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



} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
