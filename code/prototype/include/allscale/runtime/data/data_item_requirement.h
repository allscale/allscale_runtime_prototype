/*
 * The prototype implementation of the data item requirements.
 *
 *  Created on: Jul 25, 2018
 *      Author: herbert
 */

#pragma once

#include <ostream>
#include <map>
#include <memory>
#include <typeinfo>
#include <typeindex>

#include "allscale/utils/assert.h"
#include "allscale/utils/tuple_utils.h"
#include "allscale/utils/printer/join.h"

#include "allscale/api/core/data.h"

#include "allscale/runtime/data/data_item_reference.h"
#include "allscale/runtime/data/data_item_region.h"

namespace allscale {
namespace runtime {
namespace data {

	/**
	 * The kind of access modes to be stated by requirements.
	 */
	enum AccessMode {
		ReadOnly,		// < read only mode
		ReadWrite		// < read/write mode
	};

	// provide printer support for access modes
	std::ostream& operator<<(std::ostream& out, AccessMode mode);


	/**
	 * A reference to a data item valid across nodes.
	 */
	template<typename DataItem>
	class DataItemRequirement {

		using ref_type = DataItemReference<DataItem>;
		using region_type = typename DataItem::region_type;

		// the reference to the data item this requirement is referring to
		ref_type ref;

		// the region to be accessed
		region_type region;

		// the access mode
		AccessMode mode;

	public:

		DataItemRequirement(const ref_type& ref, const region_type& region, AccessMode mode)
			: ref(ref), region(region), mode(mode) {}

		// extract the data item reference
		const ref_type& getDataItemReference() const {
			return ref;
		}

		// extract the targeted region
		const region_type& getRegion() const {
			return region;
		}

		// extract the requested access mode
		AccessMode getMode() const {
			return mode;
		}

		/**
		 * Tests whether this requirement is an empty requirement.
		 */
		bool empty() const {
			return region.empty();
		}

		// adds printer support
		friend std::ostream& operator<<(std::ostream& out,const DataItemRequirement& req) {
			return out << "Requirement(" << req.ref << "," << req.region << "," << req.mode << ")";
		}
	};

	/**
	 * A factory for a data item requirement.
	 */
	template<typename DataItem>
	DataItemRequirement<DataItem> createDataItemRequirement(const DataItemReference<DataItem>& ref, const typename DataItem::region_type& region, AccessMode mode) {
		return { ref, region, mode };
	}


	// --- generic data item requirement handling ---


	/**
	 * A collection of requirements.
	 */
	class DataItemRequirements {

		// all the read requirements
		DataItemRegions readRequirements;

		// all the write requirements
		DataItemRegions writeRequirements;

	public:

		DataItemRequirements() = default;

		DataItemRequirements(DataItemRegions&& read, DataItemRegions&& write)
			: readRequirements(std::move(read)), writeRequirements(std::move(write)) {}

		/**
		 * Tests whether the requirements are empty.
		 */
		bool empty() const;

		/**
		 * Add a requirement to this list of requirements.
		 */
		template<typename DataItem>
		void add(const DataItemRequirement<DataItem>& requirement) {
			// add requirements
			switch(requirement.getMode()) {
			case ReadOnly:  readRequirements.add(requirement.getDataItemReference(),requirement.getRegion()); break;
			case ReadWrite: writeRequirements.add(requirement.getDataItemReference(), requirement.getRegion()); break;
			default: assert_fail() << "Unsupported mode: " << requirement.getMode();
			}
		}

		// Extracts the read requirements from this set of requirements.
		const DataItemRegions& getReadRequirements() const {
			return readRequirements;
		}

		// Extracts the write requirements from this set of requirements.
		const DataItemRegions& getWriteRequirements() const {
			return writeRequirements;
		}

		/**
		 * Converts a tuple of data requirements into a requirement set.
		 */
		template<typename ... DataItems>
		static DataItemRequirements fromTuple(const std::tuple<DataItemRequirement<DataItems>...>& tuple) {
			DataItemRequirements res;

			// add all entries from the tuple
			allscale::utils::forEach(tuple,[&](const auto& cur) {
				res.add(cur);
			});

			// done
			return res;
		}

		// -- serialization --

		void store(allscale::utils::ArchiveWriter& out) const;

		static DataItemRequirements load(allscale::utils::ArchiveReader& in);

		// supports printing requirements
		friend std::ostream& operator<<(std::ostream&, const DataItemRequirements&);

	};


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
