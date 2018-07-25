/*
 * The prototype implementation of the data item interface.
 *
 *  Created on: Jul 25, 2018
 *      Author: herbert
 */

#pragma once

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

	/**
	 * A reference to a data item valid across nodes.
	 */
	template<typename DataItem>
	class DataItemRequirement {

		using ref_type = DataItemReference<DataItem>;
		using region_type = typename DataItem::region_type;

		// TODO: define and implement!

		ref_type ref;

		region_type region;

		AccessMode mode;

	public:

		DataItemRequirement(const ref_type& ref, const region_type& region, AccessMode mode)
			: ref(ref), region(region), mode(mode) {}

//		DataItemRequirement(const DataItemRequirement&) = default;
//		DataItemRequirement(DataItemRequirement&&) = default;

	};

	/**
	 * A factory for a data item requirement.
	 */
	template<typename DataItem>
	DataItemRequirement<DataItem> createDataItemRequirement(const DataItemReference<DataItem>& ref, const typename DataItem::region_type& region, AccessMode mode) {
		return { ref, region, mode };
	}

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
