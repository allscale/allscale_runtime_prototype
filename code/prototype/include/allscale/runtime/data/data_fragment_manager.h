/*
 * The prototype implementation of the data item server.
 *
 *  Created on: Jul 23, 2018
 *      Author: herbert
 */

#pragma once

#include "allscale/api/core/data.h"

#include "allscale/runtime/com/node.h"
#include "allscale/runtime/com/network.h"

namespace allscale {
namespace runtime {
namespace data {

	template<typename DataItem>
	class DataFragmentManager {

		// Test that the passed data item type is valid.
		static_assert(allscale::api::core::is_data_item<DataItem>::value, "Can only be instantiated for data items!");

		// ------ some type definitions ------

		using shared_data_type = typename DataItem::shared_data_type;
		using region_type      = typename DataItem::region_type;
		using fragment_type    = typename DataItem::fragment_type;
		using facade_type      = typename DataItem::facade_type;


		/**
		 * A summary of shared and exclusively managed regions on various levels.
		 */
		struct Ownership {
			// the region maintained, potentially shared
			region_type shared;
			// the region maintained, exclusive (subregion of shared)
			region_type exclusive;

			// checks invariants on this object
			bool check() const {
				// only invariant: exclusive <= shared
				using namespace allscale::api::core;
				return isSubRegion(exclusive,shared);
			}
		};


		// -- node-local management information --

		// the management data shared among all instances
		shared_data_type shared_data;

		// the locally maintained data fragment
		fragment_type fragment;

		// the locally maintained shared and exclusive regions
		Ownership local;

		// TODO: add locking


		// -- hierarchical network management information --

		/**
		 * Aggregation of distribution info on a higher node level.
		 *
		 * Invariants:
		 * 		left.shared + right.shared <= full.shared
		 * 		left.exclusive + right.exclusive <= full.exclusive
		 * 		left.exclusive \cap right.exclusive = empty
		 * 		left.exclusive <= left.shared
		 * 		right.exclusive
		 */
		struct Info {
			// the area managed by this node on the corresponding level
			Ownership full;
			// the area managed by the left child
			Ownership left;
			// the area managed by the right child
			Ownership right;

			// checks this entry for consistency
			bool check() const {
				using namespace allscale::api::core;
				// check all the invariants on the areas in this info entry
				return full.check() && left.check() && right.check() &&
				       isSubRegion(merge(left.shared,right.shared),full.shared) &&
					   isSubRegion(merge(left.exclusive,right.exclusive),full.exclusive) &&
					   intersect(left.exclusive,right.exclusive).empty();
			}
		};

		// a vector of distribution information maintained on this node
		std::vector<Info> distribution_info;

	public:

		DataFragmentManager(const shared_data_type& shared_data)
			: shared_data(shared_data), fragment(this->shared_data) {

			// make sure that nothing is owned yet
			assert_true(local.shared.empty());
			assert_true(local.exclusive.empty());
			assert_true(fragment.getCoveredRegion().empty());

			// initialize distribution info
			auto com_size = com::Node::getNetwork().numNodes();

//			auto rank = com::Node::getLocalRank();
//			std::cout << "Initializing fragment at " << rank << " of " << com_size << "\n";

			// compute tree tree height (ceil(log2(com_size)))
			com::size_t s = 1;
			int c = 0;
			while (s < com_size) {
				s <<= 1;
				c++;
			}
//			std::cout << "Tree size: " << s << "\n";
//			std::cout << "Tree height: " << c << "\n";

			// allocate memory for management data (for simplicity, the same everywhere)
			distribution_info.resize(c);

		}

		// no copy but move
		DataFragmentManager(const DataFragmentManager&) = delete;
		DataFragmentManager(DataFragmentManager&&) = default;

		/**
		 * Obtains access to the managed fragment through a facade.
		 * The fragment itself is not exposed to avoid messing with covered regions.
		 */
		facade_type getDataItem() {
			return fragment.mask();
		}

//		friend std::ostream& operator<<(std::ostream&,const DataFragmentManager& server) {
//
//		}

	};


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
