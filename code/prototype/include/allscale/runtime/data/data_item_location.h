/*
 * A set of utilities to realize data item transfers.
 *
 *  Created on: Jul 27, 2018
 *      Author: herbert
 */

#pragma once

#include <algorithm>
#include <deque>
#include <unordered_map>
#include <memory>
#include <mutex>
#include <typeinfo>
#include <typeindex>

#include "allscale/utils/assert.h"
#include "allscale/utils/serializer/functions.h"
#include "allscale/utils/serializer/unordered_maps.h"
#include "allscale/utils/fibers.h"
#include "allscale/utils/fiber/read_write_lock.h"

#include "allscale/runtime/com/node.h"

#include "allscale/runtime/data/data_item_region.h"

namespace allscale {
namespace runtime {
namespace data {

	/**
	 * A class associating data item regions to locations where they are stored.
	 */
	class DataItemLocationInfos {

		// the type of the internally maintained index
		using entries_t = std::unordered_map<com::rank_t,DataItemRegions>;

		// the list of located data regions
		entries_t entries;

	public:

		// --- constructors ---

		DataItemLocationInfos() = default;

		DataItemLocationInfos(const DataItemLocationInfos&) = default;

		DataItemLocationInfos(DataItemLocationInfos&&) = default;

	private:

		DataItemLocationInfos(entries_t&& data) : entries(std::move(data)) {}

	public:

		DataItemLocationInfos& operator=(const DataItemLocationInfos&) = default;
		DataItemLocationInfos& operator=(DataItemLocationInfos&&) = default;

		// --- observers ---

		/**
		 * Determines whether this location info record is empty.
		 */
		bool empty() const {
			return entries.empty();
		}

		/**
		 * Obtains the regions covered by this info.
		 */
		DataItemRegions getCoveredRegions() const;

		/**
		 * Obtains access to the per-location data share located.
		 */
		const entries_t& getLocationInfo() const {
			return entries;
		}

		// --- mutators ---

		/**
		 * Adds another location info to this info set.
		 */
		template<typename DataItem>
		void add(const DataItemReference<DataItem>& ref, const typename DataItem::region_type& region, com::rank_t loc) {
			entries[loc].add(ref,region);
		}

		void add(const DataItemRegions& regions, com::rank_t loc);

		// --- operators ---

		bool operator==(const DataItemLocationInfos& other) const {
			return entries == other.entries;
		}

		bool operator!=(const DataItemLocationInfos& other) const {
			return !(*this == other);
		}

		// --- set operations ---

		DataItemLocationInfos& addAll(const DataItemLocationInfos& other);

		// --- serialization ---

		void store(allscale::utils::ArchiveWriter& out) const;

		static DataItemLocationInfos load(allscale::utils::ArchiveReader& in);


		// --- utilities ---

		friend std::ostream& operator<<(std::ostream&,const DataItemLocationInfos&);
	};


	/**
	 * A cache for maintaining location information.
	 */
	class DataItemLocationCache {

		// the cached knowledge on the data distribution
		std::unordered_map<com::rank_t,DataItemRegions> cache;

		// a lock for synchronization
		std::unique_ptr<allscale::utils::fiber::ReadWriteLock> lock;

		// the guard type
		using read_guard = allscale::utils::fiber::ReadGuard;
		using write_guard = allscale::utils::fiber::WriteGuard;

	public:

		DataItemLocationCache() : lock(std::make_unique<allscale::utils::fiber::ReadWriteLock>()) {}

		// clears the cache content
		void clear();

		// clears an entry in the cache
		void clear(const DataItemRegions& regions);

		// performs a lookup in the cache, fills what is known
		DataItemLocationInfos lookup(const DataItemRegions& regions) const;

		// updates the cached information
		void update(const DataItemLocationInfos& infos);

	};


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
