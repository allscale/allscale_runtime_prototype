
#include "allscale/runtime/data/data_item_location.h"

#include "allscale/utils/printer/join.h"

namespace allscale {
namespace runtime {
namespace data {


	DataItemRegions DataItemLocationInfos::getCoveredRegions() const {
		DataItemRegions res;
		for(const auto& cur : entries) {
			res.add(cur.second);
		}
		return res;
	}

	void DataItemLocationInfos::add(const DataItemRegions& regions, com::rank_t loc) {
		auto& entry = entries[loc];
		if (entry.empty()) {
			entry = regions;
		} else {
			entry = merge(entry,regions);
		}
	}

	// --- set operations ---

	DataItemLocationInfos& DataItemLocationInfos::addAll(const DataItemLocationInfos& other) {
		// merge location information
		for(const auto& cur : other.entries) {
			entries[cur.first].add(cur.second);
		}
		return *this;
	}

	// --- serialization ---

	void DataItemLocationInfos::store(allscale::utils::ArchiveWriter& out) const {
		// we simply need to save the list of location information
		out.write(entries);
	}

	DataItemLocationInfos DataItemLocationInfos::load(allscale::utils::ArchiveReader& in) {
		return in.read<entries_t>();
	}


	// --- utilities ---

	std::ostream& operator<<(std::ostream& out,const DataItemLocationInfos& infos) {
		return out << "Locations(" << allscale::utils::join(",",infos.entries,[](std::ostream& out, const auto& cur){
			out << cur.first << "=>" << cur.second;
		}) << ")";
	}


	// --- Location Cache ---


	void DataItemLocationCache::clear() {
		write_guard g(*lock);
		cache.clear();
	}

	void DataItemLocationCache::clear(const DataItemRegions& regions) {
		write_guard g(*lock);
		// remove knowledge of the given region
		for(auto& cur : cache) {
			cur.second = difference(cur.second,regions);
		}
	}

	DataItemLocationInfos DataItemLocationCache::lookup(const DataItemRegions& regions) const {
		read_guard g(*lock);

		// collect data to be returned
		DataItemLocationInfos res;
		for(const auto& cur : cache) {
			auto part = intersect(cur.second,regions);
			if (part.empty()) continue;
			res.add(part,cur.first);
		}
		return res;
	}

	void DataItemLocationCache::update(const DataItemLocationInfos& infos) {
		write_guard g(*lock);
		for(const auto& cur : infos.getLocationInfo()) {
			auto& known = cache[cur.first];
			known = merge(cur.second,known);
		}
	}

} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
