
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
		guard g(*lock);
		cache.clear();
	}

	DataItemLocationInfos DataItemLocationCache::lookup(const DataItemRegions& regions) const {
		guard g(*lock);
		for(const auto& cur : cache) {
			if (cur.first == regions) {
				return cur.second;
			}
		}
		return {};
	}

	void DataItemLocationCache::update(const DataItemLocationInfos& infos) {
		guard g(*lock);
		cache.push_back({infos.getCoveredRegions(),infos});
	}


} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
