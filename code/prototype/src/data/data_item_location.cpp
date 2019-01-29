
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
		for(auto& cur : cache) {
			if (cur.target == regions) {
				cur.valid = false;
				return;
			}
		}
	}

	const DataItemLocationInfos* DataItemLocationCache::lookup(const DataItemRegions& regions) const {
		read_guard g(*lock);
		for(const auto& cur : cache) {
			if (cur.target == regions) {
				if (cur.valid) {
					return &cur.info;
				} else {
					return nullptr;
				}
			}
		}
		return nullptr;
	}

	const DataItemLocationInfos& DataItemLocationCache::update(const DataItemRegions& regions, const DataItemLocationInfos& infos, bool valid) {
		write_guard g(*lock);
		for(auto& cur : cache) {
			if (cur.target == regions) {
				cur.info = infos;
				cur.valid = valid;
				return cur.info;
			}
		}
		cache.emplace_back(Entry{regions,infos,true});
		return cache.back().info;
	}


} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
