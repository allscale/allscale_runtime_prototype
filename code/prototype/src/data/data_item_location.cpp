
#include "allscale/runtime/data/data_item_location.h"

#include "allscale/utils/printer/join.h"

namespace allscale {
namespace runtime {
namespace data {


	DataItemLocationInfos::DataItemLocationInfos(const DataItemLocationInfos& other) {
		for(const auto& cur : other.entries) {
			entries[cur.first] = cur.second->clone();
		}
	}


	DataItemRegions DataItemLocationInfos::getCoveredRegions() const {
		DataItemRegions res;
		for(const auto& cur : entries) {
			cur.second->addCoveredRegions(res);
		}
		return std::move(res);
	}

	bool DataItemLocationInfos::operator==(const DataItemLocationInfos& other) const {
		if (this == &other) return true;

		// check the size
		if (entries.size() != other.entries.size()) return false;

		// check the key
		for(const auto& cur : entries) {
			auto pos = other.entries.find(cur.first);
			if (pos == other.entries.end()) return false;
			if (*cur.second != *pos->second) return false;
		}

		// all the same, we are fine
		return true;
	}

	DataItemLocationInfos& DataItemLocationInfos::addAll(const DataItemLocationInfos& other) {
		for(const auto& cur : other.entries) {
			auto pos = entries.find(cur.first);
			if (pos != entries.end()) {
				// merge elements
				pos->second->merge(*cur.second);
			} else {
				// copy entirely
				entries[cur.first] = cur.second->clone();
			}
		}
		return *this;
	}

	void DataItemLocationInfos::store(allscale::utils::ArchiveWriter& out) const {
		// we need to write out all elements
		out.write<std::size_t>(entries.size());
		for(const auto& cur : entries) {
			cur.second->store(out);
		}
	}

	DataItemLocationInfos DataItemLocationInfos::load(allscale::utils::ArchiveReader& in) {
		// restore entries
		auto num = in.read<std::size_t>();
		DataItemLocationInfos res;
		for(std::size_t i=0; i<num; i++) {
			auto cur = EntryBase::load(in);
			res.entries[cur.first] = std::move(cur.second);
		}
		return std::move(res);
	}


	std::ostream& operator<<(std::ostream& out,const DataItemLocationInfos& infos) {
		return out << "Locations(" << allscale::utils::join(",",infos.entries,[](std::ostream& out, const auto& cur){
			out << *cur.second;
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
