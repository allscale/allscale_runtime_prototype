
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
		// here we cheat for the prototype
		// todo: provide an actual implementation
		out.write<std::intptr_t>(std::intptr_t(this));
	}

	DataItemLocationInfos DataItemLocationInfos::load(allscale::utils::ArchiveReader& in) {
		return *static_cast<const DataItemLocationInfos*>((void*)in.read<std::intptr_t>());
	}


	std::ostream& operator<<(std::ostream& out,const DataItemLocationInfos& infos) {
		return out << "Locations(" << allscale::utils::join(",",infos.entries,[](std::ostream& out, const auto& cur){
			out << *cur.second;
		}) << ")";
	}


} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
