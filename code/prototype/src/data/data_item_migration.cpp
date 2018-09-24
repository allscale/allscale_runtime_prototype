
#include "allscale/runtime/data/data_item_migration.h"

#include "allscale/utils/printer/join.h"

namespace allscale {
namespace runtime {
namespace data {


	DataItemMigrationData::DataItemMigrationData(const DataItemMigrationData& other) : initRegions(other.initRegions) {
		for(const auto& cur : other.entries) {
			entries[cur.first] = cur.second->clone();
		}
	}


	DataItemRegions DataItemMigrationData::getCoveredRegions() const {
		DataItemRegions res = initRegions;
		for(const auto& cur : entries) {
			cur.second->addCoveredRegions(res);
		}
		return res;
	}

	DataItemMigrationData& DataItemMigrationData::addAll(const DataItemMigrationData& other) {
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
		addDefaultInitRegions(other.getDefaultInitializableRegions());
		return *this;
	}

	void DataItemMigrationData::store(allscale::utils::ArchiveWriter& out) const {
		// we need to write out all elements
		out.write<std::size_t>(entries.size());
		for(const auto& cur : entries) {
			cur.second->store(out);
		}
		out.write(initRegions);
	}

	DataItemMigrationData DataItemMigrationData::load(allscale::utils::ArchiveReader& in) {
		// restore entries
		auto num = in.read<std::size_t>();
		DataItemMigrationData res;
		for(std::size_t i=0; i<num; i++) {
			auto cur = EntryBase::load(in);
			res.entries[cur.first] = std::move(cur.second);
		}
		res.initRegions = in.read<DataItemRegions>();
		return res;
	}


	std::ostream& operator<<(std::ostream& out,const DataItemMigrationData& infos) {
		return out << "Data(" << allscale::utils::join(",",infos.entries,[](std::ostream& out, const auto& cur){
			out << *cur.second;
		}) << "," << infos.initRegions << ")";
	}


} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
