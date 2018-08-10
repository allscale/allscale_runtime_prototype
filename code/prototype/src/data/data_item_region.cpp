
#include "allscale/runtime/data/data_item_region.h"

#include "allscale/utils/printer/join.h"

using namespace allscale::utils;

namespace allscale {
namespace runtime {
namespace data {


	std::ostream& operator<<(std::ostream& out, const DataItemRegions::RegionsBase& base) {
		base.print(out);
		return out;
	}


	DataItemRegions::DataItemRegions(const DataItemRegions& other) {
		for(const auto& cur : other.regions) {
			regions[cur.first] = cur.second->clone();
		}
	}

	DataItemRegions& DataItemRegions::operator=(const DataItemRegions& other) {
		if (this == &other) return *this;
		regions.clear();
		for(const auto& cur : other.regions) {
			regions[cur.first] = cur.second->clone();
		}
		return *this;
	}

	bool DataItemRegions::empty() const {
		return regions.empty();
	}


	void DataItemRegions::store(allscale::utils::ArchiveWriter& out) const {
		// we need to serialize the contained map-content (type_indices are note serializable)
		out.write<std::size_t>(regions.size());
		for(const auto& cur : regions) {
			cur.second->store(out);
		}
	}

	DataItemRegions DataItemRegions::load(allscale::utils::ArchiveReader& in) {
		DataItemRegions res;
		// we re-load the content, one by one
		auto num = in.read<std::size_t>();
		for(std::size_t i=0; i<num; i++) {
			auto cur = RegionsBase::load(in);
			res.regions[cur.first] = std::move(cur.second);
		}
		return res;
	}


	std::ostream& operator<<(std::ostream& out, const DataItemRegions& a) {
		// other regions get listed
		return out << "Regions(" << join(", ",a.regions, [](std::ostream& out, const auto& value) {
			out << *value.second;
		}) << ")";
	}

	void DataItemRegions::toJSON(std::ostream& out) const {
		if (empty()) {
			out << "[]";
			return;
		}
		bool first = true;
		out << "[";
		for(const auto& cur : regions) {
			if (!first) out << ",";
			first = false;
			cur.second->printJSON(out);
		}
		out << "]";
	}

	bool DataItemRegions::operator==(const DataItemRegions& other) const {
		for(const auto& cur : regions) {
			auto pos = other.regions.find(cur.first);
			if (pos == other.regions.end()) return false;
			if (*cur.second != *pos->second) return false;
		}
		return true;
	}

	// --- set operations ---

	DataItemRegions merge(const DataItemRegions& a, const DataItemRegions& b) {
		// quick exits
		if (a.empty()) return b;
		if (b.empty()) return a;

		// compute result
		DataItemRegions res = a;

		// compute set union
		for(const auto& cur : b.regions) {
			auto pos = a.regions.find(cur.first);
			if (pos == a.regions.end()) {
				res.regions[cur.first] = cur.second->clone();
			} else {
				res.regions[cur.first] = cur.second->merge(*pos->second);
			}
		}

		return res;
	}

	DataItemRegions intersect(const DataItemRegions& a, const DataItemRegions& b) {
		// quick exits
		if (a.empty()) return a;
		if (b.empty()) return b;

		// compute result
		DataItemRegions res;

		// compute set intersection
		for(const auto& cur : a.regions) {
			auto pos = b.regions.find(cur.first);
			if (pos == b.regions.end()) {
				continue;
			}

			auto diff = cur.second->intersect(*pos->second);
			if (bool(diff)) res.regions[cur.first] = std::move(diff);
		}

		return res;
	}


	DataItemRegions difference(const DataItemRegions& a, const DataItemRegions& b) {
		// quick exits
		if (a.empty()) return a;
		if (b.empty()) return a;

		// compute actual difference
		DataItemRegions res;

		// compute set difference
		for(const auto& cur : a.regions) {
			auto pos = b.regions.find(cur.first);
			if (pos == b.regions.end()) {
				res.regions[cur.first] = cur.second->clone();
				continue;
			}

			auto diff = cur.second->difference(*pos->second);
			if (bool(diff)) res.regions[cur.first] = std::move(diff);
		}

		// done
		return res;
	}

	bool isSubRegion(const DataItemRegions& a, const DataItemRegions& b) {
		// some quick solutions
		if (a.empty()) return true;
		if (b.empty()) return false;
		return difference(a,b).empty();
	}

	bool isDisjoint(const DataItemRegions& a, const DataItemRegions& b) {
		// some quick solutions
		if (a.empty() || b.empty()) return true;
		return intersect(a,b).empty();
	}

} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
