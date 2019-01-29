
#include "allscale/runtime/data/data_item_requirement.h"

#include "allscale/utils/printer/join.h"

using namespace allscale::utils;

namespace allscale {
namespace runtime {
namespace data {

	std::ostream& operator<<(std::ostream& out, AccessMode mode) {
		switch(mode) {
		case ReadOnly: return out << "RO";
		case ReadWrite: return out << "RW";
		}
		return out << "?";
	}


	bool DataItemRequirements::empty() const {
		return readRequirements.empty() && writeRequirements.empty();
	}

	void DataItemRequirements::store(allscale::utils::ArchiveWriter& out) const {
		out.write(readRequirements);
		out.write(writeRequirements);
	}

	DataItemRequirements DataItemRequirements::load(allscale::utils::ArchiveReader& in) {
		auto read = in.read<DataItemRegions>();
		auto write = in.read<DataItemRegions>();
		return DataItemRequirements{ std::move(read),std::move(write) };
	}


	std::ostream& operator<<(std::ostream& out, const DataItemRequirements& reqs) {
		// other requirements get listed
		return out << "Requirements( RO: " << reqs.readRequirements << ", RW: " << reqs.writeRequirements << ")";
	}


} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
