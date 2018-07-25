
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

	namespace detail {

		std::ostream& operator<<(std::ostream& out, const RequirementsBase& base) {
			base.print(out);
			return out;
		}

	} // end of namespace


	bool DataItemRequirements::empty() const {
		return requirements.empty();
	}

	std::ostream& operator<<(std::ostream& out, const DataItemRequirements& reqs) {

		// special handling of empty requirements
		if (reqs.empty()) {
			return out << "Requirements ()";
		}

		// other requirements get listed
		return out << "Requirements (\n\t" << join("\n\t",reqs.requirements, [](std::ostream& out, const auto& value) {
			out << *value.second;
		}) << "\n)";
	}


} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
