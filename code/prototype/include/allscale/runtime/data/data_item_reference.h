/*
 * The prototype implementation of the data item interface.
 *
 *  Created on: Jul 25, 2018
 *      Author: herbert
 */

#pragma once

#include <cstdint>

#include "allscale/utils/serializer.h"

namespace allscale {
namespace runtime {
namespace data {


	// define the id type
	using DataItemID = std::uint32_t;

	// a generator function for fresh IDs
	DataItemID getFreshDataItemID();

	/**
	 * A reference to a data item valid across nodes.
	 */
	template<typename DataItemType>
	class DataItemReference {

		// the id of this data item
		DataItemID id;

	public:

		// a constructor to create a data item reference
		explicit DataItemReference(DataItemID id) : id(id) {}

		/**
		 * A factory for fresh data item references.
		 */
		static DataItemReference getFresh() {
			return DataItemReference(getFreshDataItemID());
		}

		DataItemID getID() const {
			return id;
		}

		// --- comparable ---

		bool operator==(const DataItemReference& other) const {
			return id == other.id;
		}

		bool operator!=(const DataItemReference& other) const {
			return id != other.id;
		}

		bool operator<(const DataItemReference& other) const {
			return id < other.id;
		}

		// --- serialization ---

		static DataItemReference load(allscale::utils::ArchiveReader& in) {
			return DataItemReference(in.read<DataItemID>());
		}

		void store(allscale::utils::ArchiveWriter& out) const {
			out.write(id);
		}

		// --- printable ---

		friend std::ostream& operator<<(std::ostream& out, const DataItemReference& ref) {
			return out << "DI-" << ref.id;
		}

	};


} // end of namespace data
} // end of namespace runtime
} // end of namespace allscale
