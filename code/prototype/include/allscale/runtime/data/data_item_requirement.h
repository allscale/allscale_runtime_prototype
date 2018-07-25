/*
 * The prototype implementation of the data item interface.
 *
 *  Created on: Jul 25, 2018
 *      Author: herbert
 */

#pragma once

#include <ostream>
#include <map>
#include <memory>
#include <typeinfo>
#include <typeindex>

#include "allscale/utils/assert.h"
#include "allscale/utils/tuple_utils.h"
#include "allscale/utils/printer/join.h"

#include "allscale/api/core/data.h"

#include "allscale/runtime/data/data_item_reference.h"

namespace allscale {
namespace runtime {
namespace data {

	/**
	 * The kind of access modes to be stated by requirements.
	 */
	enum AccessMode {
		ReadOnly,		// < read only mode
		ReadWrite		// < read/write mode
	};

	// provide printer support for access modes
	std::ostream& operator<<(std::ostream& out, AccessMode mode);


	/**
	 * A reference to a data item valid across nodes.
	 */
	template<typename DataItem>
	class DataItemRequirement {

		using ref_type = DataItemReference<DataItem>;
		using region_type = typename DataItem::region_type;

		// the reference to the data item this requirement is referring to
		ref_type ref;

		// the region to be accessed
		region_type region;

		// the access mode
		AccessMode mode;

	public:

		DataItemRequirement(const ref_type& ref, const region_type& region, AccessMode mode)
			: ref(ref), region(region), mode(mode) {}

		// extract the data item reference
		const ref_type& getDataItemReference() const {
			return ref;
		}

		// extract the targeted region
		const region_type& getRegion() const {
			return region;
		}

		// extract the requested access mode
		AccessMode getMode() const {
			return mode;
		}

		/**
		 * Tests whether this requirement is an empty requirement.
		 */
		bool empty() const {
			return region.empty();
		}

		// adds printer support
		friend std::ostream& operator<<(std::ostream& out,const DataItemRequirement& req) {
			return out << "Requirement(" << req.ref << "," << req.region << "," << req.mode << ")";
		}
	};

	/**
	 * A factory for a data item requirement.
	 */
	template<typename DataItem>
	DataItemRequirement<DataItem> createDataItemRequirement(const DataItemReference<DataItem>& ref, const typename DataItem::region_type& region, AccessMode mode) {
		return { ref, region, mode };
	}


	// --- generic data item requirement handling ---

	namespace detail {

		// a base class for a set of requirement of the same type
		class RequirementsBase {
		public:
			virtual ~RequirementsBase(){};


			// support printing requirements
			friend std::ostream& operator<<(std::ostream& out, const RequirementsBase& base);

			// tests whether this list of requirements is empty
			virtual bool empty() const =0;

			// requires specializations to be printable
			virtual void print(std::ostream& out) const=0;

		};

		// a generic derived class for a set of requirements on a given data item type
		template<typename DataItem>
		class Requirements : public RequirementsBase {

			using reference_type = DataItemReference<DataItem>;
			using requirement_type = DataItemRequirement<DataItem>;
			using region_type = typename DataItem::region_type;

			std::map<reference_type,region_type> readRequirements;
			std::map<reference_type,region_type> writeRequirements;

		public:

			bool empty() const override {
				return readRequirements.empty() && writeRequirements.empty();
			}

			void add(const requirement_type& req) {
				assert_false(req.empty());
				auto& map = (req.getMode() == ReadOnly) ? readRequirements : writeRequirements;
				auto& cur = map[req.getDataItemReference()];
				cur = allscale::api::core::merge(cur,req.getRegion());
			}


			void print(std::ostream& out) const override {
				// first the read requirements
				out << allscale::utils::join("\n\t",readRequirements,[](std::ostream& out, const auto& cur){
					out << "RO," << cur.first << "," << cur.second;
				});

				if (!readRequirements.empty() && !writeRequirements.empty()) out << "\n\t";

				// than the write requirements
				out << allscale::utils::join("\n\t",writeRequirements,[](std::ostream& out, const auto& cur){
					out << "RW," << cur.first << "," << cur.second;
				});
			}

		};

	} // end of namespace detail


	/**
	 * A collection of requirements.
	 */
	class DataItemRequirements {

		// the index of all requirements represented by collection
		std::map<std::type_index,std::unique_ptr<detail::RequirementsBase>> requirements;

	public:

		/**
		 * Tests whether the requirements are empty.
		 */
		bool empty() const;

		/**
		 * Add a requirement to this list of requirements.
		 */
		template<typename DataItem>
		void add(const DataItemRequirement<DataItem>& requirement) {
			// do not add empty requirements
			if (requirement.empty()) return;

			// add requirement to corresponding sub-list
			getRequirementsList<DataItem>().add(requirement);
		}

		/**
		 * Converts a tuple of data requirements into a requirement set.
		 */
		template<typename ... DataItems>
		static DataItemRequirements fromTuple(const std::tuple<DataItemRequirement<DataItems>...>& tuple) {
			DataItemRequirements res;

			// add all entries from the tuple
			allscale::utils::forEach(tuple,[&](const auto& cur) {
				res.add(cur);
			});

			// done
			return std::move(res);
		}

		// supports printing requirements
		friend std::ostream& operator<<(std::ostream&, const DataItemRequirements&);

	private:

		template<typename DataItem>
		detail::Requirements<DataItem>& getRequirementsList() {
			auto& req_ptr = requirements[typeid(DataItem)];
			if (!req_ptr) {
				req_ptr = std::make_unique<detail::Requirements<DataItem>>();
			}
			return static_cast<detail::Requirements<DataItem>&>(*req_ptr);
		}

		template<typename DataItem>
		const detail::Requirements<DataItem>& getRequirementsList() const {
			auto pos = requirements.find(typeid(DataItem));
			assert_true(pos != requirements.end());
			return static_cast<const detail::Requirements<DataItem>&>(*pos->second);
		}
	};


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
