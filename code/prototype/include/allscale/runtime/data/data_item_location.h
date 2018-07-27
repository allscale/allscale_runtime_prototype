/*
 * A set of utilities to realize data item transfers.
 *
 *  Created on: Jul 27, 2018
 *      Author: herbert
 */

#pragma once

#include <algorithm>
#include <map>
#include <memory>
#include <typeinfo>
#include <typeindex>

#include "allscale/utils/assert.h"

#include "allscale/runtime/com/node.h"

#include "allscale/runtime/data/data_item_region.h"

namespace allscale {
namespace runtime {
namespace data {


	/**
	 * A class associating data item regions to locations where they are stored.
	 */
	class DataItemLocationInfos {

		class EntryBase {
		public:
			virtual ~EntryBase() {}
			virtual void addCoveredRegions(DataItemRegions& res) const =0;
			virtual std::unique_ptr<EntryBase> clone() const =0;
			virtual void merge(const EntryBase&) =0;
			virtual void print(std::ostream&) const =0;
			friend std::ostream& operator<<(std::ostream& out, const EntryBase& entry) {
				entry.print(out);
				return out;
			}
		};


		template<typename DataItem>
		class Entry : public EntryBase {

			using ref_type = DataItemReference<DataItem>;
			using region_type = typename DataItem::region_type;

			struct Part {
				region_type region;
				com::rank_t location;
			};

			std::map<ref_type,std::vector<Part>> elements;

		public:

			void addCoveredRegions(DataItemRegions& res) const override {
				for(const auto& cur : elements) {
					for(const auto& part : cur.second) {
						res.add(cur.first,part.region);
					}
				}
			}

			void add(const ref_type& ref, const region_type& region, com::rank_t loc) {
				// make sure there is no overlap
				assert_true(std::all_of(elements[ref].begin(),elements[ref].end(),[&](const auto& a){
					return region_type::intersect(a.region,region).empty();
				}));
				elements[ref].emplace_back(Part{region,loc});
			}

			template<typename Op>
			void forEach(const Op& op) {
				for(const auto& cur : elements) {
					for(const auto& part : cur.second) {
						op(cur.first, part.region, part.location);
					}
				}
			}

			virtual std::unique_ptr<EntryBase> clone() const override {
				return std::make_unique<Entry>(*this);
			}

			virtual void merge(const EntryBase& base) override {
				assert_true(dynamic_cast<const Entry*>(&base));
				const Entry& other = static_cast<const Entry&>(base);
				for(const auto& cur : other.elements) {
					for(const auto& part : cur.second) {
						add(cur.first,part.region,part.location);
					}
				}
			}

			virtual void print(std::ostream& out) const override {
				for(const auto& cur : elements) {
					for(const auto& part : cur.second) {
						out << cur.first << ":" << part.region << "@" << part.location << ",";
					}
				}
			}
		};

		// the list of located entries
		std::map<std::type_index,std::unique_ptr<EntryBase>> entries;

	public:

		// --- constructors ---

		DataItemLocationInfos() = default;

		DataItemLocationInfos(const DataItemLocationInfos&);

		DataItemLocationInfos(DataItemLocationInfos&&) = default;


		// --- observers ---

		/**
		 * Determines whether this location info record is empty.
		 */
		bool empty() const {
			return entries.empty();
		}

		/**
		 * Obtains the regions covered by this info.
		 */
		DataItemRegions getCoveredRegions() const;

		template<typename DataItem, typename Op>
		void forEach(const Op& op) const {
			auto pos = entries.find(typeid(DataItem));
			if (pos == entries.end()) return;
			static_cast<Entry<DataItem>&>(*pos->second).forEach(op);
		}

		// --- mutators ---

		/**
		 * Adds another location info to this info set.
		 */
		template<typename DataItem>
		void add(const DataItemReference<DataItem>& ref, const typename DataItem::region_type& region, com::rank_t loc) {
			get<DataItem>().add(ref,region,loc);
		}

		// --- set operations ---

		DataItemLocationInfos& addAll(const DataItemLocationInfos& other);

		// --- serialization ---

		void store(allscale::utils::ArchiveWriter& out) const;

		static DataItemLocationInfos load(allscale::utils::ArchiveReader& in);


		// --- utilities ---

		friend std::ostream& operator<<(std::ostream&,const DataItemLocationInfos&);

	private:

		template<typename DataItem>
		Entry<DataItem>& get() {
			auto& ptr = entries[typeid(DataItem)];
			if (!bool(ptr)) ptr = std::make_unique<Entry<DataItem>>();
			return static_cast<Entry<DataItem>&>(*ptr);
		}

	};


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
