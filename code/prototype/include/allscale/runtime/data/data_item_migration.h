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
#include <mutex>
#include <typeinfo>
#include <typeindex>

#include "allscale/utils/assert.h"

#include "allscale/utils/optional.h"
#include "allscale/utils/serializer/optionals.h"

#include "allscale/runtime/com/node.h"

#include "allscale/runtime/data/data_item_region.h"

namespace allscale {
namespace runtime {
namespace data {


	/**
	 * A class to represent the data collected during an ownership transfer.
	 *
	 * It is the result type of the acquire call in the data item management service. With
	 * its transfer, a snapshot of the current data as well as ownership rights are transfered
	 * from the callee to caller.
	 */
	class DataItemMigrationData {

		class EntryBase {

		public:

			using load_res_t = std::pair<std::type_index,std::unique_ptr<EntryBase>>;
			using load_fun_t = load_res_t(*)(allscale::utils::ArchiveReader&);

		private:

			load_fun_t load_fun;

		public:

			EntryBase(load_fun_t load_fun) : load_fun(load_fun) {}

			virtual ~EntryBase() {}
			virtual void addCoveredRegions(DataItemRegions& res) const =0;
			virtual std::unique_ptr<EntryBase> clone() const =0;
			virtual void merge(const EntryBase&) =0;
			virtual void print(std::ostream&) const =0;
			friend std::ostream& operator<<(std::ostream& out, const EntryBase& entry) {
				entry.print(out);
				return out;
			}

			// provide serialization support
			void store(allscale::utils::ArchiveWriter& out) const {
				out.write(load_fun);
				storeInternal(out);
			}

			static load_res_t load(allscale::utils::ArchiveReader& in) {
				load_fun_t load = in.read<load_fun_t>();
				return load(in);
			}

			virtual void storeInternal(allscale::utils::ArchiveWriter&) const =0;

		};


		template<typename DataItem>
		class Entry : public EntryBase {

			using ref_type = DataItemReference<DataItem>;
			using region_type = typename DataItem::region_type;
			using data_type = allscale::utils::Archive;

			struct Part {
				region_type region;
				data_type data;

				void store(allscale::utils::ArchiveWriter& out) const {
					out.write(region);
					out.write(data);
				}

				static Part load(allscale::utils::ArchiveReader& in) {
					region_type r = in.read<region_type>();
					auto a = in.read<data_type>();
					return { std::move(r), std::move(a) };
				}
			};

			using location_map = std::map<ref_type,std::vector<Part>>;

			location_map elements;

		public:

			Entry() : EntryBase(&load) {}
			Entry(location_map&& map) : EntryBase(&load), elements(std::move(map)) {}

			void addCoveredRegions(DataItemRegions& res) const override {
				for(const auto& cur : elements) {
					for(const auto& part : cur.second) {
						res.add(cur.first,part.region);
					}
				}
			}

			void add(const ref_type& ref, const region_type& region, data_type&& archive) {
				// make sure there is no overlap
				assert_true(std::all_of(elements[ref].begin(),elements[ref].end(),[&](const auto& a){
					return region_type::intersect(a.region,region).empty();
				}));
				elements[ref].emplace_back(Part{region,std::move(archive)});
			}

			void add(const ref_type& ref, const region_type& region, const data_type& archive) {
				// make sure there is no overlap
				assert_true(std::all_of(elements[ref].begin(),elements[ref].end(),[&](const auto& a){
					return region_type::intersect(a.region,region).empty();
				}));
				elements[ref].emplace_back(Part{region,archive});
			}

			template<typename Op>
			void forEach(const Op& op) {
				for(auto& cur : elements) {
					for(auto& part : cur.second) {
						op(cur.first, part.region, part.data);
					}
				}
			}

			virtual std::unique_ptr<EntryBase> clone() const override {
				return std::make_unique<Entry>(*this);
			}

			virtual void storeInternal(allscale::utils::ArchiveWriter& out) const override {
				out.write<location_map>(elements);
			}

			static load_res_t load(allscale::utils::ArchiveReader& in) {
				return std::make_pair(
					std::type_index(typeid(DataItem)),
					std::make_unique<Entry>(in.read<location_map>())
				);
			}

			virtual void merge(const EntryBase& base) override {
				assert_true(dynamic_cast<const Entry*>(&base));
				const Entry& other = static_cast<const Entry&>(base);
				for(const auto& cur : other.elements) {
					for(const auto& part : cur.second) {
						add(cur.first,part.region,part.data);
					}
				}
			}

			virtual void print(std::ostream& out) const override {
				for(const auto& cur : elements) {
					for(const auto& part : cur.second) {
						out << cur.first << ":" << part.region << ",";
					}
				}
			}
		};

		// the list of located entries
		std::map<std::type_index,std::unique_ptr<EntryBase>> entries;

		// the list of located entries, that can be default-initialized
		DataItemRegions initRegions;

	public:

		// --- constructors ---

		DataItemMigrationData() = default;

		DataItemMigrationData(const DataItemMigrationData&);

		DataItemMigrationData(DataItemMigrationData&&) = default;


		DataItemMigrationData& operator=(DataItemMigrationData&&) = default;

		// --- observers ---

		/**
		 * Determines whether this location info record is empty.
		 */
		bool empty() const {
			return entries.empty() && initRegions.empty();
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

		/**
		 * Obtains the regions allowed to be default-initialized on the target nodes.
		 */
		const DataItemRegions& getDefaultInitializableRegions() const {
			return initRegions;
		}

		// --- mutators ---

		/**
		 * Adds another location info to this info set.
		 */
		template<typename DataItem>
		void add(const DataItemReference<DataItem>& ref, const typename DataItem::region_type& region, const allscale::utils::Archive& data) {
			get<DataItem>().add(ref,region,data);
		}

		/**
		 * Adds the given region to the set default initializable regions to be transfered.
		 */
		void addDefaultInitRegions(const DataItemRegions& regions) {
			if (regions.empty()) return;
			initRegions = merge(initRegions,regions);
		}

		// --- set operations ---

		DataItemMigrationData& addAll(const DataItemMigrationData& other);

		// --- serialization ---

		void store(allscale::utils::ArchiveWriter& out) const;

		static DataItemMigrationData load(allscale::utils::ArchiveReader& in);


		// --- utilities ---

		friend std::ostream& operator<<(std::ostream&,const DataItemMigrationData&);

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
