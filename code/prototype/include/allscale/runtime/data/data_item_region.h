/*
 * The prototype implementation of the data item region.
 *
 *  Created on: Jul 26, 2018
 *      Author: herbert
 */

#pragma once

#include <ostream>
#include <map>
#include <memory>
#include <typeinfo>
#include <typeindex>

#include "allscale/utils/assert.h"
#include "allscale/utils/serializer.h"
#include "allscale/utils/serializer/maps.h"
#include "allscale/utils/serializer/functions.h"
#include "allscale/utils/printer/join.h"

#include "allscale/api/core/data.h"

#include "allscale/runtime/data/data_item_reference.h"

namespace allscale {
namespace runtime {
namespace data {

	/**
	 * A region is the right of a entity to allocate a region
	 * granted by the next higher node in the hierarchy.
	 */
	template<typename DataItem>
	class DataItemRegion {

		using ref_type = DataItemReference<DataItem>;
		using region_type = typename DataItem::region_type;

		// the reference to the data item this region is referring to
		ref_type ref;

		// the region to be accessed
		region_type region;

	public:

		DataItemRegion(const ref_type& ref, const region_type& region)
			: ref(ref), region(region) {}

		// extract the data item reference
		const ref_type& getDataItemReference() const {
			return ref;
		}

		// extract the targeted region
		const region_type& getRegion() const {
			return region;
		}

		/**
		 * Tests whether this region is an empty region.
		 */
		bool empty() const {
			return region.empty();
		}

		// -- serialization --

		void store(allscale::utils::ArchiveWriter& out) const {
			out.write(ref);
			out.write(region);
		}

		static DataItemRegion load(allscale::utils::ArchiveReader& in) {
			auto ref = in.read<ref_type>();
			auto reg = in.read<region_type>();
			return { ref, reg };
		}

		// adds printer support
		friend std::ostream& operator<<(std::ostream& out,const DataItemRegion& a) {
			return out << "Region(" << a.ref << "," << a.region << ")";
		}
	};

	/**
	 * A factory for a data item regions.
	 */
	template<typename DataItem>
	DataItemRegion<DataItem> createDataItemRegion(const DataItemReference<DataItem>& ref, const typename DataItem::region_type& region) {
		return { ref, region };
	}


	// --- generic data item region handling ---

	/**
	 * A collection of regions.
	 */
	class DataItemRegions {

		// a base class for a set of region of the same type
		class RegionsBase {

		public:

			using load_res_t = std::pair<std::type_index,std::unique_ptr<RegionsBase>>;
			using load_fun_t = load_res_t(*)(allscale::utils::ArchiveReader&);

		private:

			load_fun_t load_fun;

		public:

			RegionsBase(load_fun_t load_fun) : load_fun(load_fun) {}

			virtual ~RegionsBase(){};

			// support printing Regions
			friend std::ostream& operator<<(std::ostream& out, const RegionsBase& base);

			// tests whether this list of regions is empty
			virtual bool empty() const =0;

			// requires specializations to be printable
			virtual void print(std::ostream& out) const=0;

			virtual bool operator==(const RegionsBase& otherBase) const=0;

			bool operator!=(const RegionsBase& other) const {
				return !(*this == other);
			}

			// provide clone support
			virtual std::unique_ptr<RegionsBase> clone() const=0;

			virtual std::unique_ptr<RegionsBase> merge(const RegionsBase& other) const =0;

			virtual std::unique_ptr<RegionsBase> intersect(const RegionsBase& other) const =0;

			virtual std::unique_ptr<RegionsBase> difference(const RegionsBase& other) const =0;

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

		// support printing
		friend std::ostream& operator<<(std::ostream& out, const DataItemRegions::RegionsBase& base);

		// a generic derived class for a set of regions on a given data item type
		template<typename DataItem>
		class Regions : public RegionsBase {

			using reference_type = DataItemReference<DataItem>;
			using region_type = typename DataItem::region_type;
			using regions_map_type = std::map<reference_type,region_type>;

			regions_map_type regions;

		public:

			Regions() : RegionsBase(&load) {};
			Regions(const Regions&) = default;

			Regions(std::map<reference_type,region_type>&& map) : RegionsBase(&load), regions(std::move(map)) {}

			bool empty() const override {
				return regions.empty();
			}

			const std::map<reference_type,region_type>& getRegions() const {
				return regions;
			}

			void add(const reference_type& ref, const region_type& region) {
				assert_false(region.empty());
				auto& cur = regions[ref];
				cur = region_type::merge(cur,region);
			}

			bool operator==(const RegionsBase& otherBase) const override {
				assert_true(dynamic_cast<const Regions*>(&otherBase));
				const auto& other = static_cast<const Regions&>(otherBase);
				return regions == other.regions;
			}

			void print(std::ostream& out) const override {
				out << allscale::utils::join(",",regions,[](std::ostream& out, const auto& cur){
					out << cur.first << ":" << cur.second;
				});
			}

			virtual std::unique_ptr<RegionsBase> clone() const override {
				return std::make_unique<Regions>(*this);
			}

			virtual void storeInternal(allscale::utils::ArchiveWriter& out) const override {
				out.write<regions_map_type>(regions);
			}

			static load_res_t load(allscale::utils::ArchiveReader& in) {
				return std::make_pair(
					std::type_index(typeid(DataItem)),
					std::make_unique<Regions>(in.read<regions_map_type>())
				);
			}

			virtual std::unique_ptr<RegionsBase> merge(const RegionsBase& otherBase) const override {
				assert_true(dynamic_cast<const Regions*>(&otherBase));
				const auto& other = static_cast<const Regions&>(otherBase);
				auto res = std::make_unique<Regions>(*this);
				for(const auto& cur : other.regions) {
					res->add(cur.first,cur.second);
				}
				return std::move(res);
			}

			virtual std::unique_ptr<RegionsBase> intersect(const RegionsBase& otherBase) const override {
				assert_true(dynamic_cast<const Regions*>(&otherBase));
				const auto& other = static_cast<const Regions&>(otherBase);

				// compute intersection of sets
				std::map<reference_type,region_type> res;
				for(const auto& cur : regions) {
					auto pos = other.regions.find(cur.first);
					if (pos == other.regions.end()) {
						continue;
					}

					// compute intersection
					auto rest = region_type::intersect(cur.second,pos->second);

					// if nothing => stop
					if (rest.empty()) continue;

					// record rest
					res[cur.first] = rest;
				}

				// if empty => skip result
				if (res.empty()) return {};

				// build result
				return std::make_unique<Regions>(std::move(res));
			}

			virtual std::unique_ptr<RegionsBase> difference(const RegionsBase& otherBase) const override {
				assert_true(dynamic_cast<const Regions*>(&otherBase));
				const auto& other = static_cast<const Regions&>(otherBase);

				// compute differences of sets
				std::map<reference_type,region_type> res;
				for(const auto& cur : regions) {
					auto pos = other.regions.find(cur.first);
					if (pos == other.regions.end()) {
						res[cur.first] = cur.second;
						continue;
					}

					// compute rest
					auto rest = region_type::difference(cur.second,pos->second);

					// if nothing => stop
					if (rest.empty()) continue;

					// record rest
					res[cur.first] = rest;
				}

				// if empty => skip result
				if (res.empty()) return {};

				// build result
				return std::make_unique<Regions>(std::move(res));
			}

		};

		// the index of all regions represented by collection
		std::map<std::type_index,std::unique_ptr<RegionsBase>> regions;

	public:

		// the default constructor should still work
		DataItemRegions() = default;

		// support copy construction
		DataItemRegions(const DataItemRegions& other);

		// also support move semantic
		DataItemRegions(DataItemRegions&&) = default;

		// support copy assignment
		DataItemRegions& operator=(const DataItemRegions&);

		// move assign is also as usual
		DataItemRegions& operator=(DataItemRegions&&) = default;

		// --- observers ---

		/**
		 * Tests whether the regions are empty.
		 */
		bool empty() const;

		/**
		 * Obtains a pointer to the region of an item covered by this region.
		 */
		template<typename DataItem>
		const typename DataItem::region_type* getRegion(const DataItemReference<DataItem>& ref) const {
			auto pos = regions.find(typeid(DataItem));
			if (pos == regions.end()) return nullptr;
			auto& regions = static_cast<const Regions<DataItem>&>(*pos->second).getRegions();
			auto pos2 = regions.find(ref);
			if (pos2 == regions.end()) return nullptr;
			return &pos2->second;
		}


		// --- mutators ---

		/**
		 * Add a region to this list of regions.
		 */
		template<typename DataItem>
		void add(const DataItemReference<DataItem>& ref, const typename DataItem::region_type& region) {
			// do not add empty regions
			if (region.empty()) return;
			getRegionsList<DataItem>().add(ref,region);
		}

		/**
		 * Add a region to this list of regions.
		 */
		template<typename DataItem>
		void add(const DataItemRegion<DataItem>& region) {
			add(region.getDataItemReference(),region.getRegion());
		}


		// -- operators --

		bool operator==(const DataItemRegions&) const;

		bool operator!=(const DataItemRegions& other) const {
			return !(*this == other);
		}

		/**
		 * Applies the given operation on every region of the given type.
		 */
		template<typename DataItem, typename Op>
		void forAll(const Op& op) const {
			auto pos = regions.find(typeid(DataItem));
			if (pos == regions.end()) return;
			for(const auto& cur : static_cast<const Regions<DataItem>&>(*pos->second).getRegions()) {
				op(DataItemRegion<DataItem>(cur.first,cur.second));
			}
		}

		// -- serialization --

		void store(allscale::utils::ArchiveWriter& out) const;

		static DataItemRegions load(allscale::utils::ArchiveReader& in);

		// supports printing regions
		friend std::ostream& operator<<(std::ostream&, const DataItemRegions&);

		// --- set operations ---

		/**
		 * Computes the set union between a and b of regions.
		 */
		friend DataItemRegions merge(const DataItemRegions& a, const DataItemRegions& b);

		/**
		 * Computes the set intersection between a and b of regions.
		 */
		friend DataItemRegions intersect(const DataItemRegions& a, const DataItemRegions& b);

		/**
		 * Computes the set difference between a \ b of regions.
		 */
		friend DataItemRegions difference(const DataItemRegions& a, const DataItemRegions& b);


	private:

		template<typename DataItem>
		Regions<DataItem>& getRegionsList() {
			auto& req_ptr = regions[typeid(DataItem)];
			if (!req_ptr) {
				req_ptr = std::make_unique<Regions<DataItem>>();
			}
			return static_cast<Regions<DataItem>&>(*req_ptr);
		}

		template<typename DataItem>
		const Regions<DataItem>& getRegionsList() const {
			auto pos = regions.find(typeid(DataItem));
			assert_true(pos != regions.end());
			return static_cast<const Regions<DataItem>&>(*pos->second);
		}
	};

	/**
	 * Determines whether a is a subregion of b.
	 */
	bool isSubRegion(const DataItemRegions& a, const DataItemRegions& b);

	/**
	 * Determines whether the two given regions are disjoint.
	 */
	bool isDisjoint(const DataItemRegions& a, const DataItemRegions& b);


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
