/*
 * The prototype implementation of the data item region.
 *
 *  Created on: Jul 26, 2018
 *      Author: herbert
 */

#pragma once

#include <algorithm>
#include <ostream>
#include <unordered_map>
#include <memory>
#include <typeinfo>
#include <typeindex>

#include "allscale/utils/assert.h"
#include "allscale/utils/serializer.h"
#include "allscale/utils/serializer/unordered_maps.h"
#include "allscale/utils/serializer/pairs.h"
#include "allscale/utils/serializer/vectors.h"
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

			// required specialization to be JSON dumpable
			virtual void printJSON(std::ostream& out) const =0;

			virtual bool operator==(const RegionsBase& otherBase) const=0;

			bool operator!=(const RegionsBase& other) const {
				return !(*this == other);
			}

			// provide clone support
			virtual std::unique_ptr<RegionsBase> clone() const=0;

			virtual bool isSubRegion(const RegionsBase& other) const=0;

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
			using regions_list_type = std::vector<std::pair<reference_type,region_type>>;

			regions_list_type regions;

		public:

			Regions() : RegionsBase(&load) {};
			Regions(const Regions&) = default;

			Regions(regions_list_type&& list) : RegionsBase(&load), regions(std::move(list)) {}

			bool empty() const override {
				return regions.empty();
			}

			const regions_list_type& getRegions() const {
				return regions;
			}

			const region_type* getRegion(const reference_type& ref) const {
				auto pos = std::lower_bound(regions.begin(),regions.end(),ref,[](const auto& cur, const reference_type& ref){
					return cur.first < ref;
				});
				return (pos == regions.end() || pos->first != ref) ? nullptr : &(pos->second);
			}

			void add(const reference_type& ref, const region_type& region) {
				assert_false(region.empty());

				// locate insertion position
				auto pos = std::lower_bound(regions.begin(), regions.end(), ref, [](const auto& cur, const reference_type& ref){
					return cur.first < ref;
				});

				// if element is not yet present => add it
				if (pos == regions.end()) {
					regions.emplace_back(ref,region);
					return;
				}

				// if referenced data item is present => merge regions
				if (pos->first == ref) {
					pos->second = region_type::merge(pos->second,region);
					return;
				}

				// insert new element at located position
				regions.emplace(pos,ref,region);
			}

			bool operator==(const RegionsBase& otherBase) const override {
				assert_true(dynamic_cast<const Regions*>(&otherBase));
				const auto& other = static_cast<const Regions&>(otherBase);

				// check identity
				if (this == &other) return true;

				// start with size
				if (regions.size() != other.regions.size()) return false;

				// first compare data item ids
				int size = regions.size();
				for(int i=0; i<size; i++) {
					if (regions[i].first != other.regions[i].first) return false;
				}

				// in a second run, check the regions
				for(int i=0; i<size; i++) {
					if (regions[i].second != other.regions[i].second) return false;
				}

				// all checks out, they are the same
				return true;
			}

			void print(std::ostream& out) const override {
				out << allscale::utils::join(",",regions,[](std::ostream& out, const auto& cur){
					out << cur.first << ":" << cur.second;
				});
			}

			void printJSON(std::ostream& out) const override {
				using allscale::utils::join;

				// TODO: support this for non-grid data items

				out << join(",",regions,[](std::ostream& out, const auto& cur){
					out << "{";
					out << "\"id\" : " << cur.first.getID() << ",";
					out << "\"type\" : \"" << region_type::Dimensions << "D-Grid\",";
					out << "\"region\" : [";

					out << join(",",cur.second.getBoxes(),[](std::ostream& out, const auto& cur){
						out << "{";
						out << "\"from\" : " << cur.getMin() << ",";
						out << "\"to\" : " << cur.getMax();
						out << "}";
					});

					out << "]";
					out << "}";
				});
			}

			virtual std::unique_ptr<RegionsBase> clone() const override {
				return std::make_unique<Regions>(*this);
			}

			virtual void storeInternal(allscale::utils::ArchiveWriter& out) const override {
				out.write<regions_list_type>(regions);
			}

			static load_res_t load(allscale::utils::ArchiveReader& in) {
				return std::make_pair(
					std::type_index(typeid(DataItem)),
					std::make_unique<Regions>(in.read<regions_list_type>())
				);
			}

			virtual bool isSubRegion(const RegionsBase& otherBase) const override {
				assert_true(dynamic_cast<const Regions*>(&otherBase));
				const auto& other = static_cast<const Regions&>(otherBase);

				// check identity
				if (this == &other) return true;

				// start with size
				if (regions.size() > other.regions.size()) return false;

				// check keys next
				bool allKeysPresent = std::includes(
					other.regions.begin(), other.regions.end(),
					regions.begin(), regions.end(),
					[](const auto& a, const auto& b) { return a.first < b.first; }
				);
				if (!allKeysPresent) return false;


				// check individual regions
				auto ap = regions.begin();
				auto ae = regions.end();
				auto bp = other.regions.begin();

				while(ap != ae) {

					// move bp to the position of ap
					while (bp->first < ap->first) {
						++bp;
						assert_true(bp != other.regions.end());
					}

					// check the actual associated regions
					if (!region_type::isSubRegion(ap->second,bp->second)) {
						return false;
					}

					// go to next
					++ap; ++bp;
				}

				// all checks out
				return true;
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
				regions_list_type res;

				// get pointer on involved regions
				auto ap = regions.begin();
				auto ae = regions.end();
				auto bp = other.regions.begin();
				auto be = other.regions.end();

				while(true) {

					if (bp == be) break;

					// move a while key is less than b's
					while(ap != ae && ap->first < bp->first) {
						++ap;
					}

					if (ap == ae) break;

					// move b while key is less than a's
					while(bp != be && bp->first < ap->first) {
						++bp;
					}

					if (bp == be) break;

					if (ap->first == bp->first) {
						// compute region intersection
						auto rest = region_type::intersect(ap->second,bp->second);
						// add to result if not empty
						if (!rest.empty()) res.emplace_back(ap->first,std::move(rest));

						++ap;
						++bp;
					}

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
				regions_list_type res;

				// get pointer on b
				auto bp = other.regions.begin();
				auto be = other.regions.end();

				for(const auto& cur : regions) {

					// move on b-pointer
					while(bp != be && bp->first < cur.first) {
						++bp;
					}

					// compute intersection
					if (bp != be && bp->first == cur.first) {
						// compute difference
						auto rest = region_type::difference(cur.second,bp->second);
						// integrate it if not empty
						if (!rest.empty()) res.emplace_back(cur.first,std::move(rest));
					} else {
						res.emplace_back(cur.first,cur.second);
					}
				}

				// if empty => skip result
				if (res.empty()) return {};

				// build result
				return std::make_unique<Regions>(std::move(res));
			}

		};

		// the index of all regions represented by collection
		std::unordered_map<std::type_index,std::unique_ptr<RegionsBase>> regions;

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
			return static_cast<const Regions<DataItem>&>(*pos->second).getRegion(ref);
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

		/**
		 * Add a set of regions.
		 */
		void add(const DataItemRegions& regions);

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
				op(cur.first,cur.second);
			}
		}

		// -- serialization --

		void store(allscale::utils::ArchiveWriter& out) const;

		static DataItemRegions load(allscale::utils::ArchiveReader& in);

		// supports printing regions
		friend std::ostream& operator<<(std::ostream&, const DataItemRegions&);

		// support printing in JSON format
		void toJSON(std::ostream& out) const;


		// --- set operations ---

		/**
		 * Computes the set union between a and b of regions.
		 */
		friend bool isSubRegion(const DataItemRegions& a, const DataItemRegions& b);

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
