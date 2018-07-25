/*
 * The prototype implementation of the data item interface.
 *
 *  Created on: Jul 23, 2018
 *      Author: herbert
 */

#pragma once

#include <map>
#include <memory>
#include <typeinfo>
#include <typeindex>

#include "allscale/utils/assert.h"
#include "allscale/runtime/com/node.h"
#include "allscale/runtime/data/data_item_reference.h"
#include "allscale/runtime/data/data_item_requirement.h"
#include "allscale/runtime/data/data_fragment_manager.h"

namespace allscale {
namespace runtime {
namespace data {

	/**
	 * A common base type of all data item registers.
	 */
	class DataItemRegisterBase {
	public:
		virtual ~DataItemRegisterBase() {};
	};

	/**
	 * A register for data items of a specific type.
	 */
	template<typename DataItem>
	class DataItemRegister : public DataItemRegisterBase {

		using reference_type = DataItemReference<DataItem>;
		using shared_data_type = typename DataItem::shared_data_type;
		using facade_type = typename DataItem::facade_type;

		// the index of registered items
		std::map<DataItemReference<DataItem>,DataFragmentManager<DataItem>> items;

	public:

		DataItemRegister() = default;
		DataItemRegister(const DataItemRegister&) = delete;
		DataItemRegister(DataItemRegister&&) = delete;

		// registers a new data item in the local registry
		void registerItem(const reference_type& ref, const shared_data_type& shared) {
			assert_not_pred1(contains,ref);
			items.emplace(ref,DataFragmentManager<DataItem>(shared));
		}

		// obtains access to a locally managed data item
		facade_type getFacade(const reference_type& ref) {
			return items.find(ref)->second.getDataItem();
		}

	private:

		bool contains(const reference_type& ref) const {
			return items.find(ref) != items.end();
		}

	};

	/**
	 * The service running on each node for handling data items of various types.
	 */
	class DataItemManagerService {

		// the maintained register of data item registers (type specific)
		std::map<std::type_index,std::unique_ptr<DataItemRegisterBase>> registers;

	public:

		DataItemManagerService(com::Node&);


		// a function to retrieve the local instance of this service
		static DataItemManagerService& getLocalService();

		/**
		 * Creates a new data item instance.
		 */
		template<typename DataItem, typename ... Args>
		DataItemReference<DataItem> create(Args&& ... args) {

			using shared_data_t = typename DataItem::shared_data_type;

			// create a new ID
			auto res = DataItemReference<DataItem>::getFresh();

			// create shared data object
			shared_data_t sharedInfo(std::forward<Args>(args)...);

			// register data item in network (also locally)
			com::Node::getNetwork().broadcast(&DataItemManagerService::registerDataItem<DataItem>)(res,sharedInfo);

			// return reference
			return res;
		}


		/**
		 * Obtains access to a data item maintained by this manager.
		 */
		template<typename DataItem>
		typename DataItem::facade_type get(const DataItemReference<DataItem>& ref) {
			return getRegister<DataItem>().getFacade(ref);
		}


		/**
		 * Requests the allocation of the requested data item regions. Blocks until available.
		 */
		void allocate(const DataItemRequirements& reqs);

		/**
		 * Releases the specified data requirements.
		 * TODO: this could also matched to an allocate call through some handler
		 */
		void release(const DataItemRequirements& reqs);


		// --- protocol interface ---

		template<typename DataItem>
		void registerDataItem(DataItemReference<DataItem> ref, const typename DataItem::shared_data_type& shared_data) {
			getRegister<DataItem>().registerItem(ref,shared_data);
		}

	private:

		// obtains the register for a given data item type
		template<typename DataItem>
		DataItemRegister<DataItem>& getRegister() {
			auto pos = registers.find(typeid(DataItem));
			if (pos != registers.end()) {
				return *static_cast<DataItemRegister<DataItem>*>(pos->second.get());
			}
			auto instance = std::make_unique<DataItemRegister<DataItem>>();
			auto& res = *instance;
			registers[typeid(DataItem)] = std::move(instance);
			return res;
		}

	};


	/**
	 * The facade of the data item management.
	 */
	class DataItemManager {

		// a private constructor (just a utility class)
		DataItemManager() {}

	public:

		/**
		 * Creates a new data item instance.
		 */
		template<typename DataItem, typename ... Args>
		static DataItemReference<DataItem> create(Args&& ... args) {
			// forward this call to the local service
			return DataItemManagerService::getLocalService().create<DataItem>(std::forward<Args>(args)...);
		}

		/**
		 * Obtains access to a data item maintained by this manager.
		 */
		template<typename DataItem>
		static typename DataItem::facade_type get(const DataItemReference<DataItem>& ref) {
			// forward this call to the local service
			return DataItemManagerService::getLocalService().get(ref);
		}

	};


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
