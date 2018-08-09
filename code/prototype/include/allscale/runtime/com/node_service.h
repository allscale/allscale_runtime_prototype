/*
 * A utility class for managing node services.
 *
 *  Created on: Jul 23, 2018
 *      Author: herbert
 */

#pragma once

#include <typeinfo>
#include <typeindex>
#include <cstdint>
#include <ostream>
#include <map>
#include <memory>
#include <mutex>
#include <utility>

#include "allscale/utils/assert.h"

namespace allscale {
namespace runtime {
namespace com {

	// forward declaration of nodes
	class Node;

	/**
	 * A registry for node services.
	 */
	class NodeServiceRegistry {

		// A base wrapper for services.
		struct ServiceBase {
			virtual ~ServiceBase() {};
		};

		// A handler for services.
		template<typename S>
		struct Service : public ServiceBase {

			template<typename ... Args>
			Service(Args&& ... args) : service(std::forward<Args>(args)...) {}
			~Service() {};
			S service;
		};

		// the node owning this registry
		Node& node;

		// The service registry.
		std::map<std::type_index,std::unique_ptr<ServiceBase>> services;

		mutable std::mutex lock;

		using guard = std::lock_guard<std::mutex>;

	public:

		/**
		 * Creates a new service registry for the given node.
		 */
		NodeServiceRegistry(Node& node) : node(node) {};

	private:

		template<typename S>
		bool hasServiceInternal() const {
			return services.find(typeid(S)) != services.end();
		}

	public:

		/**
		 * Tests whether the requested service is registered.
		 */
		template<typename S>
		bool hasService() const {
			guard g(lock);
			return hasServiceInternal<S>();
		}

		/**
		 * Starts a given service on the current node and registers it.
		 */
		template<typename S, typename ... Args>
		S& startService(Args&& ... args) {
			guard g(lock);
			// only start service at most once
			if (!hasServiceInternal<S>()) {
				services[typeid(S)] = std::make_unique<Service<S>>(node,std::forward<Args>(args)...);
			}
			return static_cast<Service<S>&>(*services[typeid(S)]).service;
		}

		/**
		 * Obtains access to a registered service.
		 */
		template<typename S>
		S& getService() const {
			guard g(lock);
			assert_true(hasServiceInternal<S>());
			return static_cast<Service<S>&>(*services.find(typeid(S))->second).service;
		}

		/**
		 * Removes the corresponding service from this node.
		 */
		template<typename S>
		void stopService() {
			guard g(lock);
			assert_true(hasServiceInternal<S>());
			services.erase(typeid(S));
		}
	};


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
