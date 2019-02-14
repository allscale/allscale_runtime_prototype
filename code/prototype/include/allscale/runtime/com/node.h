/*
 * node.h
 *
 *  Created on: Jul 23, 2018
 *      Author: herbert
 */

#pragma once

#include <cstdint>
#include <ostream>

#include "allscale/utils/assert.h"
#include "allscale/utils/finalize.h"
#include "allscale/utils/fibers.h"

#include "allscale/runtime/com/node_service.h"

namespace allscale {
namespace runtime {

// forward declaration
namespace work { class Task; }

namespace com {

	// The type used for identifying ranks in networks.
	using rank_t = std::uint32_t;

	/**
	 * An instance of a node (process) in the AllScale runtime prototype environment.
	 * A node is the entity running once within each process, managing the processes data and tasks.
	 */
	class Node {

		/**
		 * The rank of this node within it's network.
		 */
		rank_t rank;

		/**
		 * A context for performing fiber operations on this node.
		 * NOTE: this could be a service, but it is essential for the implementation of
		 * network communication and thus always required. A more direct access is thus
		 * granted and cyclic dependencies avoided.
		 */
		allscale::utils::FiberContext context;

		/**
		 * A register of services running on this node.
		 */
		NodeServiceRegistry services;

	public:

		Node(rank_t rank) : rank(rank), services(*this) {};

		// -------- observer --------

		/**
		 * Obtains the rank of this node.
		 */
		rank_t getRank() const {
			return rank;
		}

		/**
		 * Provides access to the fiber context maintained for concurrent operations
		 * on this node.
		 */
		allscale::utils::FiberContext& getFiberContext() {
			return context;
		}

		/**
		 * Starts a new service on this node.
		 */
		template<typename S, typename ... Args>
		S& startService(Args&& ... args) {
			return services.startService<S>(std::forward<Args>(args)...);
		}

		/**
		 * Tests whether a given service is installed on this node.
		 */
		template<typename S>
		bool hasService() const {
			return services.hasService<S>();
		}

		/**
		 * Obtains access to a selected service running on this node.
		 */
		template<typename S>
		S& getService() const {
			return services.getService<S>();
		}

		/**
		 * Removes the given service from this node.
		 */
		template<typename S>
		void stopService() {
			services.stopService<S>();
		}

		// -------- protocol --------

		/**
		 * A simple test case responding with x + 1
		 */
		int ping(int x = 0) const;


		// -------- development and debugging interface --------

		/**
		 * A general interface to run operations "within" this node.
		 */
		template<typename Op>
		auto run(const Op& op) -> decltype(op(*this)) {
			// fix the local node
			auto old = getLocalNodeInternal();
			setLocalNode(this);

			// ensure recovery after execution
			auto _ = allscale::utils::run_finally([&]{
				// make sure the local node is still the same
				assert_eq(getLocalNodeInternal(),this);
				// reset to old one
				setLocalNode(old);
			});

			// run this operation on this node
			return op(*this);
		}


		// -------- utilities --------

		/**
		 * Obtains a reference to the local node instance.
		 */
		static Node& getLocalNode();

		/**
		 * Obtains the fiber context active within this node.
		 */
		allscale::utils::FiberContext& getLocalFiberContext() {
			return getLocalNode().getFiberContext();
		}

		/**
		 * Obtains a reference to a locally running service instance.
		 */
		template<typename S>
		static S& getLocalService() {
			return getLocalNode().getService<S>();
		}

		/**
		 * Obtains the rank of the node currently processed in this thread.
		 */
		static rank_t getLocalRank() {
			return getLocalNode().getRank();
		}

		/**
		 * Provide print support for debugging.
		 */
		friend std::ostream& operator<<(std::ostream&,const Node&);

	private:

		/**
		 * A utility to internally obtain the currently configured local node
		 * for the current thread, or null, if there is none.
		 */
		static Node* getLocalNodeInternal();

		/**
		 * A utility to update the local node.
		 */
		static void setLocalNode(Node* node);

	};


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
