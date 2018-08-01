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
#include "allscale/runtime/com/node_service.h"

namespace allscale {
namespace runtime {
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
		 * Starts a new service on this node.
		 */
		template<typename S, typename ... Args>
		S& startService(Args&& ... args) {
			return services.startService<S>(std::forward<Args>(args)...);
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
			auto old = tp_local_node;
			tp_local_node = this;

			// ensure recovery after execution
			auto _ = allscale::utils::run_finally([&]{ tp_local_node = old; });

			// run this operation on this node
			return op(*this);
		}


		// -------- utilities --------

		/**
		 * Obtains a reference to the local node instance.
		 */
		static Node& getLocalNode() {
			assert_true(tp_local_node) << "Not processed within a node!";
			return *tp_local_node;
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
		 * A thread private value to trace who is currently executing code.
		 */
		static thread_local Node* tp_local_node;

	};


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
