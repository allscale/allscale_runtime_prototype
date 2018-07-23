/*
 * node.h
 *
 *  Created on: Jul 23, 2018
 *      Author: herbert
 */

#pragma once

#include <cstdint>
#include <ostream>

namespace allscale {
namespace runtime {
namespace com {

	// The type used for identifying ranks in networks.
	using rank_t = std::uint32_t;

	// forward declaration of the underlying communication network.
	class Network;

	/**
	 * An instance of a node (process) in the AllScale runtime prototype environment.
	 * A node is the entity running once within each process, managing the processes data and tasks.
	 */
	class Node {

		/**
		 * A reference to the network this node belongs to, for communication.
		 */
		Network& network;

		/**
		 * The rank of this node within it's network.
		 */
		rank_t rank;

	public:

		Node(Network& network, rank_t rank) : network(network), rank(rank) {};

		// -------- observer --------

		/**
		 * Obtains the rank of this node.
		 */
		rank_t getRank() const {
			return rank;
		}

		/**
		 * Obtains the network this node is part of.
		 */
		Network& getNetwork() const {
			return network;
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
			// fix rank
			tp_local_rank = getRank();
			// just run this operation on this node
			return op(*this);
		}


		// -------- utilities --------

		/**
		 * Obtains the rank of the node currently processed in this thread.
		 */
		static rank_t getLocalRank() {
			return tp_local_rank;
		}

		/**
		 * Provide print support for debugging.
		 */
		friend std::ostream& operator<<(std::ostream&,const Node&);

	private:

		/**
		 * A thread private value to trace who is currently executing code.
		 */
		static thread_local rank_t tp_local_rank;

	};


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
