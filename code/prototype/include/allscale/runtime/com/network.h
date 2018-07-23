/*
 * network.h
 *
 *  Created on: Jul 23, 2018
 *      Author: herbert
 */

#pragma once

#include <ostream>
#include <utility>
#include <vector>

#include "allscale/utils/assert.h"
#include "allscale/utils/serializer.h"

#include "allscale/runtime/com/node.h"

namespace allscale {
namespace runtime {
namespace com {

	// The type used for the size of networks.
	using size_t = rank_t;

	/**
	 * The simulated network for this prototype implementation.
	 *
	 * The network class manages a set of nodes and keeps statistics on the number of incoming and outgoing
	 * messages. It also ensures that all transfered data is serializable.
	 */
	class Network {

		/**
		 * The list of nodes on the network.
		 * To ensure that there are no side channels, this list of nodes is never exposed.
		 */
		std::vector<Node> nodes;

	public:

		/**
		 * Data and message transfer statistics in this network.
		 */
		class Statistics {
		public:

			/**
			 * The statistics entry per node.
			 */
			struct Entry {

				std::uint64_t sent_bytes = 0;			// < number of send bytes
				std::uint64_t received_bytes = 0;		// < number of received bytes
				std::uint32_t sent_calls = 0;	  		// < number of send calls
				std::uint32_t received_calls = 0;		// < number or received calls

				friend std::ostream& operator<<(std::ostream&,const Entry&);

			};

		private:

			/**
			 * The per-node statistics data represented.
			 */
			std::vector<Entry> stats;

		public:

			/**
			 * Creates a new statics for the given number of nodes.
			 */
			Statistics(size_t size) : stats(size) {}

			/**
			 * Obtain the statistics of some node.
			 */
			Entry& operator[](rank_t rank) {
				assert_lt(rank, stats.size());
				return stats[rank];
			}

			/**
			 * Obtain the statistics of some node.
			 */
			const Entry& operator[](rank_t rank) const {
				assert_lt(rank, stats.size());
				return stats[rank];
			}

			/**
			 * Support printing of statistics.
			 */
			friend std::ostream& operator<<(std::ostream&,const Statistics&);

		};

		/**
		 * A handle for remote node references.
		 */
		class RemoteNode {

			// the targeted node
			Node& node;

			// the statistics to work with
			Statistics& stats;

		public:

			/**
			 * Creates a new remote reference to the provided node.
			 */
			RemoteNode(Node& node, Statistics& stats) : node(node), stats(stats) {}

			/**
			 * The universal interface for remote procedure calls to other nodes.
			 */
			template<typename R, typename ... Args>
			R call(R(Node::*fun)(Args...), Args... args) const {
				auto src = Node::getLocalRank();
				auto trg = node.getRank();
				stats[src].sent_calls += 1;
				stats[trg].received_calls += 1;
				return transfer(trg,src,(node.*fun)(transfer(src,trg,args)...));
			}

			/**
			 * The universal interface for remote procedure calls to other nodes (const methods).
			 */
			template<typename R, typename ... Args>
			R call(R(Node::*fun)(Args...) const, Args... args) const {
				auto src = Node::getLocalRank();
				auto trg = node.getRank();
				stats[src].sent_calls += 1;
				stats[trg].received_calls += 1;
				return transfer(trg,src,(node.*fun)(transfer(src,trg,args)...));
			}

		private:

			/**
			 * The function to simulate the transfer of data.
			 */
			template<typename T>
			T transfer(rank_t src, rank_t trg, const T& value) const {
				static_assert(allscale::utils::is_serializable<T>::value, "Encountered non-serializable data element.");

				// perform serialization
				auto archive = allscale::utils::serialize(value);

				// record transfer volume
				auto size = archive.getBuffer().size();
				stats[src].sent_bytes += size;
				stats[trg].received_bytes += size;

				// de-serialize value
				auto res = allscale::utils::deserialize<T>(archive);

				// make sure serialization is correct
				assert_eq(value,res);

				// done (avoid extra copy)
				return std::move(res);
			}

			/**
			 * A special case for transferring archived data (no extra serialization needed).
			 */
			const allscale::utils::Archive& transfer(rank_t src, rank_t trg, const allscale::utils::Archive& a) {

				// record transfer volume
				auto size = a.getBuffer().size();
				stats[src].sent_bytes += size;
				stats[trg].received_bytes += size;

				// nothing else to do here
				return a;
			}

		};

	private:

		/**
		 * The statistics recorded for this network.
		 */
		Statistics stats;

	public:

		/**
		 * Creates a network of the given size.
		 */
		Network(size_t size = 1);

		// prohibit copying
		Network(const Network&) = delete;

		// but allow moving
		Network(Network&&) = default;

		/**
		 * Obtains the number
		 */
		size_t numNodes() const {
			return nodes.size();
		}

		// -------- remote procedure calls --------

		/**
		 * Obtains a remote reference to a node of this network.
		 */
		RemoteNode getNode(rank_t rank) {
			assert_lt(rank,numNodes());
			return { nodes[rank], stats };
		}

		// -------- development and debugging interface --------

		/**
		 * Triggers the given operation to be processed on the selected node. This operation
		 * is intended for test cases to initiate operations to be performed on selected nodes.
		 */
		template<typename Op>
		auto runOn(rank_t rank, const Op& op) -> decltype(op(std::declval<Node&>())) {
			assert_lt(rank,numNodes());
			return nodes[rank].run(op);
		}

		/**
		 * Obtains the network transfer statistics collected so far.
		 */
		const Statistics& getStatistics() const {
			return stats;
		}

		/**
		 * Resets the statistics collected so far.
		 */
		void resetStatistics() {
			stats = Statistics(numNodes());
		}

		// -------- utilities --------

		/**
		 * Provide print support for debugging.
		 */
		friend std::ostream& operator<<(std::ostream&,const Node&);

	};


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
