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

				std::uint64_t sent_bytes = 0;			// < number of sent bytes
				std::uint64_t received_bytes = 0;		// < number of received bytes
				std::uint32_t sent_calls = 0;	  		// < number of sent calls
				std::uint32_t received_calls = 0;		// < number or received calls
				std::uint32_t sent_bcasts = 0;			// < number of sent broadcasts
				std::uint32_t received_bcasts = 0;		// < number of received broadcasts

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
			 * The function to simulate the transfer of data.
			 */
			template<typename T>
			T transfer(rank_t src, rank_t trg, const T& value) {
				static_assert(allscale::utils::is_serializable<T>::value, "Encountered non-serializable data element.");

				// shortcut for local communication
				if (src == trg) return value;

				// perform serialization
				auto archive = allscale::utils::serialize(value);

				// record transfer volume
				auto size = archive.getBuffer().size();
				(*this)[src].sent_bytes += size;
				(*this)[trg].received_bytes += size;

				// de-serialize value
				auto res = allscale::utils::deserialize<T>(archive);

				// done (avoid extra copy)
				return std::move(res);
			}

			/**
			 * A special case for transferring archived data (no extra serialization needed).
			 */
			const allscale::utils::Archive& transfer(rank_t src, rank_t trg, const allscale::utils::Archive& a) {

				// shortcut for local communication
				if (src == trg) return a;

				// record transfer volume
				auto size = a.getBuffer().size();
				(*this)[src].sent_bytes += size;
				(*this)[trg].received_bytes += size;

				// nothing else to do here
				return a;
			}

			/**
			 * Support printing of statistics.
			 */
			friend std::ostream& operator<<(std::ostream&,const Statistics&);

		};

		/**
		 * A default service selector.
		 */
		template<typename S>
		struct direct_selector {
			S& operator()(Node& node) const {
				return node.getService<S>();
			}
		};

		/**
		 * A handle for remote procedures.
		 */
		template<typename Selector, typename S, typename R, typename ... Args>
		class RemoteProcedure {

			// the targeted node
			Node& node;

			// the service selector
			Selector selector;

			// the targeted service function
			R(S::* fun)(Args...);

			// the statistics to work with
			Statistics& stats;

		public:

			/**
			 * Creates a new remote procedure reference.
			 */
			RemoteProcedure(Node& node, const Selector& selector, R(S::*fun)(Args...), Statistics& stats)
				: node(node), selector(selector), fun(fun), stats(stats) {}

			/**
			 * Realizes the actual remote procedure call.
			 */
			R operator()(Args ... args) const {
				auto src = Node::getLocalRank();
				auto trg = node.getRank();

				// short-cut for local communication
				if (src == trg) {
					return node.run([&](Node&){
						return (selector(node).*fun)(std::forward<Args>(args)...);
					});
				}

				// perform an actual remote call
				stats[src].sent_calls += 1;
				stats[trg].received_calls += 1;
				return node.run([&](Node&){
					return stats.transfer(trg,src,(selector(node).*fun)(stats.transfer(src,trg,std::forward<Args>(args))...));
				});
			}

		};

		/**
		 * A handle for broadcasts.
		 */
		template<typename S, typename ... Args>
		class Broadcast {

			// the nodes to address
			std::vector<Node>& nodes;

			// the targeted service function
			void(S::* fun)(Args...);

			// the statistics to work with
			Statistics& stats;

		public:

			/**
			 * Creates a new remote procedure reference.
			 */
			Broadcast(std::vector<Node>& nodes, void(S::*fun)(Args...), Statistics& stats)
				: nodes(nodes), fun(fun), stats(stats) {}

			/**
			 * Realizes the actual broadcast.
			 */
			void operator()(Args ... args) const {
				auto src = Node::getLocalRank();
				stats[src].sent_bcasts += 1;
				for(auto& node : nodes) {
					auto trg = node.getRank();

					// short-cut for local communication
					if (src == trg) {
						node.run([&](Node&){
							(node.getService<S>().*fun)(std::forward<Args>(args)...);
						});
						continue;
					}

					// perform remote call
					stats[trg].received_bcasts += 1;
					node.run([&](Node&){
						(node.getService<S>().*fun)(stats.transfer(src,trg,std::forward<Args>(args))...);
					});

				}
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
		 * Obtains a handle for performing a remote procedure call of a selected service.
		 */
		template<typename Selector, typename S, typename R, typename ... Args>
		RemoteProcedure<Selector,S,R,Args...> getRemoteProcedure(rank_t rank, const Selector& selector, R(S::*fun)(Args...)) {
			return { nodes[rank], selector, fun, stats };
		}


		/**
		 * Obtains a handle for performing a remote procedure call of a selected service.
		 */
		template<typename S, typename R, typename ... Args>
		auto getRemoteProcedure(rank_t rank, R(S::*fun)(Args...)) {
			return getRemoteProcedure(rank,direct_selector<S>(),fun);
		}

		/**
		 * Obtains a handle for performing broad-casts on a selected remote service.
		 */
		template<typename S, typename ... Args>
		Broadcast<S,Args...> broadcast(void(S::*fun)(Args...)) {
			return { nodes, fun, stats };
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
		 * Runs the given operation within each node of the network.
		 */
		template<typename Op>
		void runOnAll(const Op& op) {
			for(Node& n : nodes) {
				n.run(op);
			}
		}

		/**
		 * Installs a service on all nodes.
		 */
		template<typename S, typename ... Args>
		void installServiceOnNodes(const Args& ... args) {
			runOnAll([&](Node& n){
				n.startService<S>(args...);
			});
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
