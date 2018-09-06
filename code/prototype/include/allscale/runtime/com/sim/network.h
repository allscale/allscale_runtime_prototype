/*
 * A shared-memory prototype implementation of the network interface.
 *
 *  Created on: Jul 23, 2018
 *      Author: herbert
 */

#pragma once

#include <memory>
#include <ostream>
#include <utility>
#include <vector>

#include "allscale/utils/assert.h"
#include "allscale/utils/serializer.h"

#include "allscale/runtime/com/node.h"
#include "allscale/runtime/com/statistics.h"

namespace allscale {
namespace runtime {
namespace com {
namespace sim {

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
		std::vector<std::unique_ptr<Node>> nodes;

	private:


		/**
		 * The function to simulate the transfer of data.
		 */
		template<typename T>
		static T transfer(NodeStatistics& srcStats, NodeStatistics& trgStats, const T& value) {
			static_assert(allscale::utils::is_serializable<T>::value, "Encountered non-serializable data element.");

			// shortcut for local communication
			if (&srcStats == &trgStats) return value;

			// perform serialization
			auto archive = allscale::utils::serialize(value);

			// record transfer volume
			auto size = archive.getBuffer().size();
			srcStats.sent_bytes += size;
			trgStats.received_bytes += size;

			// de-serialize value
			auto res = allscale::utils::deserialize<T>(archive);

			// done (avoid extra copy)
			return std::move(res);
		}

		/**
		 * A special case for transferring archived data (no extra serialization needed).
		 */
		static const allscale::utils::Archive& transfer(NodeStatistics& srcStats, NodeStatistics& trgStats, const allscale::utils::Archive& a) {

			// shortcut for local communication
			if (&srcStats == &trgStats) return a;

			// record transfer volume
			auto size = a.getBuffer().size();
			srcStats.sent_bytes += size;
			trgStats.received_bytes += size;

			// nothing else to do here
			return a;
		}


	public:


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
			NodeStatistics& srcStats;
			NodeStatistics& trgStats;

		public:

			/**
			 * Creates a new remote procedure reference.
			 */
			RemoteProcedure(Node& node, const Selector& selector, R(S::*fun)(Args...), NodeStatistics& srcStats, NodeStatistics& trgStats)
				: node(node), selector(selector), fun(fun), srcStats(srcStats), trgStats(trgStats) {}

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

				// update statistics
				srcStats.sent_calls += 1;
				trgStats.received_calls += 1;

				// perform an actual remote call
				return node.run([&](Node&){
					return transfer(trgStats,srcStats,(selector(node).*fun)(transfer(srcStats,trgStats,std::forward<Args>(args))...));
				});
			}

		};

		/**
		 * A handle for remote procedures.
		 */
		template<typename Selector, typename S, typename ... Args>
		class RemoteProcedure<Selector,S,void,Args...> {

			// the targeted node
			Node& node;

			// the service selector
			Selector selector;

			// the targeted service function
			void(S::* fun)(Args...);

			// the statistics to work with
			NodeStatistics& srcStats;
			NodeStatistics& trgStats;

		public:

			/**
			 * Creates a new remote procedure reference.
			 */
			RemoteProcedure(Node& node, const Selector& selector, void(S::*fun)(Args...), NodeStatistics& srcStats, NodeStatistics& trgStats)
				: node(node), selector(selector), fun(fun), srcStats(srcStats), trgStats(trgStats) {}

			/**
			 * Realizes the actual remote procedure call.
			 */
			void operator()(Args ... args) const {
				auto src = Node::getLocalRank();
				auto trg = node.getRank();

				// short-cut for local communication
				if (src == trg) {
					node.run([&](Node&){
						(selector(node).*fun)(std::forward<Args>(args)...);
					});
					return;
				}

				// perform an actual remote call
				srcStats.sent_calls += 1;
				trgStats.received_calls += 1;
				node.run([&](Node&){
					(selector(node).*fun)(transfer(srcStats,trgStats,std::forward<Args>(args))...);
				});
			}

		};


		/**
		 * A handle for remote procedures for const member functions.
		 */
		template<typename Selector, typename S, typename R, typename ... Args>
		class RemoteConstProcedure {

			// the targeted node
			Node& node;

			// the service selector
			Selector selector;

			// the targeted service function
			R(S::* fun)(Args...) const;

			// the statistics to work with
			NodeStatistics& srcStats;
			NodeStatistics& trgStats;

		public:

			/**
			 * Creates a new remote procedure reference.
			 */
			RemoteConstProcedure(Node& node, const Selector& selector, R(S::*fun)(Args...) const, NodeStatistics& srcStats, NodeStatistics& trgStats)
				: node(node), selector(selector), fun(fun), srcStats(srcStats), trgStats(trgStats) {}

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
				srcStats.sent_calls += 1;
				trgStats.received_calls += 1;
				return node.run([&](Node&){
					return transfer(trgStats,srcStats,(selector(node).*fun)(transfer(srcStats,trgStats,std::forward<Args>(args))...));
				});
			}

		};


		/**
		 * A handle for broadcasts.
		 */
		template<typename S, typename ... Args>
		class Broadcast {

			// the nodes to address
			std::vector<std::unique_ptr<Node>>& nodes;

			// the targeted service function
			void(S::* fun)(Args...);

		public:

			/**
			 * Creates a new remote procedure reference.
			 */
			Broadcast(std::vector<std::unique_ptr<Node>>& nodes, void(S::*fun)(Args...))
				: nodes(nodes), fun(fun) {}

			/**
			 * Realizes the actual broadcast.
			 */
			void operator()(Args ... args) const {
				auto src = Node::getLocalRank();
				auto& srcStats = nodes[src]->template getService<NetworkStatisticService>().getLocalNodeStats();
				srcStats.sent_bcasts += 1;
				for(auto& node : nodes) {
					auto trg = node->getRank();

					// short-cut for local communication
					if (src == trg) {
						node->run([&](Node&){
							(node->template getService<S>().*fun)(std::forward<Args>(args)...);
						});
						continue;
					}

					// perform remote call
					auto& trgStats = nodes[trg]->template getService<NetworkStatisticService>().getLocalNodeStats();
					trgStats.received_bcasts += 1;
					node->run([&](Node&){
						(node->template getService<S>().*fun)(transfer(srcStats,trgStats,std::forward<Args>(args))...);
					});

				}
			}

		};

		/**
		 * Creates a network of the given size.
		 */
		Network(size_t size = 1);

		// prohibit copying
		Network(const Network&) = delete;

		// but allow moving
		Network(Network&&) = delete;

		// factory method for a new network, using an externally determined number of nodes
		static std::unique_ptr<Network> create();

		// factory creating the given number of nodes, or null if not possible (always possible)
		static std::unique_ptr<Network> create(size_t size);

		/**
		 * Obtains the number
		 */
		size_t numNodes() const {
			return nodes.size();
		}

		// obtains the enclosing network instance
		static Network& getNetwork();

		// -------- remote procedure calls --------

		/**
		 * Obtains a handle for performing a remote procedure call of a selected service.
		 */
		template<typename Selector, typename S, typename R, typename ... Args>
		RemoteProcedure<Selector,S,R,Args...> getRemoteProcedure(rank_t rank, const Selector& selector, R(S::*fun)(Args...)) {
			assert_lt(rank,nodes.size());
			return { *(nodes[rank]), selector, fun, getNodeStats(Node::getLocalRank()), getNodeStats(rank) };
		}

		/**
		 * Obtains a handle for performing a remote procedure call of a selected service.
		 */
		template<typename Selector, typename S, typename R, typename ... Args>
		RemoteConstProcedure<Selector,S,R,Args...> getRemoteProcedure(rank_t rank, const Selector& selector, R(S::*fun)(Args...) const) {
			assert_lt(rank,nodes.size());
			return { *(nodes[rank]), selector, fun, getNodeStats(Node::getLocalRank()), getNodeStats(rank) };
		}

		/**
		 * Obtains a handle for performing a remote procedure call of a selected service.
		 */
		template<typename S, typename R, typename ... Args>
		auto getRemoteProcedure(rank_t rank, R(S::*fun)(Args...)) {
			assert_lt(rank,nodes.size());
			return getRemoteProcedure(rank,direct_selector<S>(),fun);
		}

		/**
		 * Obtains a handle for performing a remote procedure call of a selected service.
		 */
		template<typename S, typename R, typename ... Args>
		auto getRemoteProcedure(rank_t rank, R(S::*fun)(Args...) const) {
			assert_lt(rank,nodes.size());
			return getRemoteProcedure(rank,direct_selector<S>(),fun);
		}

		/**
		 * Obtains a handle for performing broad-casts on a selected remote service.
		 */
		template<typename S, typename ... Args>
		Broadcast<S,Args...> broadcast(void(S::*fun)(Args...)) {
			return { nodes, fun };
		}

		// -------- development and debugging interface --------

		/**
		 * Triggers the given operation to be processed on the selected node. This operation
		 * is intended for test cases to initiate operations to be performed on selected nodes.
		 */
		template<typename Op>
		auto runOn(rank_t rank, const Op& op) -> decltype(op(std::declval<Node&>())) {
			assert_lt(rank,numNodes());
			setLocalNetwork();
			auto f = allscale::utils::run_finally([&]{ resetLocalNetwork(); });
			return nodes[rank]->run(op);
		}

		/**
		 * Runs the given operation within each node of the network.
		 */
		template<typename Op>
		void runOnAll(const Op& op) {
			setLocalNetwork();
			for(auto& n : nodes) {
				n->run(op);
			}
			resetLocalNetwork();
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
		 * Removes a service from all nodes.
		 */
		template<typename S>
		void removeServiceOnNodes() {
			runOnAll([&](Node& n){
				n.stopService<S>();
			});
		}

		/**
		 * Get all nodes to the same state.
		 */
		void sync() { /* it's a no-op in this network implementatio */ }

		/**
		 * Obtains the network transfer statistics collected so far.
		 */
		NetworkStatistics getStatistics();

		/**
		 * Resets the statistics collected so far.
		 */
		void resetStatistics();

		// -------- utilities --------

		/**
		 * Provide print support for debugging.
		 */
		friend std::ostream& operator<<(std::ostream&,const Node&);

	private:

		void setLocalNetwork() const;

		void resetLocalNetwork() const;

		NodeStatistics& getNodeStats(rank_t rank);

	};

} // end of namespace sim
} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
