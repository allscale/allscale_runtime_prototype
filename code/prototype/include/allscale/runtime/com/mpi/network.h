/*
 * An MPI based implementation of the network interface.
 *
 *  Created on: Aug 1, 2018
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

namespace allscale {
namespace runtime {
namespace com {
namespace mpi {


	/**
	 * The network implementation for an MPI based implementation.
	 */
	class Network {

	public:

		/**
		 * Data and message transfer statistics in this network.
		 */
		class Statistics {
		public:

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
			rank_t target;

			// the service selector
			Selector selector;

			// the targeted service function
			R(S::* fun)(Args...);

		public:

			/**
			 * Creates a new remote procedure reference.
			 */
			RemoteProcedure(rank_t target, const Selector& selector, R(S::*fun)(Args...))
				: target(target), selector(selector), fun(fun) {}

			/**
			 * Realizes the actual remote procedure call.
			 */
			R operator()(Args ... args) const {
				auto src = Node::getLocalRank();
				auto trg = target;

				// short-cut for local communication
				if (src == trg) {
					auto& node = com::Node::getLocalNode();
					return (selector(node).*fun)(std::forward<Args>(args)...);
				}

				// the rest is not supported yet
				assert_not_implemented();

				Node& node = *(Node*)nullptr;
				return (selector(node).*fun)(std::forward<Args>(args)...);

//				// short-cut for local communication
//				if (src == trg) {
//					return node.run([&](Node&){
//						return (selector(node).*fun)(std::forward<Args>(args)...);
//					});
//				}


//				// perform an actual remote call
//				return node.run([&](Node&){
//					return stats.transfer(trg,src,(selector(node).*fun)(stats.transfer(src,trg,std::forward<Args>(args))...));
//				});
			}

		};

		/**
		 * A handle for remote procedures.
		 */
		template<typename Selector, typename S, typename ... Args>
		class RemoteProcedure<Selector,S,void,Args...> {

			// the targeted node
			rank_t target;

			// the service selector
			Selector selector;

			// the targeted service function
			void(S::* fun)(Args...);

		public:

			/**
			 * Creates a new remote procedure reference.
			 */
			RemoteProcedure(rank_t rank, const Selector& selector, void(S::*fun)(Args...))
				: target(rank), selector(selector), fun(fun) {}

			/**
			 * Realizes the actual remote procedure call.
			 */
			void operator()(Args ... args) const {
				auto src = Node::getLocalRank();
				auto trg = target;

				// short-cut for local communication
				if (src == trg) {
					auto& node = com::Node::getLocalNode();
					(selector(node).*fun)(std::forward<Args>(args)...);
					return;
				}

				// the rest is not supported yet
				assert_not_implemented();

//				Node& node = *(Node*)nullptr;
//				return (selector(node).*fun)(std::forward<Args>(args)...);

			}

		};

		/**
		 * A handle for broadcasts.
		 */
		template<typename S, typename ... Args>
		class Broadcast {

			// the targeted service function
			void(S::* fun)(Args...);

		public:

			/**
			 * Creates a new remote procedure reference.
			 */
			Broadcast(void(S::*fun)(Args...))
				: fun(fun) {}

			/**
			 * Realizes the actual broadcast.
			 */
			void operator()(Args ... args) const {

				// send one-by-one to each other node
				auto& net = Network::getNetwork();
				for(rank_t r = 0; r<net.numNodes(); r++) {
					net.getRemoteProcedure(r,fun)(args...);
				}

//				auto src = Node::getLocalRank();
//				stats[src].sent_bcasts += 1;
//				for(auto& node : nodes) {
//					auto trg = node.getRank();
//
//					// short-cut for local communication
//					if (src == trg) {
//						node.run([&](Node&){
//							(node.getService<S>().*fun)(std::forward<Args>(args)...);
//						});
//						continue;
//					}
//
//					// perform remote call
//					stats[trg].received_bcasts += 1;
//					node.run([&](Node&){
//						(node.getService<S>().*fun)(stats.transfer(src,trg,std::forward<Args>(args))...);
//					});
//
//				}
			}

		};

	private:

		/**
		 * The statistics recorded for this network.
		 */
		Statistics stats;

		// number of nodes in this network
		size_t num_nodes;

		// the local node, initialized during startup
		std::unique_ptr<Node> localNode;

		// the singleton instance
		static Network instance;

		Network();

		~Network();

	public:

		// prohibit copying
		Network(const Network&) = delete;

		// but allow moving
		Network(Network&&) = delete;

		// factory method obtaining the global network
		static Network* create();

		// factory creating the given number of nodes, or null if not possible (only possible if started with a matching size)
		static Network* create(size_t size);

		// obtains a reference to the currently active network
		static Network& getNetwork();

		/**
		 * Obtains the number
		 */
		size_t numNodes() const {
			return num_nodes;
		}

		// -------- remote procedure calls --------

		/**
		 * Obtains a handle for performing a remote procedure call of a selected service.
		 */
		template<typename Selector, typename S, typename R, typename ... Args>
		RemoteProcedure<Selector,S,R,Args...> getRemoteProcedure(rank_t rank, const Selector& selector, R(S::*fun)(Args...)) {
			assert_lt(rank,numNodes());
			return { rank, selector, fun };
		}


		/**
		 * Obtains a handle for performing a remote procedure call of a selected service.
		 */
		template<typename S, typename R, typename ... Args>
		auto getRemoteProcedure(rank_t rank, R(S::*fun)(Args...)) {
			assert_lt(rank,numNodes());
			return getRemoteProcedure(rank,direct_selector<S>(),fun);
		}

		/**
		 * Obtains a handle for performing broad-casts on a selected remote service.
		 */
		template<typename S, typename ... Args>
		Broadcast<S,Args...> broadcast(void(S::*fun)(Args...)) {
			return { fun };
		}

		// -------- development and debugging interface --------

		/**
		 * Triggers the given operation to be processed on the selected node. This operation
		 * is intended for test cases to initiate operations to be performed on selected nodes.
		 */
		template<typename Op>
		std::enable_if_t<!std::is_void<std::result_of_t<Op(Node&)>>::value,std::result_of_t<Op(Node&)>>
		runOn(rank_t rank, const Op& op) {
			assert_lt(rank,numNodes());
			if (rank != localNode->getRank()) return {};
			return localNode->run(op);
		}

		/**
		 * Triggers the given operation to be processed on the selected node. This operation
		 * is intended for test cases to initiate operations to be performed on selected nodes.
		 */
		template<typename Op>
		std::enable_if_t<std::is_void<std::result_of_t<Op(Node&)>>::value,void>
		runOn(rank_t rank, const Op& op) {
			assert_lt(rank,numNodes());
			if (rank != localNode->getRank()) return;
			localNode->run(op);
		}

		/**
		 * Runs the given operation within each node of the network.
		 */
		template<typename Op>
		void runOnAll(const Op& op) {
			// run everywhere
			return localNode->run(op);
		}

		/**
		 * Installs a service on all nodes.
		 */
		template<typename S, typename ... Args>
		void installServiceOnNodes(const Args& ... args) {
			// just install service locally
			localNode->startService<S>(args...);
		}

		/**
		 * Removes a service from all nodes.
		 */
		template<typename S>
		void removeServiceOnNodes() {
			// just remove service locally
			localNode->stopService<S>();
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
			stats = Statistics();
		}

	};

} // end of namespace mpi
} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
