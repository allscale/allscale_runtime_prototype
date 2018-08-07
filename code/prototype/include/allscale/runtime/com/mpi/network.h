/*
 * An MPI based implementation of the network interface.
 *
 *  Created on: Aug 1, 2018
 *      Author: herbert
 */

#pragma once

#include <atomic>
#include <memory>
#include <ostream>
#include <utility>
#include <thread>
#include <vector>

#include <mpi.h>

#include "allscale/utils/assert.h"
#include "allscale/utils/serializer.h"
#include "allscale/utils/serializer/functions.h"
#include "allscale/utils/serializer/tuple.h"

#include "allscale/runtime/com/node.h"

namespace allscale {
namespace runtime {
namespace com {
namespace mpi {

//#define DEBUG_MPI_NETWORK if(true)  std::cout
#define DEBUG_MPI_NETWORK if(false) std::cout

	// the two tags used for request/respond exchanges
	constexpr int REQUEST_TAG = 123;
	constexpr int RESPOND_TAG = 321;

	// the communicator used for point-to-point operations
	extern MPI_Comm point2point;


	namespace detail {

		// a utility for running services

		template<typename R, typename Tuple, std::size_t ... I>
		R runOperationOnHelper(Node& node, Tuple& args, std::index_sequence<I...>) {
			return (std::get<1>(args)(node).*std::get<0>(args))(std::move(std::get<I+2>(args))...);
		}

		template<typename R, typename ... Args>
		R runOperationOn(Node& node, std::tuple<Args...>& args) {
			return runOperationOnHelper<R>(node,args,std::make_index_sequence<sizeof...(Args)-2>());
		}

		template<typename Tuple, std::size_t ... I>
		void runProcedureOnHelper(Node& node, Tuple& args, std::index_sequence<I...>) {
			return (std::get<1>(args)(node).*std::get<0>(args))(std::move(std::get<I+2>(args))...);
		}

		template<typename ... Args>
		void runProcedureOn(Node& node, std::tuple<Args...>& args) {
			runProcedureOnHelper(node,args,std::make_index_sequence<sizeof...(Args)-2>());
		}


		// an epoch counter service for syncing global operations
		struct EpochService {

			rank_t rank;

			std::atomic<int>& counter;

			EpochService(Node& node, std::atomic<int>& counter)
				: rank(node.getRank()), counter(counter) {}

			// Increments the state and acknowledges the update (by returning)
			bool inc(int next) {
				DEBUG_MPI_NETWORK << "Node " << rank << ": Increasing epoch from " << counter << " to " << next << "\n";
				assert_eq(next-1,counter);
				counter++;
				return true;
			}

		};

	}

	// the mutex for synchronizing MPI accesses
	extern std::mutex G_MPI_MUTEX;

	/**
	 * The network implementation for an MPI based implementation.
	 */
	class Network {

		// the type of handler send along with messages to dispatch calls on the receiver side
		using request_handler_t = void(*)(rank_t,Node&, allscale::utils::Archive&);

		// the type of message send for service requests
		using request_msg_t = std::tuple<request_handler_t,allscale::utils::Archive>;

		// the epoch counter, for global synchronization
		std::atomic<int> epoch_counter;

		// the last epoch reached locally
		int last_epoch;

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
		struct direct_selector  : public allscale::utils::trivially_serializable {
			S& operator()(Node& node) const {
				return node.getService<S>();
			}
		};

		/**
		 * A handle for remote procedures.
		 */
		template<typename Selector, typename S, typename R, typename ... Args>
		class RemoteProcedure {

			// the local node
			Node& local;

			// the targeted node
			rank_t target;

			// the service selector
			Selector selector;

			// the targeted service function
			R(S::* fun)(Args...);

			// the tuple encoding the arguments
			using args_tuple = std::tuple<R(S::*)(Args...),Selector,std::decay_t<Args>...>;

		public:

			/**
			 * Creates a new remote procedure reference.
			 */
			RemoteProcedure(Node& local, rank_t target, const Selector& selector, R(S::*fun)(Args...))
				: local(local), target(target), selector(selector), fun(fun) {}

			static void handleRequest(rank_t source, com::Node& node, allscale::utils::Archive& archive) {
				// unpack archive to obtain arguments
				auto args = allscale::utils::deserialize<args_tuple>(archive);

				DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": Processing request ...\n";

				// run service
				R res = detail::runOperationOn<R>(node, args);

				DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": Sending response to " << source << " ...\n";

				// send back result to source
				allscale::utils::Archive a = allscale::utils::serialize(res);
				auto& buffer = a.getBuffer();
				MPI_Send(&buffer[0],buffer.size(),MPI_CHAR,source,RESPOND_TAG,point2point);

				DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": Response sent to " << source << "\n";
			}

			/**
			 * Realizes the actual remote procedure call.
			 */
			R operator()(Args ... args) {
				auto src = local.getRank();
				auto trg = target;

				// short-cut for local communication
				if (src == trg) {
					return (selector(local).*fun)(std::forward<Args>(args)...);
				}

				// perform remote call
				{
					// create an argument tuple and serialize it
					auto aa  = allscale::utils::serialize(args_tuple(fun,selector,args...));
					auto msg = allscale::utils::serialize(request_msg_t(&handleRequest,aa));
					auto& buffer = msg.getBuffer();
					// send to target node

					DEBUG_MPI_NETWORK << "Node " << src << ": Sending request ...\n";

					{
						std::lock_guard<std::mutex> g(G_MPI_MUTEX);
						MPI_Send(&buffer[0],buffer.size(),MPI_CHAR,trg,REQUEST_TAG,point2point);
					}

					DEBUG_MPI_NETWORK << "Node " << src << ": Request sent\n";

				}

				// wait for result
				// TODO: avoid blocking, yield to worker thread
//				work::yield();
				int flag = false;
				MPI_Status status;
				while(!flag) {
					std::lock_guard<std::mutex> g(G_MPI_MUTEX);
					MPI_Iprobe(trg,RESPOND_TAG,point2point,&flag,&status);
				}

				// retrieve message
				int count = 0;
				{
					std::lock_guard<std::mutex> g(G_MPI_MUTEX);
					MPI_Get_count(&status,MPI_CHAR,&count);
				}

				// allocate memory
				std::vector<char> buffer(count);

				DEBUG_MPI_NETWORK << "Node " << src << ": Receiving response ...";

				// receive message
				{
					std::lock_guard<std::mutex> g(G_MPI_MUTEX);
					MPI_Recv(&buffer[0],count,MPI_CHAR,status.MPI_SOURCE,status.MPI_TAG,point2point,&status);
				}

				DEBUG_MPI_NETWORK << "Node " << src << ": Response received\n";

				// unpack result
				allscale::utils::Archive a(std::move(buffer));
				return allscale::utils::deserialize<R>(a);
			}

		};

		/**
		 * A handle for remote procedures.
		 */
		template<typename Selector, typename S, typename ... Args>
		class RemoteProcedure<Selector,S,void,Args...> {

			// the local node
			Node& local;

			// the targeted node
			rank_t target;

			// the service selector
			Selector selector;

			// the targeted service function
			void(S::* fun)(Args...);

			// the tuple encoding the arguments
			using args_tuple = std::tuple<void(S::*)(Args...),Selector,std::decay_t<Args>...>;

		public:

			/**
			 * Creates a new remote procedure reference.
			 */
			RemoteProcedure(Node& local, rank_t target, const Selector& selector, void(S::*fun)(Args...))
				: local(local), target(target), selector(selector), fun(fun) {}

			static void handleProcedure(rank_t source, com::Node& node, allscale::utils::Archive& archive) {
				// unpack archive to obtain arguments
				auto args = allscale::utils::deserialize<args_tuple>(archive);

				DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": Processing request from " << source << " ...\n";

				// run service
				detail::runProcedureOn(node, args);

				DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": Request processed, no result produced\n";
			}


			/**
			 * Realizes the actual remote procedure call.
			 */
			void operator()(Args ... args) const {
				auto src = local.getRank();
				auto trg = target;

				// short-cut for local communication
				if (src == trg) {
					(selector(local).*fun)(std::forward<Args>(args)...);
					return;
				}

				// perform remote call
				{
					// create an argument tuple and serialize it
					auto aa  = allscale::utils::serialize(args_tuple(fun,selector,args...));
					auto msg = allscale::utils::serialize(request_msg_t(&handleProcedure,aa));
					auto& buffer = msg.getBuffer();

					// send to target node
					DEBUG_MPI_NETWORK << "Node " << src << ": Sending request " << (void*)&handleProcedure << " ...\n";

					{
						std::lock_guard<std::mutex> g(G_MPI_MUTEX);
						MPI_Send(&buffer[0],buffer.size(),MPI_CHAR,trg,REQUEST_TAG,point2point);
					}

					DEBUG_MPI_NETWORK << "Node " << src << ": Request sent (fire-and-forget)\n";

				}
			}

		};

		/**
		 * A handle for broadcasts.
		 */
		template<typename R, typename S, typename ... Args>
		class Broadcast {

			// the targeted service function
			R(S::* fun)(Args...);

		public:

			/**
			 * Creates a new remote procedure reference.
			 */
			Broadcast(R(S::*fun)(Args...))
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

		// the thread responding to incoming messages
		std::thread com_server;

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
			return { *localNode, rank, selector, fun };
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
		template<typename R,typename S, typename ... Args>
		Broadcast<R,S,Args...> broadcast(R(S::*fun)(Args...)) {
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
		 * Get all nodes to the same state.
		 */
		void sync();

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

	private:

		// a flag indicating whether it is shutdown-time
		std::atomic<bool> alive;

		void runRequestServer();

	};

} // end of namespace mpi
} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
