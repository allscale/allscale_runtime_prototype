/*
 * An MPI based implementation of the network interface.
 *
 *  Created on: Aug 1, 2018
 *      Author: herbert
 */

#pragma once

#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <utility>
#include <thread>
#include <vector>

#include <mpi.h>

#include "allscale/utils/assert.h"
#include "allscale/utils/serializer.h"
#include "allscale/utils/serializer/functions.h"
#include "allscale/utils/serializer/tuple.h"

#include "allscale/utils/fibers.h"
#include "allscale/utils/fiber/future.h"

#include "allscale/runtime/com/node.h"
#include "allscale/runtime/com/statistics.h"


namespace allscale {
namespace runtime {
namespace com {
namespace mpi {

#define DEBUG_MPI_NETWORK if(false) std::cout



	// tag handling for requests and responses
	using tag_t = int;

	tag_t getFreshRequestTag();

	inline bool isRequestTag(tag_t tag) { return tag % 2; }

	inline bool isResponseTag(tag_t tag) { return !isRequestTag(tag); }

	inline tag_t getResponseTag(tag_t tag) {
		assert_pred1(isRequestTag,tag);
		return tag + 1;
	}

	inline tag_t getRequestTag(tag_t tag) {
		assert_pred1(isResponseTag,tag);
		return tag - 1;
	}


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

	}

	/**
	 * Define the type of result produced by remote calls.
	 */
	template<typename T>
	using RemoteCallResult = utils::fiber::Future<T>;

	/**
	 * The network implementation for an MPI based implementation.
	 */
	class Network {

		// the kind of guard used for locking operations
		using guard = std::lock_guard<std::mutex>;

		// the type of handler send along with messages to dispatch calls on the receiver side
		using request_handler_t = void(*)(Network&,rank_t,int tag, Node&, allscale::utils::Archive&);

		// the type of message send for service requests
		using request_msg_t = std::tuple<request_handler_t,allscale::utils::Archive>;

		using clock = std::chrono::high_resolution_clock;
		using time = typename clock::time_point;

		using response_id = std::pair<int,int>;

		struct ResponseHandler {
			utils::fiber::EventId event;	// < the event the handler is waiting for
			std::vector<char>* message;		// < a pointer to the storage for the received message
		};

		// a registry for events fibers are waiting on for responses from remote calls
		std::map<response_id,ResponseHandler> responde_handler;

		// a lock to protect accesses to the responde_handler registry
		mutable utils::spinlock responde_handler_lock;

	public:

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

			// the network being part of
			Network& network;

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
			RemoteProcedure(Network& net, Node& local, rank_t target, const Selector& selector, R(S::*fun)(Args...))
				: network(net), local(local), target(target), selector(selector), fun(fun) {}

			static void handleRequest(Network& net, rank_t source, int request_tag, com::Node& node, allscale::utils::Archive& archive) {
				assert_pred1(isRequestTag,request_tag);

				// count calls
				getLocalStats().received_calls++;

				// unpack archive to obtain arguments
				auto args = allscale::utils::deserialize<args_tuple>(archive);

				DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": Processing request " << request_tag << " ...\n";

				// run service
				R res = detail::runOperationOn<R>(node, args);

				DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": Sending response to request " << request_tag << " ...\n";

				// encapsulate result and send back
				allscale::utils::Archive a = allscale::utils::serialize(res);
				auto& buffer = a.getBuffer();
				net.sendResponse(buffer,source,getResponseTag(request_tag));

				DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": Response sent to request " << request_tag << " ...\n";
			}

			/**
			 * Realizes the actual remote procedure call.
			 */
			R call(Args ... args) {
				auto src = local.getRank();
				auto trg = target;

				// short-cut for local communication
				if (src == trg) {
					return (selector(local).*fun)(std::forward<Args>(args)...);
				}

				// count calls
				getLocalStats().sent_calls++;

				// perform remote call
				int request_tag = getFreshRequestTag();
				int response_tag = getResponseTag(request_tag);

				// compose the request message
				std::vector<char> request;
				{
					// create an argument tuple and serialize it
					auto aa  = allscale::utils::serialize(args_tuple(fun,selector,args...));
					auto msg = allscale::utils::serialize(request_msg_t(&handleRequest,aa));
					request = msg.getBuffer();
				}

				// send message and wait for response
				std::vector<char> buffer = network.sendRequestAndWaitForResponse(request,request_tag,trg,response_tag);

				DEBUG_MPI_NETWORK << "Node " << src << ": Response " << response_tag << " for request " << request_tag << " received from " << trg << "\n";

				// unpack result
				allscale::utils::Archive a(std::move(buffer));
				return allscale::utils::deserialize<R>(a);
			}

			/**
			 * Wraps up the remote call in an asynchronous handler.
			 */
			RemoteCallResult<R> operator()(Args...args) {

				auto src = local.getRank();
				auto trg = target;

				if (src == trg) {
					return call(std::forward<Args>(args)...);
				}

				RemoteCallResult<R> res;
				local.getFiberContext().start([&]{
					allscale::utils::fiber::Promise<R> promise(local.getFiberContext());
					res = promise.get_future();
					promise.set_value(call(std::forward<Args>(args)...));
				});
				return std::move(res);
			}

		};

		/**
		 * A handle for remote procedures.
		 */
		template<typename Selector, typename S, typename ... Args>
		class RemoteProcedure<Selector,S,void,Args...> {

			// the network being part of
			Network& network;

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
			RemoteProcedure(Network& net, Node& local, rank_t target, const Selector& selector, void(S::*fun)(Args...))
				: network(net), local(local), target(target), selector(selector), fun(fun) {}

			static void handleProcedure(Network&, rank_t source, int request_tag, com::Node& node, allscale::utils::Archive& archive) {
				// unpack archive to obtain arguments
				auto args = allscale::utils::deserialize<args_tuple>(archive);

				// count calls
				getLocalStats().received_calls++;

				DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": Processing request " << request_tag << " from " << source << " ...\n";

				// run service
				detail::runProcedureOn(node, args);

				DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": Request " << request_tag << " processed, no result produced\n";
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

				// count calls
				getLocalStats().sent_calls++;

				// perform remote call (in suspendable fiber context)
				local.getFiberContext().start([&]{

					// create an argument tuple and serialize it
					auto aa  = allscale::utils::serialize(args_tuple(fun,selector,std::forward<Args>(args)...));
					auto msg = allscale::utils::serialize(request_msg_t(&handleProcedure,aa));
					auto& buffer = msg.getBuffer();

					// send to target node
					network.sendRequest(buffer,trg,getFreshRequestTag());

				});
			}

		};


		/**
		 * A handle for remote procedures.
		 */
		template<typename Selector, typename S, typename R, typename ... Args>
		class RemoteConstProcedure {

			// the network working on
			Network& network;

			// the local node
			Node& local;

			// the targeted node
			rank_t target;

			// the service selector
			Selector selector;

			// the targeted service function
			R(S::* fun)(Args...) const;

			// the tuple encoding the arguments
			using args_tuple = std::tuple<R(S::*)(Args...) const,Selector,std::decay_t<Args>...>;

		public:

			/**
			 * Creates a new remote procedure reference.
			 */
			RemoteConstProcedure(Network& net, Node& local, rank_t target, const Selector& selector, R(S::*fun)(Args...) const)
				: network(net), local(local), target(target), selector(selector), fun(fun) {}

			static void handleRequest(Network& net, rank_t source, int request_tag, com::Node& node, allscale::utils::Archive& archive) {
				assert_pred1(isRequestTag,request_tag);

				// count calls
				getLocalStats().received_calls++;

				// unpack archive to obtain arguments
				auto args = allscale::utils::deserialize<args_tuple>(archive);

				DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": Processing request " << request_tag << " ...\n";

				// run service
				R res = detail::runOperationOn<R>(node, args);

				DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": Sending response to request " << request_tag << " ...\n";

				// encapsulate result and send back
				allscale::utils::Archive a = allscale::utils::serialize(res);
				auto& buffer = a.getBuffer();
				net.sendResponse(buffer,source,getResponseTag(request_tag));

				DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": Response sent to request " << request_tag << " ...\n";
			}

			/**
			 * Realizes the actual remote procedure call.
			 */
			R call(Args ... args) {
				auto src = local.getRank();
				auto trg = target;

				// short-cut for local communication
				if (src == trg) {
					return (selector(local).*fun)(std::forward<Args>(args)...);
				}

				// count calls
				getLocalStats().sent_calls++;

				// perform remote call
				int request_tag = getFreshRequestTag();
				int response_tag = getResponseTag(request_tag);

				// create request message
				std::vector<char> request;
				{
					// create an argument tuple and serialize it
					auto aa  = allscale::utils::serialize(args_tuple(fun,selector,args...));
					auto msg = allscale::utils::serialize(request_msg_t(&handleRequest,aa));
					request = msg.getBuffer();
				}

				// wait for response
				std::vector<char> buffer = network.sendRequestAndWaitForResponse(request,request_tag,trg,response_tag);

				DEBUG_MPI_NETWORK << "Node " << src << ": Response " << response_tag << " for " << request_tag << " received\n";

				// unpack result
				allscale::utils::Archive a(std::move(buffer));
				return allscale::utils::deserialize<R>(a);
			}

			/**
			 * Wraps up the remote call in an asynchronous handler.
			 */
			RemoteCallResult<R> operator()(Args...args) {

				auto src = local.getRank();
				auto trg = target;

				if (src == trg) {
					return call(std::forward<Args>(args)...);
				}

				RemoteCallResult<R> res;
				local.getFiberContext().start([&]{
					allscale::utils::fiber::Promise<R> promise(local.getFiberContext());
					res = promise.get_future();
					promise.set_value(call(std::forward<Args>(args)...));
				});
				return std::move(res);
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

			}

		};

		/**
		 * The syncing target of syncing broad-casts.
		 */
		class SyncCallHandlerService {

			Node& node;

		public:

			SyncCallHandlerService(Node& node) : node(node) {}

			template<typename S, typename ... Args>
			bool process(void(S::* fun)(Args...), Args... args) {
				(node.getService<S>().*fun)(args...);
				return true;
			}
		};

		/**
		 * A handle for broadcasts waiting for task completion.
		 */
		template<typename R, typename S, typename ... Args>
		class BroadcastAndSync {

			// the targeted service function
			R(S::* fun)(Args...);

		public:

			/**
			 * Creates a new remote procedure reference.
			 */
			BroadcastAndSync(R(S::*fun)(Args...))
				: fun(fun) {}

			/**
			 * Realizes the actual broadcast.
			 */
			void operator()(Args ... args) const {

				// send one-by-one to each other node
				auto& net = Network::getNetwork();

				// collect handles
				std::vector<RemoteCallResult<R>> calls;
				calls.reserve(net.numNodes());

				for(rank_t r = 0; r<net.numNodes(); r++) {
					calls.push_back(net.getRemoteProcedure(r,fun)(args...));
				}

				// wait for completion
				for(auto& cur : calls) {
					cur.wait();
				}

			}

		};

		/**
		 * A handle for broadcasts waiting for task completion.
		 */
		template<typename S, typename ... Args>
		class BroadcastAndSync<void,S,Args...> {

			// the targeted service function
			void(S::* fun)(Args...);

		public:

			/**
			 * Creates a new remote procedure reference.
			 */
			BroadcastAndSync(void(S::*fun)(Args...))
				: fun(fun) {}

			/**
			 * Realizes the actual broadcast.
			 */
			void operator()(Args ... args) const {

				// send one-by-one to each other node
				auto& net = Network::getNetwork();

				// collect handles
				std::vector<RemoteCallResult<bool>> calls;
				calls.reserve(net.numNodes());

				for(rank_t r = 0; r<net.numNodes(); r++) {
					calls.push_back(net.getRemoteProcedure(r,&SyncCallHandlerService::process<S,Args...>)(fun,args...));
				}

				// wait for completion
				for(auto& cur : calls) {
					cur.wait();
				}

			}

		};

	private:

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

		// obtains a reference to the local node instance
		static Node& getLocalNode();

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
			return { *this, *localNode, rank, selector, fun };
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
		 * Obtains a handle for performing a remote procedure call of a selected service.
		 */
		template<typename Selector, typename S, typename R, typename ... Args>
		RemoteConstProcedure<Selector,S,R,Args...> getRemoteProcedure(rank_t rank, const Selector& selector, R(S::*fun)(Args...) const) {
			assert_lt(rank,numNodes());
			return { *this, *localNode, rank, selector, fun };
		}

		/**
		 * Obtains a handle for performing a remote procedure call of a selected service.
		 */
		template<typename S, typename R, typename ... Args>
		auto getRemoteProcedure(rank_t rank, R(S::*fun)(Args...) const) {
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

		/**
		 * Obtains a handle for performing broad-casts on selected remote services, waiting
		 * for all services to complete the operation.
		 */
		template<typename R, typename S, typename ... Args>
		BroadcastAndSync<R,S,Args...> broadcastAndSync(R(S::*fun)(Args...)) {
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
			localNode->run([&](Node& node){
				node.startService<S>(args...);
			});
		}

		/**
		 * Removes a service from all nodes.
		 */
		template<typename S>
		void removeServiceOnNodes() {
			// just remove service locally
			localNode->run([&](Node& node){
				node.stopService<S>();
			});
		}

		/**
		 * Get all nodes to the same state.
		 */
		void sync();

		/**
		 * Obtains the network transfer statistics collected so far.
		 */
		NetworkStatistics getStatistics();

		/**
		 * Resets the statistics collected so far.
		 */
		void resetStatistics() {
			localNode->run([](Node& node){
				node.getService<NetworkStatisticService>().resetNodeStats();
			});
		}

	private:

		// a flag indicating whether it is shutdown-time
		std::atomic<bool> alive;

		void runRequestServer();

		void processMessageBlocking();

		void processMessageNonBlocking();

		void processPendingMessage(MPI_Status& status);

		void sendRequest(const std::vector<char>& msg, com::rank_t trg, int request_tag);

		void sendResponse(const std::vector<char>& msg, com::rank_t trg, int response_tag);

		void send(const std::vector<char>& msg, com::rank_t trg, int tag);

		std::vector<char> sendRequestAndWaitForResponse(const std::vector<char>& msg, int request_tag, com::rank_t trg, int response_tag);

	public:

		static NodeStatistics& getLocalStats();
	};

} // end of namespace mpi
} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
