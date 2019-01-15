
#if defined(ENABLE_MPI)

#include <mpi.h>
#include <mutex>
#include <condition_variable>

#include "allscale/utils/spinlock.h"
#include "allscale/runtime/com/mpi/network.h"

namespace allscale {
namespace runtime {
namespace com {
namespace mpi {

	int getFreshRequestTag() {
		static std::atomic<int> counter(1);
		auto tag = counter.fetch_add(2,std::memory_order_relaxed);
		assert_pred1(isRequestTag,tag);
		return tag;
	}

	namespace detail {


		// an epoch counter service for syncing global operations
		class EpochService {

			// the network this service is being a part of
			Network& net;

			// the rank of the local node
			rank_t rank;

			// the mutex guarding the epoch counter and condition var
			std::mutex mutex;

			// the condition variable for the epoch counter
			std::condition_variable condition_var;

			// the epoch counter reflecting the globally reached epoch
			int counter;

			// a counter maintained by the root node to see how many nodes are ready for the next epoch
			int ready_counter = 0;

		public:

			EpochService(Node& node, Network& net)
				: net(net), rank(node.getRank()), counter(0) {}


			// -- local interface --

			void nextEpochReached() {

				// get current epoch
				int current = counter;
				DEBUG_MPI_NETWORK << "Rank " << rank << " leafing epoch " << current << " ...\n";

				if (rank == 0) {

					// wait until all nodes are in the next epoch
					{
						int num_other_nodes = net.numNodes() - 1;

						std::unique_lock<std::mutex> g(mutex);
						if (ready_counter != num_other_nodes) {
							// wait for all nodes to be present
							condition_var.wait(g,[&]{
								return ready_counter == num_other_nodes;
							});
						}

						// reset counter for next epoch
						ready_counter = 0;
					}

					// signal next epoch to all nodes
					net.broadcast(&EpochService::globalEpochReached)(current+1);

				} else {

					// signal root node that this node has reached the next epoch
					net.getRemoteProcedure(0,&EpochService::localEpochReached)(rank);

					// wait for next epoch to be globally reached
					{
						std::unique_lock<std::mutex> g(mutex);
						// wait for reaching next global state
						condition_var.wait(g,[&]{
							return counter > current;
						});
					}

				}

				DEBUG_MPI_NETWORK << "Rank " << rank << " starting into epoch " << counter << "\n";

			}

			// -- local interface --

			void localEpochReached(com::rank_t) {
				std::lock_guard<std::mutex> g(mutex);
				ready_counter++;
				condition_var.notify_one();
			}

			void globalEpochReached(int epoch) {
				std::lock_guard<std::mutex> g(mutex);
				counter = epoch;
				condition_var.notify_one();
			}

		};

	}


	// the communicator used for point-to-point operations
	MPI_Comm point2point;

	// the mutex for synchronizing MPI accesses
	utils::spinlock G_MPI_MUTEX;

	// the singleton network instance
	Network Network::instance;


	Network::Network() : num_nodes(1), alive(true) {
		// start up MPI environment
		int available;
		MPI_Init_thread(nullptr,nullptr,MPI_THREAD_SERIALIZED,&available);
		assert_le(MPI_THREAD_SERIALIZED,available);

		// get the number of nodes
		int size;
		MPI_Comm_size(MPI_COMM_WORLD,&size);
		num_nodes = size;

		// startup local node
		int rank;
		MPI_Comm_rank(MPI_COMM_WORLD,&rank);
		localNode = std::make_unique<Node>(rank);

		// install epoch counter service
		localNode->startService<detail::EpochService>(*this);
		localNode->startService<SyncCallHandlerService>();

		// create communicator groups
		MPI_Comm_dup(MPI_COMM_WORLD,&point2point);

		// start up communication server
		com_server = std::thread([&]{
			localNode->run([&](com::Node&){
				runRequestServer();
			});
		});

		// start the network statistic service
		localNode->startService<NetworkStatisticService>();

		// wait for all nodes to complete the startup phase
		sync();

	}

	Network::~Network() {

		DEBUG_MPI_NETWORK << "Node " << localNode->getRank() << " beginning shutdown ..\n";

		// wait for all nodes to reach the shutdown barrier
		sync();

		DEBUG_MPI_NETWORK << "Node " << localNode->getRank() << " conducting shutdown ..\n";

		// kill request server
		alive = false;

		// wait for completion
		com_server.join();

		// free communicator groups
		MPI_Comm_free(&point2point);

		// shut down environment
		MPI_Finalize();

		DEBUG_MPI_NETWORK << "Node " << localNode->getRank() << " shutdown complete\n";
	}

	Network* Network::create() {
		return &instance;
	}

	Network* Network::create(size_t size) {
		return (instance.numNodes() == size) ? &instance : nullptr;
	}

	Network& Network::getNetwork() {
		return instance;
	}

	Node& Network::getLocalNode() {
		return *getNetwork().localNode;
	}

	void Network::send(const std::vector<char>& msg, com::rank_t trg, int tag) {
		MPI_Request request;
		{
			std::lock_guard<utils::spinlock> g(G_MPI_MUTEX);
			getLocalStats().sent_bytes += msg.size();
			MPI_Isend(&msg[0],msg.size(),MPI_CHAR,trg,tag,point2point,&request);
		}

		// a utility to test that the message has been send
		auto done = [&]()->bool{
			std::lock_guard<utils::spinlock> g(G_MPI_MUTEX);
			int done;
			MPI_Test(&request,&done,MPI_STATUS_IGNORE);
			return done == true;
		};

		// wait for completion of send call
		while(!done()) {
			// TODO: suspend fibers here, and do something more useful instead ...
			processMessageNonBlocking();
		}
	}

	void Network::sendRequest(const std::vector<char>& msg, com::rank_t trg, int request_tag) {

		// check validity of parameters
		assert_ne(trg,localNode->getRank());
		assert_pred1(isRequestTag,request_tag);

		// send to target node
		DEBUG_MPI_NETWORK << "Node " << localNode->getRank() << ": Sending request " << request_tag << " to " << trg << " ...\n";
		send(msg,trg,request_tag);
		DEBUG_MPI_NETWORK << "Node " << localNode->getRank() << ": Request " << request_tag << " sent to " << trg << "\n";
	}

	void Network::sendResponse(const std::vector<char>& msg, com::rank_t trg, int response_tag) {

		// check validity of parameters
		assert_ne(trg,localNode->getRank());
		assert_pred1(isResponseTag,response_tag);

		// send to target node
		DEBUG_MPI_NETWORK << "Node " << localNode->getRank() << ": Sending response to request " << getRequestTag(response_tag) << " to source " << trg << " ...\n";
		send(msg,trg,response_tag);
		DEBUG_MPI_NETWORK << "Node " << localNode->getRank() << ": Response sent to request " << getRequestTag(response_tag) << " to source " << trg << "\n";
	}



	std::vector<char> Network::sendRequestAndWaitForResponse(const std::vector<char>& msg, com::rank_t src, int request_tag, com::rank_t trg, int response_tag) {

		// check validity of parameters
		assert_eq(src,localNode->getRank());
		assert_ne(src,trg);
		assert_true(isRequestTag(request_tag));
		assert_true(isResponseTag(response_tag));

		// -- First install the response handler --

		// create a response event
		auto& eventReg = localNode->getFiberContext().getEventRegister();
		auto responseReadyEvent = eventReg.create();

		// create buffer for response
		std::vector<char> response;

		// register response event
		std::pair<int,int> key(trg,response_tag);
		{
			std::lock_guard<utils::spinlock> g(responde_handler_lock);
			responde_handler[key] = { responseReadyEvent, &response };
		}

		// send request
		sendRequest(msg,trg,request_tag);

		// wait for response to be ready
		utils::fiber::suspend(responseReadyEvent);

		// now the response should be available
		assert_false(response.empty());

		// process response
		return response;

	}

	bool Network::processMessage() {

		auto& node = *localNode;
		int flag;
		MPI_Status status;

		// probe for some incoming message
		std::vector<char> buffer;
		{
			G_MPI_MUTEX.lock();
			MPI_Iprobe(MPI_ANY_SOURCE,MPI_ANY_TAG,point2point,&flag,&status);

			// if there is nothing, do nothing
			if (!flag) {
				G_MPI_MUTEX.unlock();
				return false;
			}

			// handle responses
			if (isResponseTag(status.MPI_TAG)) {
				response_id id(status.MPI_SOURCE,status.MPI_TAG);

				// locate response handler
				ResponseHandler handler;

				{
					std::lock_guard<utils::spinlock> g(responde_handler_lock);
					auto pos = responde_handler.find(id);
					assert_true(pos != responde_handler.end());
					handler = pos->second;
					responde_handler.erase(pos);
				}

				// get response message buffer
				auto& response = *handler.message;

				// retrieve message
				int count = 0;
				MPI_Get_count(&status,MPI_CHAR,&count);

				// allocate memory
				response.resize(count);

				// receive message
				DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": Receiving response " << status.MPI_TAG << " from " << status.MPI_SOURCE << " of size " << count << " bytes ...\n";
				Network::getLocalStats().received_bytes += count;
				MPI_Recv(&response[0],count,MPI_CHAR,status.MPI_SOURCE,status.MPI_TAG,point2point,&status);

				// free MPI access
				G_MPI_MUTEX.unlock();

				// signal readiness of response
				localNode->getFiberContext().getEventRegister().trigger(handler.event);

				// yield the control -- probably to the receiver
				node.getFiberContext().yield();

				// done
				return true;
			}

			// otherwise, handle requests
			assert_pred1(isRequestTag,status.MPI_TAG);

			// retrieve message
			int count = 0;
			MPI_Get_count(&status,MPI_CHAR,&count);

			// allocate memory
			buffer.resize(count);

			// receive message
			DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": Receiving request " << status.MPI_TAG << " from " << status.MPI_SOURCE << " of size " << count << " bytes ...\n";
			Network::getLocalStats().received_bytes += count;
			MPI_Recv(&buffer[0],count,MPI_CHAR,status.MPI_SOURCE,status.MPI_TAG,point2point,&status);

			// free global MPI access lock
			G_MPI_MUTEX.unlock();
		}

		// process request handler in a fiber context (to allow interrupts)
		localNode->getFiberContext().start([&]{

			// parse message
			allscale::utils::Archive a(std::move(buffer));
			auto msg = allscale::utils::deserialize<request_msg_t>(a);

			DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": Processing message " << (void*)(std::get<0>(msg)) << " of request " << status.MPI_TAG << "\n";

			// process message
			node.run([&](Node&){
				std::get<0>(msg)(*this,status.MPI_SOURCE,status.MPI_TAG,node,std::get<1>(msg));
			});
			DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": processing of request " << status.MPI_TAG << " from " << status.MPI_SOURCE << " complete.\n";

		});

		// done
		return true;
	}

	void Network::processMessageNonBlocking() {
		localNode->getFiberContext().start([&]{
			processMessage();
		});
	}

	void Network::runRequestServer() {
		using namespace std::literals::chrono_literals;

		auto& node = *localNode;
		DEBUG_MPI_NETWORK << "Starting up request server on node " << node.getRank() << "\n";

		while(alive) {
			// try processing some request
			if (!processMessage()) {
				// be nice if no message is present
				std::this_thread::sleep_for(1us);
			}
		}

		DEBUG_MPI_NETWORK << "Shutting down request server on node " << node.getRank() << "\n";
	}

	void Network::sync() {

		// signal reaching of next epoch
		localNode->getService<detail::EpochService>().nextEpochReached();

	}

	namespace {

		NetworkStatistics getStatisticsWithinThread(Network& net) {
			// this implementation does not support calls within fibers
			assert_false(allscale::utils::fiber::getCurrentFiber())
				<< "Calls within fibers not supported!";

			auto num_nodes = net.numNodes();
			NetworkStatistics res(num_nodes);

			// use an actual lock to sync on completion of operation
			std::mutex lock;
			lock.lock();

			net.getLocalNode().getFiberContext().start([&]{
				std::vector<RemoteCallResult<NodeStatistics>> futures(num_nodes);
				for(rank_t i=0; i<num_nodes; i++) {
					futures[i] = net.getRemoteProcedure(i,&NetworkStatisticService::getNodeStats)();
				}
				for(rank_t i=0; i<num_nodes; i++) {
					res[i] = futures[i].get();
				}
				lock.unlock();
			});

			// block until done
			lock.lock();

			// done
			return res;
		}

		NetworkStatistics getStatisticsWithinFiber(Network& net) {
			// this implementation does not support calls within fibers
			assert_true(allscale::utils::fiber::getCurrentFiber())
				<< "Must be called within a fiber!";

			auto num_nodes = net.numNodes();
			NetworkStatistics res(num_nodes);

			// collect data asynchron
			std::vector<RemoteCallResult<NodeStatistics>> futures(num_nodes);
			for(rank_t i=0; i<num_nodes; i++) {
				futures[i] = net.getRemoteProcedure(i,&NetworkStatisticService::getNodeStats)();
			}
			for(rank_t i=0; i<num_nodes; i++) {
				res[i] = futures[i].get();
			}

			// done
			return res;
		}

	}


	NetworkStatistics Network::getStatistics() {
		auto fiber = allscale::utils::fiber::getCurrentFiber();
		if (fiber) {
			return getStatisticsWithinFiber(*this);
		}
		return getStatisticsWithinThread(*this);
	}

	NodeStatistics& Network::getLocalStats() {
		return getNetwork().localNode->getService<NetworkStatisticService>().getLocalNodeStats();
	}

} // end of namespace mpi
} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale

#include "allscale/runtime/com/hierarchy.h"
#include "allscale/runtime/data/data_item_index.h"

void dumpSystemState() {

	using namespace allscale::runtime;

	auto& net = com::mpi::Network::getNetwork();
	com::HierarchicalOverlayNetwork hierarchy = net;

	auto& node = com::mpi::Network::getLocalNode();
	std::cout << "System state of node " << node.getRank() << ":\n";

	node.run([&](com::Node&){
		hierarchy.forAllLocal<data::DataItemIndexService>([&](const data::DataItemIndexService& service){
			service.dumpState("\t");
		});
	});

}

#endif
