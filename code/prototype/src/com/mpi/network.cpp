
#if defined(ENABLE_MPI)

#include <mpi.h>
#include <mutex>
#include <condition_variable>

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


		bool EpochService::inc(int next) {
			DEBUG_MPI_NETWORK << "Node " << rank << ": Increasing epoch from " << counter << " to " << next << "\n";

			// update epoche counter
			{
				guard g(mutex);
				assert_eq(next-1,counter);
				counter++;
			}

			// notify consumers
			condition_variable.notify_all();

			// done
			return true;
		}

	}


	// the communicator used for point-to-point operations
	MPI_Comm point2point;

	// the mutex for synchronizing MPI accesses
	std::mutex G_MPI_MUTEX;

	// the singleton network instance
	Network Network::instance;


	Network::Network() : epoch_counter(0), last_epoch(0), num_nodes(1), alive(true) {
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
std::cout << "Starting up rank " << rank << "/" << num_nodes << "\n";
		// install epoch counter service
		localNode->startService<detail::EpochService>(epoch_mutex,epoch_condition_var,epoch_counter);

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
			std::lock_guard<std::mutex> g(G_MPI_MUTEX);
			getLocalStats().sent_bytes += msg.size();
			MPI_Isend(&msg[0],msg.size(),MPI_CHAR,trg,tag,point2point,&request);
		}

		// a utility to test that the message has been send
		auto done = [&]()->bool{
			std::lock_guard<std::mutex> g(G_MPI_MUTEX);
			int done;
			MPI_Test(&request,&done,MPI_STATUS_IGNORE);
			return done == true;
		};

		// wait for completion of send call
		while(!done()) {
			Network::getNetwork().processMessage();
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

		// get the id of the response waiting for
		std::pair<int,int> key(trg,response_tag);

		// submit a fiber waiting for the response
		std::atomic<bool> done(false);
		std::vector<char> response;
		auto fiber = pool.start([&]{

			// suspend fiber immediately
			utils::FiberPool::suspend();

			// NOTE: the lock protection is given by the context continuing this fiber

			// upon awakening, the message should be available
			int flag;
			MPI_Status status;
			MPI_Iprobe(trg,response_tag,point2point,&flag,&status);

			// message should be available
			assert_true(flag);

			// retrieve message
			int count = 0;
			MPI_Get_count(&status,MPI_CHAR,&count);

			// allocate memory
			response.resize(count);

			// receive message
			DEBUG_MPI_NETWORK << "Node " << src << ": Receiving response " << response_tag << " from " << trg << " of size " << count << " bytes ...\n";
			Network::getLocalStats().received_bytes += count;
			MPI_Recv(&response[0],count,MPI_CHAR,status.MPI_SOURCE,status.MPI_TAG,point2point,&status);

			// signal completion
			done = true;
		});

		// the fiber should be suspended
		assert_true(fiber);

		// finally, register the response handler
		{
			guard g(responde_handler_lock);
			responde_handler[key] = *fiber;
		}

		// -- send request --

		sendRequest(msg,trg,request_tag);


		// -- finally wait for response --

		// while not done ..
		while(!done) {
			// .. help processing requests
			processMessage();
		}

		// done
		return response;

	}

	bool Network::processMessage() {

		auto& node = *localNode;
		int flag;
		MPI_Status status;

		// probe for some incoming message
		std::vector<char> buffer;
		{
			std::lock_guard<std::mutex> g(G_MPI_MUTEX);
			MPI_Iprobe(MPI_ANY_SOURCE,MPI_ANY_TAG,point2point,&flag,&status);

			// if there is nothing, do nothing
			if (!flag) return false;

			// handle responses
			if (isResponseTag(status.MPI_TAG)) {
				response_id id(status.MPI_SOURCE,status.MPI_TAG);

				// obtain the handler registered for this message
				response_handler handler;
				{
					guard g(responde_handler_lock);
					auto pos = responde_handler.find(id);
					assert_true(pos != responde_handler.end());
					handler = pos->second;
					responde_handler.erase(pos);
				}

				// process handler
				pool.resume(handler);

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
		}

		// process request handler in a fiber context (to allow interrupts)
		pool.start([&]{

			// parse message
			allscale::utils::Archive a(std::move(buffer));
			auto msg = allscale::utils::deserialize<request_msg_t>(a);

			DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": Processing message " << (void*)(std::get<0>(msg)) << "\n";

			// process message
			node.run([&](Node&){
				std::get<0>(msg)(*this,status.MPI_SOURCE,status.MPI_TAG,node,std::get<1>(msg));
			});
			DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": processing of request " << status.MPI_TAG << " from " << status.MPI_SOURCE << " complete.\n";

		});

		// done
		return true;
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
		auto rank = localNode->getRank();
		if (rank == 0) {

			DEBUG_MPI_NETWORK << "Node " << rank << ": stepping from epoch " << last_epoch << " to " << last_epoch+1 << "\n";

			// check current epoch
			int cur_epoch = epoch_counter;
			assert_eq(last_epoch,cur_epoch);

			// broadcast epoch update
			broadcast(&detail::EpochService::inc)(cur_epoch+1);

			last_epoch++;
			assert_eq(last_epoch,epoch_counter);

		} else {

			DEBUG_MPI_NETWORK << "Node " << rank << ": waiting for epoch step from " << last_epoch << " to " << last_epoch+1 << "\n";

			// wait for update
			last_epoch++;	// this is now the one we should be in
			{
				std::unique_lock<std::mutex> g(epoch_mutex);
				// wait for condition
				epoch_condition_var.wait(g,[&](){
					return last_epoch == epoch_counter;
				});
			}

			DEBUG_MPI_NETWORK << "Node " << rank << ": reached epoch " << last_epoch << "\n";
		}
	}

	NetworkStatistics Network::getStatistics() {

		NetworkStatistics res(num_nodes);
		for(rank_t i=0; i<num_nodes; i++) {
			res[i] = getNetwork().getRemoteProcedure(i,&NetworkStatisticService::getNodeStats)();
		}
		return res;
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
