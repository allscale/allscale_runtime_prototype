
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
			runRequestServer();
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

	void Network::processResponse(MPI_Status& status) {

		// make sure the current message is a response
		assert_true(isResponseTag(status.MPI_TAG));

		// retrieve message size
		int count = 0;
		MPI_Get_count(&status,MPI_CHAR,&count);

		// allocate memory
		std::vector<char> buffer(count);

		int src = status.MPI_SOURCE;
		int tag = status.MPI_TAG;

		DEBUG_MPI_NETWORK << "Node " << localNode->getRank() << ": Receiving response " << tag << " from " << src << " of size " << count << " bytes ...\n";

		// receive message
		getLocalStats().received_bytes += count;
		MPI_Recv(&buffer[0],count,MPI_CHAR,status.MPI_SOURCE,status.MPI_TAG,point2point,&status);

		DEBUG_MPI_NETWORK << "Node " << localNode->getRank() << ": Response " << tag << " for " << src << " received\n";

		// insert into response buffer
		guard g(response_buffer_lock);
		response_buffer[{src,tag}] = std::move(buffer);

	}

	std::vector<char> Network::waitForResponse(com::rank_t src, int response_tag) {

		// check validity of parameters
		assert_ne(src,localNode->getRank());
		assert_true(isResponseTag(response_tag));

		// get lookup key
		std::pair<int,int> key(src,response_tag);

		while(true) {

			{
				// check whether response has been received ..
				guard g(response_buffer_lock);
				auto pos = response_buffer.find(key);
				if (pos != response_buffer.end()) {
					// take response message from buffer ..
					std::vector<char> res = std::move(pos->second);
					response_buffer.erase(pos);

					DEBUG_MPI_NETWORK << "Node " << localNode->getRank() << ": Response " << response_tag << " for " << src << " consumed\n";

					return res;
				}
			}

			// keep processing messages in the meanwhile ..
			processMessage();
		}
	}

	void Network::processMessage() {
		using namespace std::literals::chrono_literals;

		auto& node = *localNode;
		int flag;
		MPI_Status status;

		// probe for some incoming message
		std::vector<char> buffer;
		{
			std::lock_guard<std::mutex> g(G_MPI_MUTEX);
			MPI_Iprobe(MPI_ANY_SOURCE,MPI_ANY_TAG,point2point,&flag,&status);

			// if there is nothing ...
			if (!flag) {
				// ... be nice here
				std::this_thread::sleep_for(1us);
				return;
			}

			// do not consume responses
			if (!isRequestTag(status.MPI_TAG)) {
				processResponse(status);
				return;
			}

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

		// parse message
		allscale::utils::Archive a(std::move(buffer));
		auto msg = allscale::utils::deserialize<request_msg_t>(a);

		DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": Processing message " << (void*)(std::get<0>(msg)) << "\n";

		// process message
		node.run([&](Node&){
			std::get<0>(msg)(status.MPI_SOURCE,status.MPI_TAG,node,std::get<1>(msg));
		});
		DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": processing of request " << status.MPI_TAG << " from " << status.MPI_SOURCE << " complete.\n";
	}

	void Network::runRequestServer() {

		auto& node = *localNode;
		DEBUG_MPI_NETWORK << "Starting up request server on node " << node.getRank() << "\n";

		while(alive) {
			// try processing some request
			processMessage();
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
#endif
