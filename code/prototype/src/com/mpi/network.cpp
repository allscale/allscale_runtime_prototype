
#if defined(ENABLE_MPI)

#include <mpi.h>

#include "allscale/runtime/com/mpi/network.h"

namespace allscale {
namespace runtime {
namespace com {
namespace mpi {

	std::ostream& operator<<(std::ostream& out, const Network::Statistics&) {
		return out << " -- reporting for MPI not implemented yet --\n";
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

		// install epoch counter service
		localNode->startService<detail::EpochService>(epoch_counter);

		// create communicator groups
		MPI_Comm_dup(MPI_COMM_WORLD,&point2point);

		// start up communication server
		com_server = std::thread([&]{
			runRequestServer();
		});

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

	void Network::runRequestServer() {
		auto& node = *localNode;
		DEBUG_MPI_NETWORK << "Starting up request server on node " << node.getRank() << "\n";

		int flag;
		MPI_Status status;
		while(alive) {

			// probe for some incoming message
			{
				std::lock_guard<std::mutex> g(G_MPI_MUTEX);
				MPI_Iprobe(MPI_ANY_SOURCE,REQUEST_TAG,point2point,&flag,&status);
			}

			// if there is nothing ...
			if (!flag) {
				// ... be nice here
				std::this_thread::yield();
				continue;
			}

			// retrieve message
			int count = 0;
			{
				std::lock_guard<std::mutex> g(G_MPI_MUTEX);
				MPI_Get_count(&status,MPI_CHAR,&count);
			}

			// allocate memory
			std::vector<char> buffer(count);

			// receive message
			DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": Receiving request ...\n";
			{
				std::lock_guard<std::mutex> g(G_MPI_MUTEX);
				MPI_Recv(&buffer[0],count,MPI_CHAR,status.MPI_SOURCE,status.MPI_TAG,point2point,&status);
			}

			// parse message
			allscale::utils::Archive a(std::move(buffer));
			auto msg = allscale::utils::deserialize<request_msg_t>(a);

			DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": Processing message " << (void*)(std::get<0>(msg)) << "\n";

			// process message
			node.run([&](Node&){
				std::get<0>(msg)(status.MPI_SOURCE,node,std::get<1>(msg));
			});
			DEBUG_MPI_NETWORK << "Node " << node.getRank() << ": processing complete.\n";
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
			while(last_epoch != epoch_counter) {
				std::this_thread::yield();
			}

			DEBUG_MPI_NETWORK << "Node " << rank << ": reached epoch " << last_epoch << "\n";
		}
	}

} // end of namespace mpi
} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
#endif
