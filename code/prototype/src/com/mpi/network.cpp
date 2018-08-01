
#ifdef ENABLE_MPI

#include <mpi.h>
//#include <mpi/mpi.h>

#include "allscale/runtime/com/mpi/network.h"

namespace allscale {
namespace runtime {
namespace com {
namespace mpi {

	std::ostream& operator<<(std::ostream& out, const Network::Statistics&) {
		return out << " -- nothing to report --";
	}


	// the singleton network instance
	Network Network::instance;

	Network::Network() : num_nodes(1) {
		// start up MPI environment
		MPI_Init(nullptr,nullptr);

		// get the number of nodes
		int size;
		MPI_Comm_size(MPI_COMM_WORLD,&size);
		num_nodes = size;

		// startup local node
		int rank;
		MPI_Comm_rank(MPI_COMM_WORLD,&rank);
		localNode = std::make_unique<Node>(rank);
	}

	Network::~Network() {
		// shut down environment
		MPI_Finalize();
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

} // end of namespace mpi
} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
#endif
