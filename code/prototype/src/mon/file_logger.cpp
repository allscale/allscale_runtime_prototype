
#include "allscale/runtime/mon/file_logger.h"

#include <atomic>
#include <chrono>
#include <fstream>
#include <mutex>
#include <vector>

#include "allscale/runtime/com/network.h"
#include "allscale/runtime/utils/timer.h"
#include "allscale/runtime/work/worker.h"

namespace allscale {
namespace runtime {
namespace mon {

	/**
	 * The name of the environment variable checked for the name of the targeted logger file. If
	 * no set, the logger will be disabled.
	 */
	constexpr const char* ENVVAR_LOGGER_FILE = "ART_LOGGER_FILE";

	namespace {

		struct Stats : public allscale::utils::trivially_serializable {
			// processing time in ns
			uint64_t processingTime;
			// number of workers
			uint32_t numWorker;
		};

		class FileLoggerService {

			using clock = std::chrono::high_resolution_clock;
			using time = clock::time_point;

			com::Network& network;

			com::Node& node;

			std::fstream out;

			time start;

			com::rank_t numNodes;

			time lastTime;

			std::vector<uint64_t> lastProcessedWork;

		public:

			FileLoggerService(com::Node& node, const std::string& filename)
				: network(com::Network::getNetwork())
				, node(node)
				, numNodes(network.numNodes())
				, lastProcessedWork(numNodes) {

				// initialize processed work
				for(auto& cur : lastProcessedWork) {
					cur = 0;
				}

				// stop here, if it is not the root node
				if (node.getRank() != 0) return;

				// open output file
				out = std::fstream(filename, std::ios_base::out);

				// test whether opening file was successful
				if (!out) {
					std::cout << "WARNING: unable to open file logger target " << filename << "\n";
					std::cout << "Performance data logging will be disabled.\n";
					return;
				}

				// write the header
				out << "time_ms,node,load\n";

				// print first entries
				for(com::rank_t r = 0; r<network.numNodes(); r++) {
					out << "0," << r << ",0\n";
				}

				// register periodic invocation of logging operation
				node.getService<utils::PeriodicExecutorService>().runPeriodically(
					[&]()->bool { logState(); return true; },
					std::chrono::seconds(1)
				);

				// record start time
				start = clock::now();
				lastTime = start;
			}

			~FileLoggerService() {
				if (out) out.close();
			}


			void logState() {

				// check the state of the output stream
				if (!out) return;

				// issue RPC calls
				std::vector<com::RemoteCallResult<Stats>> results;
				results.reserve(numNodes);
				for(com::rank_t r = 0; r < network.numNodes(); ++r) {
					results.push_back(
						network.getRemoteProcedure(r,&FileLoggerService::getLocalStatistisc)()
					);
				}

				// wait for results
				std::vector<Stats> data;
				data.reserve(numNodes);
				for(auto& cur : results) {
					data.push_back(cur.get());
				}

				// get current time and duration since last sample
				auto currentTime = clock::now();
				auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(currentTime - lastTime).count();
				lastTime = currentTime;

				// log state
				auto time = std::chrono::duration_cast<std::chrono::milliseconds>(currentTime - start).count();
				for(com::rank_t r = 0; r < numNodes; r++ ) {
					auto& cur = data[r];

					// get the total progress
					auto diff = cur.processingTime - lastProcessedWork[r];
					lastProcessedWork[r] = cur.processingTime;

					// compute the efficiency
					double efficiency = double(diff) / (double(duration) * cur.numWorker);

					// add a log line
					out << time << "," << r << "," << efficiency << "\n";
				}

			}

			Stats getLocalStatistisc() {
				auto& pool = node.getService<work::WorkerPool>();
				Stats res;
				res.processingTime = pool.getProcessTime().count();
				res.numWorker = pool.getNumWorkers();
				return res;
			}


		};


	}

	void installFileLoggerService(com::Network& network) {

		// test whether a filename is given
		auto filename = getenv(ENVVAR_LOGGER_FILE);
		if (!filename) return;

		// start the logger service
		network.installServiceOnNodes<FileLoggerService>(std::string(filename));
	}

	void shutdownFileLoggerService(com::Network& network) {
		network.removeServiceOnNodes<FileLoggerService>();
	}




} // end namespace log
} // end namespace runtime
} // end namespace allscale
