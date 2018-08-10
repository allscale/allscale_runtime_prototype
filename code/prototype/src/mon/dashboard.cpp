/*
 * The main implementation file for the dashboard service.
 *
 *  Created on: Aug 10, 2018
 *      Author: herbert
 */


#include "allscale/runtime/mon/dashboard.h"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>

// headers for IP socket:

#include <cstdlib>
#include <unistd.h>
#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <endian.h>


#include "allscale/utils/printer/vectors.h"

#include "allscale/runtime/data/data_item_manager.h"
#include "allscale/runtime/work/worker.h"

namespace allscale {
namespace runtime {
namespace mon {


	/**
	 * The name of the environment variable checked for the IP address of the dashboard server.
	 */
	constexpr const char* ENVVAR_DASHBOARD_IP = "ART_DASHBOARD_IP";

	/**
	 * The name of the environment variable checked for the IP port number of the dashboard server.
	 */
	constexpr const char* ENVVAR_DASHBOARD_PORT = "ART_DASHBOARD_PORT";

	/**
	 * The default dashboard server IP address.
	 */
	constexpr const char* DEFAULT_DASHBOARD_IP = "127.0.0.1";

	/**
	 * The default port utilized to connect to the dashboard.
	 */
	constexpr int DEFAULT_DASHBOARD_PORT = 1337;


	// -- Node State --


	std::ostream& operator<<(std::ostream& out,const NodeState& state) {
		state.toJSON(out);
		return out;
	}

	void NodeState::toJSON(std::ostream& out) const {
		out << "{";
		out << "\"id\":" << rank << ",";
		out << "\"time\":" << time << ",";
		out << "\"state\":\"" << (online ? "online\"," : "offline\"");
		if (!online) {
			out << "}";
			return;
		}
		out << "\"cpu_load\":" << cpu_load << ",";
		out << "\"mem_load\":" << memory_load << ",";
		out << "\"task_throughput\":" << task_throughput << ",";
		out << "\"weighted_task_througput\":" << weighted_task_throughput << ",";
		out << "\"network_in\":" << network_in << ",";
		out << "\"network_out\":" << network_out << ",";
		out << "\"idle_rate\":" << idle_rate << ",";
		out << "\"owned_data\":";
		ownership.toJSON(out);
		out << "}";
	}

	// -- serialization --

	void NodeState::store(allscale::utils::ArchiveWriter& out) const {
		out.write(time);
		out.write(rank);
		out.write(online);
		if (!online) return;
		out.write(cpu_load);
		out.write(memory_load);
		out.write(task_throughput);
		out.write(weighted_task_throughput);
		out.write(network_in);
		out.write(network_out);
		out.write(idle_rate);
		out.write(ownership);
	}

	NodeState NodeState::load(allscale::utils::ArchiveReader& in) {
		NodeState res;
		res.time = in.read<std::uint64_t>();
		res.rank = in.read<com::rank_t>();
		res.online = in.read<bool>();
		if (!res.online) return res;
		res.cpu_load = in.read<float>();
		res.memory_load = in.read<std::uint64_t>();
		res.task_throughput = in.read<float>();
		res.weighted_task_throughput = in.read<float>();
		res.network_in = in.read<std::uint64_t>();
		res.network_out = in.read<std::uint64_t>();
		res.idle_rate = in.read<float>();
		res.ownership = in.read<data::DataItemRegions>();
		return res;
	}


	// -- Node State Service --

	class NodeStateService {

		using clock = std::chrono::high_resolution_clock;
		using time_point = clock::time_point;
		using duration = clock::duration;

		// the network being part of
		com::Network& network;

		com::Node& localNode;

		// the time this service was started
		time_point startup_time;

		// the last time data has been collected
		time_point last;

		// old network states
		com::Network::Statistics::Entry last_network_state;

		// old worker state
		std::uint64_t lastTaskCount = 0;
		double lastProcessedWork = 0;
		std::chrono::nanoseconds lastProcessTime;

	public:

		NodeStateService(com::Node& node)
			: network(com::Network::getNetwork()),
			  localNode(node),
			  startup_time(clock::now()),
			  last(startup_time),
			  lastProcessTime(0) {}

		NodeState getState() {

			// make a time step
			auto now = clock::now();

			NodeState res;
			res.rank = localNode.getRank();
//			res.time = std::chrono::duration_cast<std::chrono::milliseconds>(now - startup_time).count();
			res.time = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() / 1000;
			res.online = true;

			// -- duration independent values --

			// TODO: get some actual source
			res.cpu_load = 1.0;				// 100% usage
			res.memory_load = (1<<30);		// 1GB

			// fill in data information
			if (localNode.hasService<data::DataItemManagerService>()) {
				auto& dim = localNode.getService<data::DataItemManagerService>();
				res.ownership = dim.getExclusiveRegions();
			}


			// -- duration dependent values --

			// take the time interval
			auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(step(now));
			if (duration.count() <= 0) {
				return res;		// not enough time to capture anything
			}

			// get the interval in seconds (float)
			auto interval = std::chrono::duration_cast<std::chrono::duration<float>>(duration);

			auto calcThroughput = [&](const auto& begin, const auto& end)->float {
				return (end-begin)/interval.count();
			};

			// fill in network information
			{
				auto current_network_state = network.getStatistics()[localNode.getRank()];
				res.network_in  = calcThroughput(last_network_state.received_bytes,current_network_state.received_bytes);
				res.network_out = calcThroughput(last_network_state.sent_bytes,current_network_state.sent_bytes);
				last_network_state = current_network_state;
			}

			// fill in worker information
			if (localNode.hasService<work::Worker>()) {
				auto& worker = localNode.getService<work::Worker>();
				auto curTaskCount = worker.getNumProcessedTasks();
				auto curProcess = worker.getProcessedWork();
				auto processTime = worker.getProcessTime();

				res.task_throughput = calcThroughput(lastTaskCount,curTaskCount);
				lastTaskCount = curTaskCount;

				res.weighted_task_throughput = calcThroughput(lastProcessedWork,curProcess);
				lastProcessedWork = curProcess;

				res.idle_rate = 1 - ((processTime - lastProcessTime) / interval);
				lastProcessTime = processTime;
			}

			// done
			return res;
		}

	private:

		duration step(const time_point& now) {
			auto res = now - last;
			last = now;
			return res;
		}

	};


	// -- Dashboard Service --

	class DashboardService {

		// the network being part of
		com::Network& network;

		// the node being installed on
		com::Node& node;

		// TODO: maybe add a general timer service?

		// the thread periodically collecting data and sending it to the dashboard
		std::thread thread;

		// flag indicating whether this service is still alive
		std::atomic<bool> alive;

		// condition variable to communicate with reporting thread
		std::mutex mutex;
		std::condition_variable condition_var;

		using guard = std::lock_guard<std::mutex>;

		// -- connection to dashboard server --
		int sock;

	public:

		DashboardService(com::Node& node)
			: network(com::Network::getNetwork()),
			  node(node),
			  alive(node.getRank() == 0) {

			// only on node 0
			if (!alive) return;

			// try to get a connection to the dashboard server

			// create a client socket
			sock = socket(AF_INET, SOCK_STREAM, 0);
			if (sock < 0) {
				alive = false;
				return;
			}

			// get dashboard IP address
			std::string dashboardIP = DEFAULT_DASHBOARD_IP;
			if (auto ip = std::getenv(ENVVAR_DASHBOARD_IP)) {
				dashboardIP = ip;
			}

			// get dashboard port
			int dashboardPort = DEFAULT_DASHBOARD_PORT;
			if (auto port = std::getenv(ENVVAR_DASHBOARD_PORT)) {
				dashboardPort = std::atoi(port);
			}

			// create server address
			sockaddr_in serverAddress;
			serverAddress.sin_family = AF_INET;
			serverAddress.sin_port = htons(dashboardPort);
			auto success = inet_pton(AF_INET, dashboardIP.c_str(), &serverAddress.sin_addr);
			if (!success) {
				std::cerr << "Ignoring dashboard at unsupported address: " << dashboardIP << "\n";
				alive = false;
				return;
			}

			// connect to server
			success = connect(sock,reinterpret_cast<sockaddr*>(&serverAddress),sizeof(serverAddress));
			if (success < 0) {
				std::cerr << "Unable to connect to dashboard server at " << dashboardIP << ":" << dashboardPort << ", reporting disabled.\n";
				alive = false;
				return;
			}

			// start up reporter thread
			if (!alive) return;

			// start thread
			thread = std::thread([&]{ run(); });
		}

		~DashboardService() {
			if (!alive) return;

			// set alive to false
			{
				guard g(mutex);
				alive = false;
			}

			// signal change to worker
			condition_var.notify_all();

			// wait for thread to finish
			thread.join();

			// also handle the socket
			close(sock);
		}

	private:

		void run() {
			using namespace std::literals::chrono_literals;
			while(true) {
				std::unique_lock<std::mutex> g(mutex);
				condition_var.wait_for(g, 1s,[&](){ return !alive; });
				if (!alive) return;
				node.run([&](com::Node&){
					update();
				});
			}
		}

		void update() {

			// collect data
			auto data = getSystemState(network);
			auto time = data[0].time;

			// create JSON data block
			std::stringstream msg;
			msg << "{\"time\":" << time << ",\"nodes\":" << data << "}";

			// sent to bashboard

			// get as string
			auto json = msg.str();

			// create send buffer
			auto bufferSize = sizeof(std::uint64_t) + json.length();
			char* buffer = new char[bufferSize];

			// copy message to buffer
			auto msgSizeBE = htobe64(json.length());		// conversion to big-endian
			memcpy(buffer,&msgSizeBE,sizeof(std::uint64_t));
			memcpy(buffer+sizeof(std::uint64_t),json.c_str(),json.size());

			// send message (several times)
			write(sock,buffer,bufferSize);

			// free buffer
			delete [] buffer;

		}
	};


	void installDashbordService(com::Network& net) {
		net.installServiceOnNodes<NodeStateService>();
		net.installServiceOnNodes<DashboardService>();
	}


	void shutdownDashbordService(com::Network& net) {
		net.removeServiceOnNodes<DashboardService>();
		net.removeServiceOnNodes<NodeStateService>();
	}

	std::vector<NodeState> getSystemState(com::Network& net) {

		// simply collect state from all nodes
		std::vector<NodeState> res;
		for(com::rank_t cur = 0; cur < net.numNodes(); cur++) {
			res.push_back(net.getRemoteProcedure(cur,&NodeStateService::getState)());
		}

		// done
		return res;
	}

} // end namespace log
} // end namespace runtime
} // end namespace allscale
