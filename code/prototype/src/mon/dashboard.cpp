/*
 * The main implementation file for the dashboard service.
 *
 *  Created on: Aug 10, 2018
 *      Author: herbert
 */


#include "allscale/runtime/mon/dashboard.h"

#include <atomic>
#include <chrono>

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

#include "allscale/runtime/com/statistics.h"
#include "allscale/runtime/data/data_item_manager.h"
#include "allscale/runtime/utils/timer.h"
#include "allscale/runtime/work/worker.h"
#include "allscale/runtime/work/balancer.h"
#include "allscale/runtime/work/optimizer.h"

#include "allscale/runtime/hw/frequency_scaling.h"

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
		out << "\"state\":\"" << (online ? (active ? "active\"," : "standby\",") : "offline\"");
		if (!online) {
			out << "}";
			return;
		}
		out << "\"num_cores\":" << num_cores << ",";
		out << "\"cpu_load\":" << cpu_load << ",";
		out << "\"max_frequency\":" << max_frequency.toHz() << ",";
		out << "\"cur_frequency\":" << cur_frequency.toHz() << ",";
		out << "\"mem_load\":" << memory_load << ",";
		out << "\"total_memory\":" << total_memory << ",";
		out << "\"task_throughput\":" << task_throughput << ",";
		out << "\"weighted_task_througput\":" << weighted_task_throughput << ",";
		out << "\"network_in\":" << network_in << ",";
		out << "\"network_out\":" << network_out << ",";
		out << "\"idle_rate\":" << idle_rate << ",";
		out << "\"productive_cycles_per_second\":" << productive_cycles_per_second << ",";
		out << "\"cur_power\":" << cur_power.toWatt() << ",";
		out << "\"max_power\":" << max_power.toWatt() << ",";
		out << "\"speed\":" << speed << ",";
		out << "\"efficiency\":" << efficiency << ",";
		out << "\"power\":" << power << ",";
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
		out.write(active);
		out.write(cpu_load);
		out.write(cur_frequency);
		out.write(max_frequency);
		out.write(memory_load);
		out.write(total_memory);
		out.write(task_throughput);
		out.write(weighted_task_throughput);
		out.write(network_in);
		out.write(network_out);
		out.write(idle_rate);
		out.write(ownership);
		out.write(productive_cycles_per_second);
		out.write(cur_power);
		out.write(max_power);
		out.write(speed);
		out.write(efficiency);
		out.write(power);
	}

	NodeState NodeState::load(allscale::utils::ArchiveReader& in) {
		NodeState res;
		res.time = in.read<std::uint64_t>();
		res.rank = in.read<com::rank_t>();
		res.online = in.read<bool>();
		if (!res.online) return res;
		res.active = in.read<bool>();
		res.cpu_load = in.read<float>();
		res.cur_frequency = in.read<hw::Frequency>();
		res.max_frequency = in.read<hw::Frequency>();
		res.memory_load = in.read<std::uint64_t>();
		res.total_memory = in.read<std::uint64_t>();
		res.task_throughput = in.read<float>();
		res.weighted_task_throughput = in.read<float>();
		res.network_in = in.read<std::uint64_t>();
		res.network_out = in.read<std::uint64_t>();
		res.idle_rate = in.read<float>();
		res.ownership = in.read<data::DataItemRegions>();
		res.productive_cycles_per_second = in.read<hw::cycles>();
		res.cur_power = in.read<hw::Power>();
		res.max_power = in.read<hw::Power>();
		res.speed = in.read<float>();
		res.efficiency = in.read<float>();
		res.power = in.read<float>();
		return res;
	}


	// -- system state --

	std::ostream& operator<<(std::ostream& out,const SystemState& state) {
		state.toJSON(out);
		return out;
	}

	void SystemState::toJSON(std::ostream& out) const {
		out << "{";
		out << "\"type\" : \"status\",";
		out << "\"time\" : " << time << ",";
		out << "\"speed\":" << speed << ",";
		out << "\"efficiency\":" << efficiency << ",";
		out << "\"power\":" << power << ",";
		out << "\"objective_exponent\": {";
			out << "\"speed\":" << objective.getSpeedExponent() << ",";
			out << "\"efficiency\":" << objective.getEfficiencyExponent() << ",";
			out << "\"power\":" << objective.getPowerExponent();
		out << "},";
		out << "\"score\":" << score << ",";
		out << "\"scheduler\": \"" << scheduler << "\",";
		out << "\"nodes\" : " << nodes;
		out << "}";
	}

	namespace {

		using clock = std::chrono::high_resolution_clock;
		using time_point = clock::time_point;
		using duration = clock::duration;

		std::uint64_t getCurrentTime(const time_point& now) {
			return std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() / 1000;
		}

		std::uint64_t getCurrentTime() {
			return getCurrentTime(clock::now());
		}

		struct CPULoadSensor {

			std::array<long double,4> a = {{0}};

			float getCPUUsage() {

				// the file contains:
				// cpu <usr> <nice> <sys> <idle> ...

				// get current snapshot
				std::array<long double,4> b;
				FILE *fp;
				fp = fopen("/proc/stat","r");
				int count = fscanf(fp,"%*s %Lf %Lf %Lf %Lf",&b[0],&b[1],&b[2],&b[3]);
				if (count != 4) { assert_fail() << "Unable to read values from /proc/stat\n"; }
				fclose(fp);

				// if this is the first round => nothing to report
				if (a[0] == 0) {
					a = b;
					return 0.0f;
				}

				// compute system load
				float res = ((b[0]+b[1]+b[2]) - (a[0]+a[1]+a[2])) / ((b[0]+b[1]+b[2]+b[3]) - (a[0]+a[1]+a[2]+a[3]));

				// remember current state
				a = b;

				// return result
				return res;

			}

		};

		std::pair<std::uint64_t,std::uint64_t> getMemoryUsage() {

			int count;
			std::uint64_t total, free, available;

			FILE *fp;
			fp = fopen("/proc/meminfo","r");
			count = fscanf(fp,"%*s %lud", &total);
			if (count != 1) { assert_fail() << "Unable to read value from /proc/meminfo\n"; }
			count = fscanf(fp,"%*s");
			if (count != 0) { assert_fail() << "Unable to read value from /proc/meminfo\n"; }

			count = fscanf(fp,"%*s %lud", &free);
			if (count != 1) { assert_fail() << "Unable to read value from /proc/meminfo\n"; }
			count = fscanf(fp,"%*s");
			if (count != 0) { assert_fail() << "Unable to read value from /proc/meminfo\n"; }

			count = fscanf(fp,"%*s %lud", &available);
			if (count != 1) { assert_fail() << "Unable to read value from /proc/meminfo\n"; }
			count = fscanf(fp,"%*s");
			if (count != 0) { assert_fail() << "Unable to read value from /proc/meminfo\n"; }

			fclose(fp);

			return std::make_pair((total - available)*1024,available*1024);
		}

	}


	namespace {

		template<typename T, std::size_t size>
		class MeasureBuffer {

			std::array<T,size> data {};

			std::array<time_point,size> times;

			std::size_t counter = 0;
			std::size_t next = 0;

		public:

			void push(const T& value, const time_point& now) {
				data[next] = value;
				times[next] = now;
				next = (next + 1) % size;
				counter++;
			}

			const T& getOldest() {
				return data[getOldestPosition()];
			}

			const time_point& getOldestTime() {
				return times[getOldestPosition()];
			}

		public:

			std::size_t getOldestPosition() const {
				return (counter < size) ? 0 : next;
			}

		};

	}


	// -- Node State Service --

	class NodeStateService {

		// the network being part of
		com::Network& network;

		com::Node& localNode;

		// the time this service was started
		time_point startup_time;

		// the last time data has been collected
		time_point last;

		// old network states
		com::NodeStatistics last_network_state;

		// old worker state
		std::uint64_t lastTaskCount = 0;
		double lastProcessedWork = 0;

		// a roughly X-second work buffer (X observation samples)
		MeasureBuffer<std::chrono::nanoseconds,1> processTimeBuffer;

		CPULoadSensor cpu_sensor;

		hw::Energy lastEnergy;

	public:

		NodeStateService(com::Node& node)
			: network(com::Network::getNetwork()),
			  localNode(node),
			  startup_time(clock::now()),
			  last(startup_time) {}

		NodeState getState() {

			using namespace std::literals::chrono_literals;

			// make a time step
			auto now = clock::now();

			NodeState res;
			res.rank = localNode.getRank();
			res.time = getCurrentTime(now);
			res.online = true;

			// determine scheduling state
			res.active = work::isLocalNodeActive();

			// -- duration independent values --

			// TODO: get some actual source
			res.cpu_load = cpu_sensor.getCPUUsage();

			auto mem = getMemoryUsage();
			res.memory_load = mem.first;
			res.total_memory = mem.second;

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
			if (localNode.hasService<work::WorkerPool>()) {
				auto& pool = localNode.getService<work::WorkerPool>();
				auto numWorker = pool.getNumWorkers();
				auto curTaskCount = pool.getNumProcessedTasks();
				auto curProcess = pool.getProcessedWork();
				auto processTime = pool.getProcessTime();

				res.task_throughput = calcThroughput(lastTaskCount,curTaskCount);
				lastTaskCount = curTaskCount;

				res.weighted_task_throughput = calcThroughput(lastProcessedWork,curProcess);
				lastProcessedWork = curProcess;

				// compute idle rate over entire observed interval
				res.idle_rate = 1 - ((processTime - processTimeBuffer.getOldest())/std::chrono::duration_cast<std::chrono::duration<float>>(now - processTimeBuffer.getOldestTime()) / numWorker);

				// aggregate process time
				processTimeBuffer.push(processTime, now);

			} else {

				// no worker -> all idle
				res.idle_rate = 1;

			}

			// -- frequency --

			res.cur_frequency = hw::getFrequency(res.rank);
			res.max_frequency = hw::getFrequencyOptions(res.rank).back();

			// -- power --

			// TODO: factor in CPU core idle rate in cur power
			res.max_power = (hw::estimateEnergyUsage(res.max_frequency,res.max_frequency * 1s) * res.num_cores) / 1s;

			if (hw::providesEnergyMeasurement(res.rank)) {
				auto currentEnergy = hw::getEnergyConsumedOn(res.rank);
				res.cur_power = (currentEnergy - lastEnergy) / interval;
				lastEnergy = currentEnergy;
			} else {
				// estimated power consumption
				res.cur_power = res.active ? (hw::estimateEnergyUsage(res.cur_frequency,res.cur_frequency * 1s) * res.num_cores) / 1s : res.max_power * 0.05;
			}


			// the number of productive cycles
			res.productive_cycles_per_second = res.cur_frequency.toHz() * (1-res.idle_rate);

			// compute aggregated values
			res.speed = res.productive_cycles_per_second / float(res.max_frequency.toHz());
			res.efficiency = res.productive_cycles_per_second / float(res.cur_frequency.toHz());
			res.power = res.cur_power / res.max_power;


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

		// a flag determining whether this service is connected to a dashboard
		std::atomic<bool> alive;

		std::atomic<bool> shutdown;

		// -- connection to dashboard server --

		int sock;

		std::mutex socket_lock;

		using guard = std::lock_guard<std::mutex>;

	public:

		DashboardService(com::Node& node)
			: network(com::Network::getNetwork()),
			  node(node),
			  alive(node.getRank() == 0),
			  shutdown(false) {

			// only on node 0
			if (!alive) return;

			// try to get a connection to the dashboard server

			// create a client socket
			sock = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
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
			node.getService<utils::PeriodicExecutorService>().runPeriodically(
				[&]()->bool { if (alive && !shutdown) update(); return alive; },
				std::chrono::seconds(1)
			);
		}

		~DashboardService() {
			if (!alive) return;

			// move to shutdown mode
			shutdown = true;

			// send final info
			sendShutdownInfo();

			// also handle the socket
			close(sock);
		}

	private:

		void update() {
			node.run([&](com::Node&){

				// retrieve commands
				processCommands();

				// collect data
				auto data = getSystemState(network);

				// send data
				sendUpdate(data);

			});

		}

		void processCommands() {

			// consume all commands received
			while(alive) {

				// set up time to wait for input
				timeval time;
				time.tv_sec = 0;
				time.tv_usec = 0;

				// set up input socket
				fd_set set;
				FD_ZERO(&set);
				FD_SET(sock,&set);

				// test if commands are available
				auto numReady = select(sock+1,&set,nullptr,nullptr,&time);

				// test for errors
				if (numReady == -1) {
					std::cerr << "Lost dashboard back-channel connection, ending dashboard communication.";
					alive = false;
					continue;
				}

				// stop processing if no input is ready
				if (numReady == 0) break;

				// -- process input command --

				// read message size
				std::uint64_t msgSize;
				if (read(sock,&msgSize,sizeof(msgSize)) != sizeof(msgSize)) {
					std::cerr << "Error reading incoming command size, ending dashboard communication.";
					alive = false;
					continue;
				}

				// correct endian
				msgSize = be64toh(msgSize);

				// load message
				std::string msg(msgSize,' ');
				if (read(sock,&msg[0],msgSize) != int(msgSize)) {
					std::cerr << "Error reading incoming command, ending dashboard communication.";
					alive = false;
					continue;
				}

				// parse input command
				auto endOfCmd = std::find(msg.begin(),msg.end(),' ');
				auto cmd = std::string(msg.begin(),endOfCmd);

				// interpret command
				if (cmd == "set_scheduler") {

					// get target schedule type
					auto type = std::string(endOfCmd+1,msg.end());

					work::SchedulerType newType;
					if (type == "uniform") {
						newType = work::SchedulerType::Uniform;
					} else if (type == "balanced") {
						newType = work::SchedulerType::Balanced;
					} else if (type == "tuned") {
						newType = work::SchedulerType::Tuned;
					} else if (type == "random") {
						newType = work::SchedulerType::Random;
					} else {
						std::cerr << "Invalid scheduler type received: " << type << "\n";
						continue;
					}

					// update the scheduler type
					work::setCurrentSchedulerType(newType);

				} else if (cmd == "toggle_node") {

					// get target node
					auto node = std::string(endOfCmd+1,msg.end());
					work::toggleActiveState(std::atoi(node.c_str()));

				} else if (cmd == "set_speed" || cmd == "set_efficiency" || cmd == "set_power" ) {

					// parse new exponent
					auto weight = std::string(endOfCmd+1,msg.end());
					auto exponent = std::atof(weight.c_str());

					// update tuning objective
					auto obj = work::getActiveTuningObjectiv();
					if (cmd == "set_speed") {
						obj.setSpeedExponent(exponent);
					} else if (cmd == "set_efficiency") {
						obj.setEfficiencyExponent(exponent);
					} else if (cmd == "set_power") {
						obj.setPowerExponent(exponent);
					}
					work::setActiveTuningObjectiv(obj);

				} else {
					std::cout << "Received unsupported command: " << cmd << "\n";
					std::cout << "Command is ignored.\n";
				}

			}

		}

		void sendShutdownInfo() {

			// create shutdown info
			SystemState info;
			info.time = getCurrentTime() + 1;  // to make sure it does not collide with the last update
			for(com::rank_t i = 0; i<network.numNodes(); i++) {
				NodeState state;
				state.rank = i;
				state.time = info.time;
				state.online = false;
				info.nodes.push_back(state);
			}

			// send shutdown info
			sendUpdate(info,true);

		}

		void sendUpdate(const SystemState& state, bool isShutdownMsg = false) {

			// create JSON data block
			std::stringstream msg;
			state.toJSON(msg);

			// get as string
			auto json = msg.str();

//			std::cout << "Sending:\n" << json << "\n\n";

			// sent to bashboard
			auto mySend = [&](const void* msg, int size) {
				if (!alive) return;

				const std::uint8_t* buffer = reinterpret_cast<const std::uint8_t*>(msg);
				while(size > 0) {
					int rv = send(sock,buffer,size,MSG_NOSIGNAL);
					if (rv < 0) {
						std::cerr << "Lost dashboard connection, ending status broadcasts.\n";
						std::cerr << "Problem: " << strerror(errno) << "\n";
						alive = false;
						size = 0;
					}
					size -= rv;
					buffer += rv;
				}

			};

			// serialized access on socket from here
			guard g(socket_lock);

			// do not send this message if system is shutting down
			if (shutdown && !isShutdownMsg) return;

			// send message size
			std::uint64_t msgSizeBE = htobe64(json.length());
			mySend(&msgSizeBE,sizeof(std::uint64_t));

			// send message
			mySend(json.c_str(),json.length());

		}
	};


	void installDashbordService(com::Network& net) {
		net.installServiceOnNodes<NodeStateService>();
		net.sync();	// wait until node state services are installed
		net.installServiceOnNodes<DashboardService>();
	}


	void shutdownDashbordService(com::Network& net) {
		net.removeServiceOnNodes<DashboardService>();
		net.removeServiceOnNodes<NodeStateService>();
	}

	SystemState getSystemState(com::Network& net) {

		using namespace std::literals::chrono_literals;

		// simply collect state from all nodes
		SystemState res;
		for(com::rank_t cur = 0; cur < net.numNodes(); cur++) {
			res.nodes.push_back(net.getRemoteProcedure(cur,&NodeStateService::getState)().get());
		}

		// update global state
		res.time = res.nodes[0].time;

		// compute overall speed
		std::uint64_t total_productive =0;
		std::uint64_t total_available =0;
		std::uint64_t max_available =0;

		hw::Power cur_power = 0;
		hw::Power max_power = 0;

		for(const auto& cur : res.nodes) {
			if (cur.online && cur.active) {
				total_productive += cur.productive_cycles_per_second;
				total_available += cur.num_cores * cur.cur_frequency * 1s;
				cur_power += cur.cur_power;
			}
			max_available += cur.num_cores * cur.max_frequency * 1s;
			max_power += cur.max_power;
		}

		// compute system-wide ratings
		res.speed = (max_available > 0) ? total_productive / float(max_available) : 0;
		res.efficiency = (total_available > 0) ? total_productive / float(total_available) : 0;
		res.power = (max_power > 0) ? cur_power / max_power : 0;

		// get tuning objective
		res.objective = work::getActiveTuningObjectiv();

		// compute score based on objective function
		res.score = res.objective.getScore(res.speed,res.efficiency,res.power);

		// add scheduler information
		res.scheduler = work::getCurrentSchedulerType();

		// done
		return res;
	}

} // end namespace log
} // end namespace runtime
} // end namespace allscale
