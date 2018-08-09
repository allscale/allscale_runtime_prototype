#include <gtest/gtest.h>

#include "allscale/runtime/mon/dashboard.h"

#include <string>

#include <stdio.h>
#include <sys/socket.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <endian.h>

namespace allscale {
namespace runtime {
namespace mon {

	const std::string example_msg = R"(
			{
			  "time": 1234,
			  "nodes": [
			    {
			      "id": 0,
			      "state": "online",
			      "cpu_load": 0.8,
			      "mem_load": 2423435,
			      "task_throughput": 423,
			      "weighted_task_througput": 4.23,
			      "network_in": 232134,
			      "network_out": 14234,
			      "idle_rate": 0.123,
			      "owned_data": [
			        {
			          "id": 12,
			          "type": "Grid<int,2>",
			          "region": [
			            {
			              "from": "[1,2,3]",
			              "to": "[3,4,5]"
			            },
			            {
			              "from": "[1,2,3]",
			              "to": "[3,4,5]"
			            }
			          ]
			        },
			        {
			          "id": 14,
			          "type": "Grid<int,2>",
			          "region": [
			            {
			              "from": "[1,2,3]",
			              "to": "[3,8,5]"
			            }
			          ]
			        }
			      ]
			    },
			    {
			      "id": 1,
			      "state": "offline"
			    },
			    {
			      "id": 2,
			      "state": "online",
			      "cpu_load": 0.9,
			      "mem_load": 2423435,
			      "task_throughput": 423,
			      "weighted_task_througput": 4.23,
			      "network_in": 232134,
			      "network_out": 14234,
			      "idle_rate": 0.012,
			      "owned_data": []
			    },
			    {
			      "id": 3,
			      "state": "online",
			      "cpu_load": 0.798,
			      "mem_load": 2423435,
			      "task_throughput": 423,
			      "weighted_task_througput": 4.23,
			      "network_in": 232134,
			      "network_out": 14234,
			      "idle_rate": 0.33,
			      "owned_data": []
			    }
			  ]
			})";



	TEST(TCP_Connection_Test, ClientConnect) {

		// create a client socket
		int sock = socket(AF_INET, SOCK_STREAM, 0);
		if (sock < 0) {
			std::cout << "WARNING: failed to create socket, stopping test.\n";
			return;
		}

		// create server address
		sockaddr_in serverAddress;
		serverAddress.sin_family = AF_INET;
		serverAddress.sin_port = htons(DEFAULT_DASHBOARD_PORT);
		auto success = inet_pton(AF_INET, DEFAULT_DASHBOARD_IP, &serverAddress.sin_addr);
		EXPECT_GT(success,0) << "Invalid address: " << DEFAULT_DASHBOARD_IP;

		// connect to server
		success = connect(sock,reinterpret_cast<sockaddr*>(&serverAddress),sizeof(serverAddress));
		if (success < 0) {
			std::cout << "Unable to connect to server, is it offline?\nStopping unit test.\n";
			return;
		}

		// create send buffer
		auto bufferSize = sizeof(std::uint64_t) + example_msg.length();
		char* buffer = new char[bufferSize];

		// copy message to buffer
		auto msgSizeBE = htobe64(example_msg.length());		// conversion to big-endian
		memcpy(buffer,&msgSizeBE,sizeof(std::uint64_t));
		memcpy(buffer+sizeof(std::uint64_t),example_msg.c_str(),example_msg.size());

		// send message (several times)
		for(int i=0; i<3; i++) {
			write(sock,buffer,bufferSize);
			sleep(1);
		}

		// free buffer
		delete [] buffer;

		// close socket
		close(sock);
	}

} // end of namespace mon
} // end of namespace runtime
} // end of namespace allscale
