/*
 * The facade of the prototype runtime.
 *
 *  Created on: Jul 24, 2018
 *      Author: herbert
 */

#pragma once

#include "allscale/runtime/com/network.h"

namespace allscale {
namespace runtime {

	/**
	 * A class managing a runtime instance.
	 */
	class Runtime {

		// the network of nodes to be included
		com::Network& network;

	public:

		/**
		 * Creates a new runtime instance with the given number of nodes.
		 */
		Runtime(com::Network&);

		// --- interaction ---

		/**
		 * Processes the shutdown of the runtime. Blocks until completed.
		 */
		void shutdown();

		// --- observers ---

		/**
		 * Obtains access to the managed network.
		 */
		com::Network& getNetwork() {
			return network;
		}
	};

} // end namespace runtime
} // end namespace allscale
