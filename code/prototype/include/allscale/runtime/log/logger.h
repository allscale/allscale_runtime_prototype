/*
 * A simple logger utility to log events in the runtime.
 *
 *  Created on: Jul 25, 2018
 *      Author: herbert
 */

#pragma once

#include <mutex>

#include <iostream>

#include "allscale/runtime/com/node.h"

namespace allscale {
namespace runtime {
namespace log {

	// a switch to enable/disable logging
	#ifndef ENABLE_LOGGING
		#define ENABLE_LOGGING false
	#endif

	// the lock to sync logging messages
	extern std::mutex g_log_lock;

	// a utility to guard
	struct log_guard {
		std::mutex& m;
		log_guard(std::mutex& m) : m(m) { if (ENABLE_LOGGING) m.lock(); };
		~log_guard() { if (ENABLE_LOGGING) m.unlock(); }
		operator bool() { return ENABLE_LOGGING; }
	};

	// a macro to log messages in a thread-save manor
	#define DLOG if(allscale::runtime::log::log_guard g = allscale::runtime::log::g_log_lock) \
		std::cerr << "Node: " << allscale::runtime::com::Node::getLocalRank() << ": "
//		std::cerr << "Node: " << allscale::runtime::com::Node::getLocalRank() << " - " << __FILE__ << ":" << __LINE__ << ": "



} // end namespace log
} // end namespace runtime
} // end namespace allscale



