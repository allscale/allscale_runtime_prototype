#pragma once

#include <atomic>

namespace allscale {
namespace utils {


	/**
	 * A simple implementation of a spin lock.
	 */
	class spinlock {

		// the atomic flag the lock is based on
		std::atomic_flag state;

	public:

		spinlock() { state.clear(); }

		void lock() {
			// spin until acquired
			while(state.test_and_set(std::memory_order_acquire)) ;
		}

		bool try_lock() {
			return !state.test_and_set(std::memory_order_acquire);
		}

		void unlock() {
			state.clear(std::memory_order_release);
		}

	};


} // end namespace utils
} // end namespace allscale
