#pragma once

#include <atomic>

#include <allscale/utils/assert.h>
#include <allscale/utils/spinlock.h>

#include "allscale/utils/fibers.h"

namespace allscale {
namespace utils {
namespace fiber {

	// -------- Fiber Read/Write Locks ------------

	class ReadWriteLock;

	class ReadGuard;

	class WriteGuard;


	// --------------------------------------------

	namespace detail {

		struct non_movable {

			non_movable() = default;

			non_movable(const non_movable&) = delete;
			non_movable(non_movable&&) = delete;

			non_movable& operator=(const non_movable&) = delete;
			non_movable& operator=(non_movable&&) = delete;

		};

	}

	/**
	 * A read/write lock permitting multiple reads and exclusive writes. Once a write
	 * is issued, reads are blocked until the write is completed. Multiple write requests
	 * may starve.
	 */
	class ReadWriteLock : private detail::non_movable {

		/**
		 * Derived from: https://github.com/souffle-lang/souffle/blob/master/src/ParallelUtils.h
		 *
		 * Based on paper:
		 *         Scalable Reader-Writer Synchronization
		 *         for Shared-Memory Multiprocessors
		 *
		 * Layout of the lock:
		 *      31        ...             2                    1                    0
		 *      +-------------------------+--------------------+--------------------+
		 *      | interested reader count |   waiting writer   | active writer flag |
		 *      +-------------------------+--------------------+--------------------+
		 */

		// a lock protecting the internal state
		mutable spinlock syncLock;

		// a guard for the internal state synchronization
		using guard = std::lock_guard<spinlock>;

		// the internally maintained lock state
		uint32_t lck = 0;

		// a condition variable to suspend and resume readers
		ConditionalVariable readers;

		// a condition variable to suspend and resume writers
		ConditionalVariable writers;

	public:

		/**
		 * Requests a read permission. Careful: requesting a read permission while
		 * already owning a write permission will lead to a deadlock. Also, requesting
		 * a read permission while owning a read permission can lead to a deadlock
		 * if in the meanwhile another thread has starting its request for a write
		 * permission.
		 */
		void startRead() {
			guard g(syncLock);

			// wait until there is no writer any more
			while(lck & 0x3) {
				readers.wait(syncLock);
			}

			// register a new reader
			lck += 4;
		}

		bool tryStartRead() {
			guard g(syncLock);

			// stop if there is a (waiting) writer
			if (lck & 0x3) return false;

			// add another reader
			lck += 4;
			return true;
		}

		void endRead() {
			guard g(syncLock);
			lck -= 4;
			if ((lck >> 2) == 0) writers.notifyAll();
		}

		bool isReadLocked() const {
			guard g(syncLock);
			return lck > 3;
		}

		void startWrite() {
			guard g(syncLock);

			// wait until writer bit can be set
			while( lck & 0x2 ) {
				writers.wait(syncLock);
			}

			// set request-to-write bit
			lck = lck | 0x2;

			// wait until there are no readers or writers any more
			while( lck != 2 ) {
				writers.wait(syncLock);
			}

			// mark as active writer
			lck = 1;

		}

		bool tryStartWrite() {
			guard g(syncLock);
			if ( lck != 0 ) return false;
			lck = 1;
			return true;
		}

		void endWrite() {
			guard g(syncLock);
			// remove writer bit
			lck = lck & (~1);
			// signal waiting writers that access may now be possible
			writers.notifyAll();
			// signal all readers that access may be available
			readers.notifyAll();
		}

		bool isWriteLocked() const {
			guard g(syncLock);
			return lck & 0x1;
		}
	};

	/**
	 * A read guard, to scope read access on shared data using a read/write lock.
	 */
	class ReadGuard : private detail::non_movable {

		ReadWriteLock& lock;

	public:

		ReadGuard(ReadWriteLock& lock) : lock(lock) {
			lock.startRead();
		}

		~ReadGuard() {
			lock.endRead();
		}

	};

	/**
	 * A write guard, to scope write access on shared data using a read/write lock.
	 */
	class WriteGuard : private detail::non_movable {

		ReadWriteLock& lock;

	public:

		WriteGuard(ReadWriteLock& lock) : lock(lock) {
			lock.startWrite();
		}

		~WriteGuard() {
			lock.endWrite();
		}

	};

} // end of namespace fiber
} // end of namespace utils
} // end of namespace allscale
