#pragma once

#include <atomic>
#include <cstdint>
#include <stdlib.h>
#include <ucontext.h>
#include <mutex>
#include <vector>

#include <sys/types.h>
#include <unistd.h>

#include <sys/mman.h>

#include "allscale/utils/assert.h"
#include "allscale/utils/optional.h"
#include "allscale/utils/printer/vectors.h"

namespace allscale {
namespace utils {

	/**
	 * A utility class for handling multiple thread contexts using
	 * to be processed by externally managed threads concurrently.
	 *
	 * Unlike a thread pool, this pool only covers contexts, and does
	 * not maintain independent OS level threads. The degree of
	 * concurrent execution / parallelism is determined by the number
	 * of threads interacting with this pool.
	 */
	class FiberPool {

		constexpr static bool DEBUG = false;

		// we use 8 MB per fiber stack
		constexpr static std::size_t DEFAULT_STACK_SIZE = (1<<23);

		// the management information required per fiber
		struct fiber_info {
			FiberPool& pool;			// < the pool it belongs to
			std::atomic<bool> running;	// < the running state
			void* stack;				// < the associated stack memory
			const int stack_size;		// < the size of the stack
			ucontext_t* continuation;	// < an optional continuation to be processed after finishing or suspending a fiber

			fiber_info(FiberPool& pool, int stackSize = DEFAULT_STACK_SIZE)
				: pool(pool),
				  running(false),
				  stack(nullptr),
				  stack_size(stackSize),
				  continuation(nullptr) {

				// allocate stack memory
				stack = (stack_t*)mmap(nullptr,stackSize,PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_STACK | MAP_32BIT | MAP_GROWSDOWN | MAP_ANONYMOUS,0,0);

				// provide proper error reporting
				if (stack == MAP_FAILED) {
					std::cerr << "Can not allocate stack of size " << stackSize << " byte: ";
					switch(errno) {
					case ENOMEM: std::cerr << "not enough memory"; break;
					default: std::cerr << "errno code " << errno << " (see errno.h on your system)"; break;
					}
					// terminate program (unrecoverable)
					exit(1);
				}

				assert_ne(MAP_FAILED,stack) << "Error code: " << errno << "\n";
				assert_true(stack);
			}

			fiber_info(const fiber_info&) = delete;
			fiber_info(fiber_info&& other) = delete;
			~fiber_info() {
				munmap(stack,sizeof(stack_t));
			}

		};

		// the list of all fiber infos associated to this pool
		std::vector<fiber_info*> infos;

		// the list of free fiber info entries (reuse list)
		std::vector<fiber_info*> freeInfos;

		// a lock to synchronize accesses to the free lists
		mutable std::mutex lock;

		// the guard type used for protection operations
		using guard = std::lock_guard<std::mutex>;

	public:

		using Fiber = fiber_info*;

		FiberPool(std::size_t initialSize = 10) {
			// create an initial pool of fiber infos
			infos.reserve(initialSize);
			freeInfos.reserve(initialSize);
			for(std::size_t i=0; i<initialSize; i++) {
				infos.push_back(new fiber_info(*this));
				assert_false(infos.back()->running);
				freeInfos.push_back(infos.back());
			}
		}

		FiberPool(const FiberPool&) = delete;
		FiberPool(FiberPool&&) = delete;

		~FiberPool() {
			// clear owned fiber info entries
			for(const auto& cur : infos) {
				// make sure processing has completed!
				assert_false(cur->running) << "Incomplete fiber encountered during destruction.";
				delete cur;
			}
		}

	private:

		static std::mutex*& getMutexLock() {
			thread_local static std::mutex* tl_lock = nullptr;
			return tl_lock;
		}

		static void setMutexLock(std::mutex* newLock) {
			auto& lock = getMutexLock();
			assert_false(lock);
			lock = newLock;
		}

		static void clearMutexLock() {
			auto& lock = getMutexLock();
			if (!lock) return;
			lock->unlock();
			lock = nullptr;
		}

	public:


		/**
		 * Starts the processing of a new fiber by executing the given lambda.
		 * The calling thread will start processing a new fiber until the fiber
		 * is either suspended or terminated. In this case, control flow
		 * will return to the caller.
		 *
		 * @param lambda the lambda to be processed by the created fiber
		 * @return the id of the fiber created if the fiber got suspended, or nothing if the fiber terminated.
		 */
		template<typename Fun>
		allscale::utils::optional<Fiber> start(Fun&& lambda) {

			// get fiber info
			fiber_info* fiber;
			{
				guard g(lock);

				// re-use an existing fiber or create a new one
				if (!freeInfos.empty()) {
					fiber = freeInfos.back();
					freeInfos.pop_back();
					assert_false(fiber->running) << "Faulty fiber: " << fiber;
				} else {
					fiber = new fiber_info(*this);
					infos.push_back(fiber);
					assert_false(fiber->running) << "Faulty fiber: " << fiber;
				}
			}

			assert_true(fiber);

			// make sure the fiber is not running
			assert_false(fiber->running) << "Faulty fiber: " << fiber;

			// capture current context
			ucontext_t local;
			getcontext(&local);

			// register local context as continuation
			fiber->continuation = &local;

			// create a context for the new fiber
			ucontext_t target = local;
			target.uc_link = &local;

			// exchange stack
			target.uc_stack.ss_sp = fiber->stack;
			target.uc_stack.ss_size = fiber->stack_size;

			// encode pool pointer and pointer to lambda into integers
			static_assert(2*sizeof(int) == sizeof(void*), "Assumption on size of pointer failed!\n");
			auto fiberInfoPtr = reinterpret_cast<std::intptr_t>(fiber);
			auto lambdaPtr = reinterpret_cast<std::intptr_t>(&lambda);

			// set starting point function
			makecontext(&target,(void(*)())&exec<Fun>,4,
					int(fiberInfoPtr >> 32), int(fiberInfoPtr),
					int(lambdaPtr >> 32), int(lambdaPtr)
			);

			// backup current fiber state
			auto localFiber = getCurrentFiber();

			if (DEBUG) std::cout << "Starting fiber " << fiber << " from " << localFiber << " @ " << &getCurrentFiberInfo() << "\n";

			// switch to fiber context
			int success = swapcontext(&local,&target);
			if (success != 0) assert_fail() << "Unable to switch thread context to fiber!";

			// restore local fiber information
			if (DEBUG) std::cout << "Resuming fiber " << localFiber << " after starting " << fiber << " @ " << &getCurrentFiberInfo() << "\n";
			setCurrentFiberInfo(localFiber);

			// unlock other sides lock
			clearMutexLock();

			// determine whether fiber is still running
			if (fiber->running) {
				return fiber;
			}

			// fiber is done, nothing to return
			return {};
		}

		/**
		 * Suspends the currently processed fiber. This function
		 * must only be called within the context of a fiber managed
		 * by this pool.
		 */
		static void suspend(std::mutex* lock = nullptr) {

			// get current fiber
			auto fiber = getCurrentFiberInfo();

			// make sure this is within a fiber
			assert_true(fiber);

			// make sure this fiber is indeed running
			assert_true(fiber->running);

			if (DEBUG) std::cout << "Suspending fiber " << fiber << "/" << getCurrentFiber() << " @ " << &getCurrentFiberInfo() << "\n";

			// remove state information
			resetCurrentFiberInfo();

			// capture local state
			ucontext_t local;
			getcontext(&local);

			// get previous continuation
			ucontext_t& continuation = *fiber->continuation;

			// make local state the new continuation for this fiber
			fiber->continuation = &local;

			setMutexLock(lock);

			// switch to old continuation
			int success = swapcontext(&local,&continuation);
			if (success != 0) assert_fail() << "Unable to switch thread context back to continuation!";

			// after resuming:

			// unlock other sides lock
			clearMutexLock();

			// restore locked mutex
			if (lock) lock->lock();

			// restore state
			setCurrentFiberInfo(fiber);

			if (DEBUG) std::cout << "Resuming fiber " << fiber << "/" << getCurrentFiber() << " @ " << &getCurrentFiberInfo() << "\n";

		}

		/**
		 * Resumes the processing of the given fiber previously suspended.
		 * This must not be called within a fiber.
		 *
		 * @param f the fiber to be continued (must be active)
		 * @return true if the fiber has been again suspended, false otherwise
		 */
		static bool resume(Fiber f) {

			// make sure fiber is active
			assert_true(f->running);

			// get local context
			ucontext_t local;
			getcontext(&local);

			// get fiber continuation
			ucontext_t& continuation = *f->continuation;

			// set local as the next continuation
			f->continuation = &local;

			// backup current fiber
			auto currentFiber = getCurrentFiberInfo();
			assert_ne(currentFiber,f);


			if (DEBUG) std::cout << "Resuming fiber " << f << " in " << currentFiber << " @ " << &getCurrentFiberInfo() << "\n";

			// switch to context
			int success = swapcontext(&local,&continuation);
			if (success != 0) assert_fail() << "Unable to switch thread context back to fiber!";

			// restore current fiber information
			setCurrentFiberInfo(currentFiber);

			// unlock other sides lock
			clearMutexLock();

			if (DEBUG) std::cout << "Returning to fiber " << currentFiber << " from " << f << " / " << getCurrentFiber() << " @ " << &getCurrentFiberInfo() << "\n";

			// signal whether fiber has completed its task
			return f->running;
		}

		/**
		 * Tests whether the current execution context is a fiber.
		 *
		 * @return true if the current context is a fiber context, false otherwise
		 */
		static bool isFiberContext() {
			return getCurrentFiberInfo();
		}

		/**
		 * Obtains the currently active fiber, null if not within a fiber context.
		 */
		static Fiber getCurrentFiber() {
			return getCurrentFiberInfo();
		}

	private:

		static fiber_info*& getCurrentFiberInfo() {
			static thread_local fiber_info* info = nullptr;
			return info;
		}

		static void setCurrentFiberInfo(fiber_info* info) {
			auto& context = getCurrentFiberInfo();
			context = info;
		}

		static void resetCurrentFiberInfo() {
			auto& context = getCurrentFiberInfo();
			context = nullptr;
		}


		template<typename Fun>
		static void exec(int hiFiberInfoPtr, int loFiberInfoPtr, int hiLambdaPtr, int loLambdaPtr) {

			// re-construct pool pointer
			std::intptr_t fiberInfoPtr = (std::intptr_t(hiFiberInfoPtr) << 32) | std::intptr_t(unsigned(loFiberInfoPtr));
			std::intptr_t lambdaPtr = (std::intptr_t(hiLambdaPtr) << 32) | std::intptr_t(unsigned(loLambdaPtr));

			fiber_info& info = *reinterpret_cast<fiber_info*>(fiberInfoPtr);
			Fun& lambda = *reinterpret_cast<Fun*>(lambdaPtr);

			// set up thread-local state
			setCurrentFiberInfo(&info);

			// mark fiber as running
			assert_false(info.running);
			info.running = true;

			if (DEBUG) std::cout << "Starting fiber " << &info << "/" << getCurrentFiber() << " @ " << &getCurrentFiberInfo() << "\n";

			{

				// move lambda into this context
				Fun fun(std::move(lambda));

				// process the passed function
				fun();

			} // destruct function context

			if (DEBUG) std::cout << "Completing fiber " << &info << "/" << getCurrentFiber() << " @ " << &getCurrentFiberInfo() << "\n";

			// mark fiber as done
			info.running = false;

			// make sure fiber info has been maintained
			assert_eq(&info,getCurrentFiberInfo());

			// reset thread-local state
			resetCurrentFiberInfo();

			// swap back to continuation
			ucontext_t local;
			ucontext_t& continuation = *info.continuation;
			info.continuation = nullptr;

			// free info block
			{
				guard g(info.pool.lock);
				if (DEBUG) std::cout << "Recycling fiber " << &info << " by thread " << &getCurrentFiberInfo()<< "\n";
				assert_false(info.running);
				info.pool.freeInfos.push_back(&info);
			}

			// swap back to continuation
			int success = swapcontext(&local,&continuation);
			if (success != 0) assert_fail() << "Unable to switch thread context back to continuation!";

			// Note: this code will never be reached!
			assert_fail() << "Should never be reached!";
		}

		// for debugging
		bool dump_state() {
			std::cout << "Fiber Pool:\n";
			int count = 0;
			for(std::size_t i=0; i<infos.size(); i++) {
				if (!infos[i]->running) continue;
				std::cout << "\tfiber " << i << " running\n";
				count++;
			}
			if (count == 0) {
				std::cout << "\t - no active fibers -\n";
			}
			return true;
		}

	};


	// -- fiber utilities --

	using Fiber = FiberPool::Fiber;

	/**
	 * Tests whether the current context is a fiber context.
	 *
	 * @return true if the current context is a fiber context, false otherwise.
	 */
	inline bool isFiberContext() {
		return FiberPool::isFiberContext();
	}

	/**
	 * If the current execution is in the context of a fiber, it will be suspended. If
	 * not, this operation has no effect.
	 */
	inline void suspend() {
		if (isFiberContext()) FiberPool::suspend();
	}

	inline void suspend(std::mutex& lock) {
		if (isFiberContext()) FiberPool::suspend(&lock);
	}


	/**
	 * A mutex lock avoiding blocking of fibers. Instead of blocking fibers,
	 * fibers get suspended until the lock can be acquired.
	 */
	class FiberMutex {

		constexpr static bool DEBUG = false;

		// the internally maintained mutex (using a flag, that can not spontaneously fail)
		std::atomic_flag mux;

		// a list of blocked fibers
		std::vector<Fiber> blocked;

		// the lock to protect accessed to the blocked list of fibers
		std::mutex state_lock;

		using guard = std::lock_guard<std::mutex>;

		int pid;

	public:

		FiberMutex() : pid(::getpid()) {
			mux.clear();
		}

		void lock() {

			// get current fiber
			auto fiber = FiberPool::getCurrentFiber();

			if (DEBUG) {
				guard g(state_lock);
				std::cout << std::dec << pid << ": Locking   " << this << " by fiber " << fiber << "..\n";
			}

			// if it is not a fiber, no special handling needed
			if (!fiber) {
				// loop until set
				if (!mux.test_and_set()) {
					return;
				}
				if (DEBUG) std::cout << std::dec << pid << ": Waiting for " << this << "\n";
				while(mux.test_and_set()) {
					// spin ..
				};
				if (DEBUG) std::cout << std::dec << pid << ": Releasing " << this << "\n";
			} else {
				state_lock.lock();
				while(mux.test_and_set()) {
					blocked.push_back(fiber);
					if (DEBUG) std::cout << std::dec << pid << ": Suspending " << fiber << " for " << this << "\n";
					suspend(state_lock);
					if (DEBUG) std::cout << std::dec << pid << ": Resuming " << fiber << " for " << this << "\n";
				}
				state_lock.unlock();
			}

			if (DEBUG) {
				guard g(state_lock);
				std::cout << std::dec << pid << ": Locked    " << this << " by fiber " << fiber << "..\n";
			}
		}

		void unlock() {
			if (DEBUG) {
				guard g(state_lock);
				std::cout << std::dec << pid << ": Unlocking " << this << " by fiber " << FiberPool::getCurrentFiber() << "..\n";
			}

			state_lock.lock();

			assert_true(mux.test_and_set());

			// free the lock
			mux.clear();
			// resume all suspended fibers
			std::vector<Fiber> list;
			list.swap(blocked);
			state_lock.unlock();
			for(const auto& cur : list) {
				if (DEBUG) {
					guard g(state_lock);
					std::cout << "\t" << std::dec << pid << ": Resuming " << cur << " for " << this << " - full list: " << list << "\n";
				}
				FiberPool::resume(cur);
			}

			if (DEBUG) {
				guard g(state_lock);
				std::cout << std::dec << pid << ": Unlocked  " << this << "..\n";
			}

		}

		bool try_lock() {
			// nothing special here
			if (DEBUG) {
				bool res = !mux.test_and_set();
				{
					guard g(state_lock);
					std::cout << std::dec << pid << ": Try Locking   " << this << " - " << res << " ..\n";
				}
				return res;
			}

			return !mux.test_and_set();
		}

	};


} // end namespace utils
} // end namespace allscale
