#pragma once

#include <atomic>
#include <cstdint>
#include <stdlib.h>
#include <ucontext.h>
#include <mutex>
#include <vector>
#include <thread>

#include <sys/types.h>
#include <unistd.h>

#include <sys/mman.h>

#include "allscale/utils/assert.h"
#include "allscale/utils/optional.h"
#include "allscale/utils/printer/vectors.h"
#include "allscale/utils/spinlock.h"

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
	 *
	 *
	 * Known issues:
	 * 	This implementation uses thread-local variables in combination with user-level context switching.
	 * 	This can lead to problems, since the address of thread-local variable is considered to be constant
	 * 	throughout functions. However, when switching contexts, parts of the same function may be processed
	 * 	by different threads, leading to problems since now thread-local variable addresses are no longer
	 * 	constant.
	 *
	 * Related topics:
	 *  - https://stackoverflow.com/questions/25673787/making-thread-local-variables-fully-volatile
	 *  - https://akkadia.org/drepper/tls.pdf
	 */
	class FiberPool {

		constexpr static bool DEBUG = false;

		// we use 8 MB per fiber stack
		constexpr static std::size_t DEFAULT_STACK_SIZE = (1<<23);

		// an extended context to allow parameter passing along context switches
		struct ext_ucontext_t {
			// the context required for context switching
			ucontext_t context;
			// extra parameter to be passed along context switches
			spinlock* volatile lock = nullptr;		// < a potential lock to be freed after a context switch (the pointer is volatile, not the object)
		};

		// the management information required per fiber
		struct fiber_info {
			FiberPool& pool;			   // < the pool it belongs to
			std::atomic<bool> running;	   // < the running state
			std::atomic<bool> active;      // < debugging, make sure that infos are only active once
			void* stack;				   // < the associated stack memory
			const int stack_size;		   // < the size of the stack
			ext_ucontext_t* continuation;  // < an optional continuation to be processed after finishing or suspending a fiber

			fiber_info(FiberPool& pool, int stackSize = DEFAULT_STACK_SIZE)
				: pool(pool),
				  running(false),
				  active(false),
				  stack(nullptr),
				  stack_size(stackSize),
				  continuation(nullptr) {

				// allocate stack memory
				stack = mmap(nullptr,stackSize,PROT_READ | PROT_WRITE,MAP_PRIVATE | MAP_STACK | MAP_ANONYMOUS,0,0);

				// provide proper error reporting
				if (stack == MAP_FAILED) {
					std::cerr << "Can not allocate stack of size " << stackSize << " byte: ";
					switch(errno) {
					case ENOMEM: std::cerr << "not enough memory"; break;
					default: std::cerr << "errno code " << errno << " (see errno.h on your system)"; break;
					}
					assert_fail();
					// terminate program (unrecoverable)
					exit(1);
				}

				assert_ne(MAP_FAILED,stack) << "Error code: " << errno << "\n";
				assert_true(stack);
			}

			fiber_info(const fiber_info&) = delete;
			fiber_info(fiber_info&& other) = delete;
			~fiber_info() {
				munmap(stack,stack_size);
			}

			void activate() {
				assert_decl({
					bool cur = false;
					auto success = active.compare_exchange_strong(cur,true);
					assert_true(success) << "Possible double-usage of fiber detected!";
				});
			}

			void deactivate() {
				assert_decl({
					bool cur = true;
					auto success = active.compare_exchange_strong(cur,false);
					assert_true(success) << "Possible double-usage of fiber detected!";
				});
			}

		};

		// the list of all fiber infos associated to this pool
		std::vector<fiber_info*> infos;

		// the list of free fiber info entries (reuse list)
		std::vector<fiber_info*> freeInfos;

		// a lock to synchronize accesses to the free lists
		mutable spinlock freeInfosLock;

		// the guard type used for protection operations
		using guard = std::lock_guard<spinlock>;

	public:

		using Fiber = fiber_info*;

		FiberPool(std::size_t initialSize = 10) {
			// create an initial pool of fiber infos
			infos.reserve(initialSize);
			freeInfos.reserve(initialSize);
			for(std::size_t i=0; i<initialSize; i++) {
				infos.push_back(new fiber_info(*this));
				assert_false(infos.back()->running);
				assert_false(infos.back()->active);
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
				guard g(freeInfosLock);

				// re-use an existing fiber or create a new one
				if (!freeInfos.empty()) {
					fiber = freeInfos.back();
					freeInfos.pop_back();
					assert_false(fiber->running) << "Faulty fiber: " << fiber;
					assert_false(fiber->active) << "Faulty fiber: " << fiber;
				} else {
					fiber = new fiber_info(*this);
					infos.push_back(fiber);
					assert_false(fiber->running) << "Faulty fiber: " << fiber;
					assert_false(fiber->active) << "Faulty fiber: " << fiber;
				}
			}

			assert_true(fiber);

			// make sure the fiber is not running
			assert_false(fiber->running) << "Faulty fiber: " << fiber;
			assert_false(fiber->active) << "Fiber: " << fiber;

			// capture current context
			ext_ucontext_t local;
			getcontext(&local.context);

			// register local context as continuation
			fiber->continuation = &local;

			// create a context for the new fiber
			ext_ucontext_t target = local;
			target.context.uc_link = &local.context;

			// exchange stack
			target.context.uc_stack.ss_sp = fiber->stack;
			target.context.uc_stack.ss_size = fiber->stack_size;

			// encode pool pointer and pointer to lambda into integers
			static_assert(2*sizeof(int) == sizeof(void*), "Assumption on size of pointer failed!\n");
			auto fiberInfoPtr = reinterpret_cast<std::intptr_t>(fiber);
			auto lambdaPtr = reinterpret_cast<std::intptr_t>(&lambda);

			// set starting point function
			makecontext(&target.context,(void(*)())&exec<Fun>,4,
					int(fiberInfoPtr >> 32), int(fiberInfoPtr),
					int(lambdaPtr >> 32), int(lambdaPtr)
			);

			// backup current fiber state
			auto localFiber = getCurrentFiber();

			if (DEBUG) std::cout << "Starting fiber " << fiber << " from " << localFiber << " @ " << &getCurrentFiberInfo() << "\n";

			// switch to fiber context
			swap(local,target);

			// restore local fiber information
			if (DEBUG) std::cout << "Resuming fiber " << localFiber << " after starting " << fiber << " @ " << &getCurrentFiberInfo() << "\n";

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
		static void suspend(spinlock* lock = nullptr) {

			// get current fiber
			auto fiber = getCurrentFiberInfo();

			// make sure this is within a fiber
			assert_true(fiber);

			// make sure this fiber is indeed running
			assert_true(fiber->running);

			// capture local state
			ext_ucontext_t local;
			getcontext(&local.context);

			// get previous continuation
			ext_ucontext_t& continuation = *fiber->continuation;

			// make local state the new continuation for this fiber
			fiber->continuation = &local;

			if (DEBUG) std::cout << "Suspending fiber " << fiber << "/" << getCurrentFiber() << " @ " << &getCurrentFiberInfo() << "\n";

			// switch to continuation context
			swap(local,continuation,lock);

			// after resuming:

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
			ext_ucontext_t local;
			getcontext(&local.context);

			// get fiber continuation
			ext_ucontext_t& continuation = *f->continuation;

			// set local as the next continuation
			f->continuation = &local;

			if (DEBUG) std::cout << "Resuming fiber " << f << " in " << getCurrentFiberInfo() << " @ " << &getCurrentFiberInfo() << "\n";

			// switch to context
			swap(local,continuation);

			if (DEBUG) std::cout << "Returning to fiber " << getCurrentFiberInfo() << " from " << f << " / " << getCurrentFiber() << " @ " << &getCurrentFiberInfo() << "\n";

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

		static void swap(ext_ucontext_t& src, ext_ucontext_t& trg, spinlock* lock = nullptr) {

			// get current context
			auto currentFiber = getCurrentFiberInfo();

			// record mutex lock
			assert_true(trg.lock == nullptr);
			trg.lock = lock;

			// clear fiber context info
			resetCurrentFiberInfo();

			// deactivate current fiber
			if (currentFiber) currentFiber->deactivate();

			// switch context
			int success = swapcontext(&src.context,&trg.context);
			if (success != 0) assert_fail() << "Unable to switch thread context!";

			// unlock src-lock (which after the swap is in the source context)
			if (src.lock) {
				src.lock->unlock();
				src.lock = nullptr;
			}

			// re-activate current fiber
			if (currentFiber) currentFiber->activate();

			// reset current fiber information
			setCurrentFiberInfo(currentFiber);

			// restore lock
			if (lock) lock->lock();

		}

		__attribute__ ((noinline)) static fiber_info*& getCurrentFiberInfo() {
			static thread_local fiber_info* info = nullptr;
			asm(""); // for the compiler, this changes everything :)
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

			// mark info as being actively processed
			info.activate();

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

			// swap back to continuation
			ext_ucontext_t local;
			ext_ucontext_t& continuation = *info.continuation;
			info.continuation = nullptr;

			// free info block
			info.pool.freeInfosLock.lock();
			if (DEBUG) std::cout << "Recycling fiber " << &info << " by thread " << &getCurrentFiberInfo()<< "\n";
			assert_false(info.running);
			info.pool.freeInfos.push_back(&info);

			// swap back to continuation, and release lock for free list to make this info re-usable
			swap(local,continuation,&info.pool.freeInfosLock);

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

	inline void suspend(spinlock& lock) {
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
		spinlock state_lock;

		using guard = std::lock_guard<spinlock>;

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


	// -------- Fiber Futures ------------


	template<typename T>
	class FiberPromise;

	template<typename T>
	class FiberFuture;

	template<typename T>
	class FiberPromiseFutureBase {

		friend class FiberPromise<T>;
		friend class FiberFuture<T>;

		/**
		 * A global lock per future type for synchronizing promise/future
		 * connections.
		 */
		static spinlock& getLock() {
			static spinlock lock;
			return lock;
		}

	};


	template<typename T>
	class FiberFuture {

		friend class FiberPromise<T>;

		using promise_t = FiberPromise<T>;

		using guard = std::lock_guard<spinlock>;

		promise_t* promise;

		allscale::utils::optional<T> value;

		mutable spinlock wait_lock;

		mutable Fiber fiber = nullptr;		// a potential fiber being blocked

		using yield_function_t = std::function<void()>;

		yield_function_t yield;

		FiberFuture(promise_t& promise, const yield_function_t& yield) : promise(&promise), yield(yield) {
			promise.set_future(*this);
		};

	public:

		FiberFuture() : promise(nullptr), yield(&std::this_thread::yield) {}

		FiberFuture(const FiberFuture& other) = delete;

		FiberFuture(FiberFuture&& other) : promise(nullptr), yield(other.yield) {

			guard g(FiberPromiseFutureBase<T>::getLock());

			// if there is still a promise
			if (other.promise) {

				// move promise
				std::swap(promise,other.promise);

				// inform promise about new future target
				promise->set_future(*this);
			}

			// move value
			value = std::move(other.value);

		}

		~FiberFuture() {
			guard g(FiberPromiseFutureBase<T>::getLock());
			if (promise) promise->reset_future();
		}

		// support initialization with the result value
		FiberFuture(const T& value) : promise(nullptr), value(value), yield(nullptr) {}
		FiberFuture(T&& value) : promise(nullptr), value(std::move(value)), yield(nullptr) {}

		FiberFuture& operator=(const FiberFuture&) = delete;

		FiberFuture& operator=(FiberFuture&& other) {
			//
			if (this == &other) return *this;

			{
				guard g(FiberPromiseFutureBase<T>::getLock());

				// clear old state
				if (promise) promise->reset_future();

				// if there is still a promise
				if (other.promise) {

					// move promise
					std::swap(promise,other.promise);

					// inform promise about new future target
					promise->set_future(*this);
				}

				// move value
				value = std::move(other.value);

			}

			// move yield
			yield = std::move(other.yield);

			return *this;
		}

		bool valid() const {
			guard g(FiberPromiseFutureBase<T>::getLock());
			return promise || bool(value);
		}

		void wait() const {
			assert_true(valid());

			wait_lock.lock();

			// get current fiber
			assert_true(fiber == nullptr);
			fiber = FiberPool::getCurrentFiber();

			// check promise state
			while(promise) {

				if (fiber) {
					// in a fiber, suspend operation
					suspend(wait_lock);
					assert_eq(fiber,FiberPool::getCurrentFiber());
				} else {
					wait_lock.unlock();
					yield();	// < user defined yield
					wait_lock.lock();
				}
			}

			// reset fiber
			fiber = nullptr;

			wait_lock.unlock();
		}

		T get() {
			wait();
			return std::move(*value);
		}

	private:

		/**
		 * Delivers the result to this future, returns a potential
		 * fiber waiting for this result to be resumed.
		 */
		Fiber deliver(const T& result) {
			value = result;
			Fiber waiting = nullptr;
			{
				guard g(wait_lock);
				promise = nullptr;
				waiting = fiber;
			}
			return waiting;
		}

		/**
		 * Delivers the result to this future, returns a potential
		 * fiber waiting for this result to be resumed.
		 */
		Fiber deliver(T&& result) {
			value = std::move(result);
			Fiber waiting = nullptr;
			{
				guard g(wait_lock);
				promise = nullptr;
				waiting = fiber;
			}
			return waiting;
		}

	};


	template<typename T>
	class FiberPromise {

		using future_t = FiberFuture<T>;

		friend class FiberFuture<T>;

		// the future the result should be delivered to (at most one)
		future_t* future;

		using guard = std::lock_guard<spinlock>;

	public:

		FiberPromise() : future(nullptr) {}

		FiberPromise(const FiberPromise&) = delete;

		FiberPromise(FiberPromise&&) = delete;

		~FiberPromise() {
			assert_false(future) << "Promise destroyed without delivery!";
		}

		/**
		 * Obtains a future associated to this promise. This function
		 * must only be called once.
		 */
		future_t get_future(const std::function<void()>& yield = &std::this_thread::yield) {
			assert_true(nullptr == future);
			future_t res(*this,yield);
			return res;
		}

		/**
		 * Delivers the given value to the future associated to this promise.
		 * This function must only be called once.
		 */
		void set_value(const T& value) {
			Fiber waiting = nullptr;
			{
				guard g(FiberPromiseFutureBase<T>::getLock());
				if (!future) return;
				waiting = future->deliver(value);
				future = nullptr;
			}
			if (waiting) FiberPool::resume(waiting);
		}

		/**
		 * Delivers the given value to the future associated to this promise.
		 * This function must only be called once.
		 */
		void set_value(T&& value) {
			Fiber waiting = nullptr;
			{
				guard g(FiberPromiseFutureBase<T>::getLock());
				if (!future) return;
				waiting = future->deliver(std::move(value));
				future = nullptr;
			}
			if (waiting) FiberPool::resume(waiting);
		}

	private:

		void set_future(FiberFuture<T>& f) {
			future = &f;
		}

		void reset_future() {
			future = nullptr;
		}

	};


} // end namespace utils
} // end namespace allscale
