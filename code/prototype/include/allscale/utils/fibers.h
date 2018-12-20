#pragma once

#include <atomic>
#include <mutex>
#include <memory>
#include <ucontext.h>
#include <unordered_map>
#include <vector>

#include <sys/mman.h>

#include "allscale/utils/assert.h"
#include "allscale/utils/spinlock.h"

namespace allscale {
namespace utils {

	// TODO: split up into multiple files

	class FiberContext;

	namespace fiber {

		// --- fiber ---

		struct EventHandler {
			void(*fun)(void*) =nullptr;	// < the function to be triggered
			void* arg;					// < the argument to be passed
			void trigger() {
				if (fun) (fun)(arg);
			}
		};

		using priority_t = int;


		// an extended context to allow parameter passing along context switches
		struct ext_ucontext_t {
			// the context required for context switching
			ucontext_t context;
			// extra parameter to be passed along context switches
			spinlock* volatile lock = nullptr;		// < a potential lock to be freed after a context switch (the pointer is volatile, not the object)
		};



		// we use 8 MB per fiber stack
		constexpr static std::size_t DEFAULT_STACK_SIZE = (1<<23);

		struct Stack {
			void* stack;				   // < the associated stack memory
			const int stackSize;		   // < the size of the stack

			Stack(int size) : stackSize(size) {

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

			Stack(const Stack&) = delete;
			Stack(Stack&&) = delete;

			~Stack() {
				munmap(stack,stackSize);
			}

		};

		struct Fiber {

			// the context this fiber is part of
			FiberContext& ctxt;

			// the priority of this fiber
			priority_t priority = 0;

			// the owned stack
			Stack stack;

			// -- registered event handler --
			EventHandler suspendHandler;
			EventHandler resumeHandler;
			EventHandler terminateHandler;

			// the context information when targeting this fiber
			ext_ucontext_t ucontext = {};

			// the context to resume this fiber
			ext_ucontext_t* continuation;


			Fiber(FiberContext& ctxt, int stack_size = DEFAULT_STACK_SIZE)
				: ctxt(ctxt), stack(stack_size), continuation(nullptr) {

				// initialize context with current setup
				getcontext(&ucontext.context);

				// link with managed stack
				ucontext.context.uc_stack.ss_sp = stack.stack;
				ucontext.context.uc_stack.ss_size = stack.stackSize;
			};

			void suspend(spinlock& lock);

			void resume();

		};


		__attribute__ ((noinline)) static Fiber*& getCurrentFiberInfo() {
			static thread_local Fiber* fiber = nullptr;
			asm(""); // for the compiler, this changes everything :)
			return fiber;
		}

		inline void setCurrentFiberInfo(Fiber* info) {
			auto& context = getCurrentFiberInfo();
			context = info;
		}

		inline void resetCurrentFiberInfo() {
			auto& context = getCurrentFiberInfo();
			context = nullptr;
		}


		inline Fiber* getCurrentFiber() {
			return getCurrentFiberInfo();
		}



		// --- fiber pool ---

		class Pool {

			friend class allscale::utils::FiberContext;

			FiberContext& ctxt;

			spinlock freeListLock;

			// the full list of owned fibers
			std::vector<std::unique_ptr<Fiber>> fibers;

			// list of available fibers
			std::vector<Fiber*> free;

			using guard = std::lock_guard<spinlock>;

		public:

			Pool(FiberContext& ctxt, int initial_num_fibers = 20) : ctxt(ctxt) {
				fibers.reserve(initial_num_fibers*2);
				free.reserve(initial_num_fibers*2);
				for(int i=0; i<initial_num_fibers; i++) {
					createNewFiber();
				}
			}


			Fiber* getFreeFiber() {
				guard g(freeListLock);
				if (free.empty()) {
					createNewFiber();
				}
				auto res = free.back();
				free.pop_back();
				return res;
			}

		private:

			void createNewFiber() {
				fibers.push_back(std::make_unique<Fiber>(ctxt));
				free.push_back(fibers.back().get());
			}

		};


		// --- event handling ---

		// the type to reference events
		using EventId = int;

		class EventRegister {

			std::atomic<EventId> counter { 0 };

			spinlock lock;

			std::unordered_map<EventId, std::vector<Fiber*>> events;

			using guard = std::lock_guard<spinlock>;

		public:

			EventId create() {
				auto res = ++counter;
				guard g(lock);
				events[res];
				return res;
			}

			void trigger(EventId event);

			void waitFor(Fiber& fiber, EventId event) {

				guard g(lock);
				auto pos = events.find(event);
				if (pos == events.end()) return;

				// register fiber for suspension
				pos->second.push_back(&fiber);
				fiber.suspend(lock);
			}

		};


		class ConditionalVariable {

			using guard = std::lock_guard<spinlock>;

			spinlock waitingListLock;

			std::vector<Fiber*> waiting;

		public:

			void wait(spinlock& lock) {
				auto fiber = getCurrentFiber();
				assert_true(fiber) << "Unable to suspend non-fiber context!";
				{
					guard g(waitingListLock);
					waiting.push_back(fiber);
				}
				fiber->suspend(lock);
			}

			void notifyOne() {
				Fiber* f = nullptr;
				{
					guard g(waitingListLock);
					if (waiting.empty()) return;
					f = waiting.back();
					waiting.pop_back();
				}
				f->resume();
			}

			void notifyAll() {
				std::vector<Fiber*> fibers;
				{
					guard g(waitingListLock);
					fibers.swap(waiting);
				}
				for(auto& fiber : fibers) {
					fiber->resume();
				}
			}

		};


		class Mutex {

			spinlock syncLock;

			// the internally maintained mutex (using a flag, that can not spontaneously fail)
			std::atomic_flag mux;

			ConditionalVariable var;

			using guard = std::lock_guard<spinlock>;

		public:

			Mutex() {
				mux.clear();
			}

			void lock() {
				guard g(syncLock);
				while(mux.test_and_set(std::memory_order_acquire)) {
					var.wait(syncLock);
				}
			}

			void unlock() {
				mux.clear(std::memory_order_release);
				var.notifyOne();
			}

			bool try_lock() {
				return !mux.test_and_set(std::memory_order_acquire);
			}

		};





		void suspend(EventId event);

	} // end namespace fiber



	/**
	 * A fiber context, organizing ...
	 */
	class FiberContext {

		friend class fiber::Fiber;

		fiber::Pool pool;

		std::vector<fiber::Fiber*> runable;

		spinlock runableLock;

		using guard = std::lock_guard<spinlock>;

	public:

		fiber::EventRegister eventRegister;

		FiberContext() : pool(*this) {}

		FiberContext(const FiberContext&) = delete;
		FiberContext(FiberContext&&) = delete;


		template<typename Fun>
		void start(Fun&& lambda) {

			using namespace fiber;

			// get a fresh fiber
			auto fiber = pool.getFreeFiber();
			assert_true(fiber);

			// capture current context
			ext_ucontext_t local;
			getcontext(&local.context);

			// register local context as continuation
			fiber->continuation = &local;

			// create target context
			auto& target = fiber->ucontext;

			// encode pool pointer and pointer to lambda into integers
			static_assert(2*sizeof(int) == sizeof(void*), "Assumption on size of pointer failed!\n");
			auto fiberInfoPtr = reinterpret_cast<std::intptr_t>(fiber);
			auto lambdaPtr = reinterpret_cast<std::intptr_t>(&lambda);

			// set starting point function
			makecontext(&target.context,(void(*)())&exec<Fun>,4,
					int(fiberInfoPtr >> 32), int(fiberInfoPtr),
					int(lambdaPtr >> 32), int(lambdaPtr)
			);

			// switch to fiber context
			swap(local,target);

		}

		bool yield() {
			fiber::Fiber* fiber;
			{
				guard g(runableLock);
				if(runable.empty()) return false;
				fiber = runable.back();
				runable.pop_back();
			}


			// capture current context
			fiber::ext_ucontext_t local;
			getcontext(&local.context);

			fiber->continuation = &local;

			swap(local,fiber->ucontext);

			return true;
		}


	private:

		void resume(fiber::Fiber& fiber) {
			guard g(runableLock);
			runable.push_back(&fiber);
		}

		static void swap(fiber::ext_ucontext_t& src, fiber::ext_ucontext_t& trg, spinlock* lock = nullptr) {

			// get current context
			auto currentFiber = fiber::getCurrentFiber();

			// run suspension handler
			if (currentFiber) currentFiber->suspendHandler.trigger();

			// record mutex lock
			assert_true(trg.lock == nullptr);
			trg.lock = lock;

			// clear fiber context info
			fiber::resetCurrentFiberInfo();

//			// deactivate current fiber
//			if (currentFiber) currentFiber->deactivate();

			// switch context
			int success = swapcontext(&src.context,&trg.context);
			if (success != 0) assert_fail() << "Unable to switch thread context!";

			// unlock src-lock (which after the swap is in the source context)
			if (src.lock) {
				src.lock->unlock();
				src.lock = nullptr;
			}

//			// re-activate current fiber
//			if (currentFiber) currentFiber->activate();

			// reset current fiber information
			setCurrentFiberInfo(currentFiber);

			// run suspension handler
			if (currentFiber) currentFiber->resumeHandler.trigger();

			// restore lock
			if (lock) lock->lock();

		}


		template<typename Fun>
		static void exec(int hiFiberInfoPtr, int loFiberInfoPtr, int hiLambdaPtr, int loLambdaPtr) {

			// re-construct pool pointer
			std::intptr_t fiberInfoPtr = (std::intptr_t(hiFiberInfoPtr) << 32) | std::intptr_t(unsigned(loFiberInfoPtr));
			std::intptr_t lambdaPtr = (std::intptr_t(hiLambdaPtr) << 32) | std::intptr_t(unsigned(loLambdaPtr));

			fiber::Fiber& info = *reinterpret_cast<fiber::Fiber*>(fiberInfoPtr);
			Fun& lambda = *reinterpret_cast<Fun*>(lambdaPtr);

			// set up thread-local state
			setCurrentFiberInfo(&info);

//			// mark fiber as running
//			assert_false(info.running);
//			info.running = true;
//
//			// mark info as being actively processed
//			info.activate();

//			if (DEBUG) std::cout << "Starting fiber " << &info << "/" << getCurrentFiber() << " @ " << &getCurrentFiberInfo() << "\n";

			{

				// move lambda into this context
				Fun fun(std::move(lambda));

				// process the passed function
				fun();

			} // destruct function context

//			if (DEBUG) std::cout << "Completing fiber " << &info << "/" << getCurrentFiber() << " @ " << &getCurrentFiberInfo() << "\n";
//
//			// mark fiber as done
//			info.reset();
//
//			// make sure fiber info has been maintained
//			assert_eq(&info,getCurrentFiberInfo());

			info.terminateHandler.trigger();

			// swap back to continuation
			fiber::ext_ucontext_t local;
			fiber::ext_ucontext_t& continuation = *info.continuation;
			info.continuation = nullptr;

			// free info block
			auto& pool = info.ctxt.pool;
			pool.freeListLock.lock();
//			if (DEBUG) std::cout << "Recycling fiber " << &info << " by thread " << &getCurrentFiberInfo()<< "\n";
//			assert_false(info.running);
			pool.free.push_back(&info);

			// swap back to continuation, and release lock for free list to make this info re-usable
			swap(local,continuation,&pool.freeListLock);

			// Note: this code will never be reached!
			assert_fail() << "Should never be reached!";
		}


	};

	namespace fiber {

		inline void Fiber::suspend(spinlock& lock) {
			assert_true(continuation);
			FiberContext::swap(ucontext,*continuation,&lock);
		}

		inline void Fiber::resume() {
			ctxt.resume(*this);
		}

		void suspend(EventId event) {
			auto fiber = getCurrentFiber();
			assert_true(fiber) << "Error: can not suspend non-fiber context!";
			fiber->ctxt.eventRegister.waitFor(*fiber,event);
		}

		void EventRegister::trigger(EventId event) {
			guard g(lock);
			auto pos = events.find(event);
			assert_true(pos != events.end()) << "Invalid event: " << event;

			for(auto& fiber : pos->second) {
				fiber->resume();
			}
		}

	} // end namespace fiber


} // end namespace utils
} // end namespace allscale
