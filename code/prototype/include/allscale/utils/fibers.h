#pragma once

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <ucontext.h>
#include <unordered_map>
#include <vector>
#include <set>

#include <sys/mman.h>

#include "allscale/utils/assert.h"
#include "allscale/utils/finalize.h"
#include "allscale/utils/spinlock.h"
#include "allscale/utils/optional.h"

namespace allscale {
namespace utils {

	// TODO: split up into multiple files

	class FiberContext;

	namespace fiber {

		// --- fiber ---

		struct EventHandler {
			void(*fun)(void*) = nullptr;	// < the function to be triggered
			void* arg = nullptr;			// < the argument to be passed
			void trigger() {
				if (fun) (fun)(arg);
			}
			void reset() {
				fun = nullptr;
			}
		};


		struct FiberEvents {
			EventHandler suspend;
			EventHandler resume;
			EventHandler terminate;
		};

		// an extended context to allow parameter passing along context switches
		struct ext_ucontext_t {
			// the context required for context switching
			ucontext_t context;
			// extra parameter to be passed along context switches
			spinlock* volatile lock = nullptr;		// < a potential lock to be freed after a context switch (the pointer is volatile, not the object)
			EventHandler postSwapHandler;			// < to be triggered after a swap
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

		/**
		 * Fiber execution priority. The higher the priority,
		 * the sooner it will be processed.
		 */
		enum class Priority : int {
			LOW = 0,
			MEDIUM = 1,
			HIGH = 2,

			// the default priority
			DEFAULT = MEDIUM
		};


		class Fiber {
		public:

			// the context this fiber is part of
			FiberContext& ctxt;

			// the priority of this fiber
			Priority priority = Priority::DEFAULT;

			// the owned stack
			Stack stack;

			// -- registered event handler --
			FiberEvents eventHandler;

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

		};


		Fiber*& getCurrentFiberInfo();

		void setCurrentFiberInfo(Fiber* info);

		void resetCurrentFiberInfo();

		Fiber* getCurrentFiber();



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

		// a special event id for events to be ignored
		constexpr EventId EVENT_IGNORE = 0;

		class EventRegister {

			std::atomic<EventId> counter { 1 };

			spinlock lock;

			// TODO: make this more light-weight
			std::unordered_multimap<EventId,Fiber*> events;

			using guard = std::lock_guard<spinlock>;

		public:

			EventId create() {
				auto res = counter++;
				guard g(lock);
				events.insert({res,nullptr});
				assert_ne(EVENT_IGNORE,res);
				return res;
			}

			template<typename Iter>
			void create(int num, Iter target) {
				auto first = counter.fetch_add(num);
				guard g(lock);
				for(int i=0; i<num; i++) {
					*target = first + i;
					events.insert({*target,nullptr});
					target++;
				}
			}

			void trigger(EventId event);

			void waitFor(EventId event, Fiber* current = nullptr) {
				// if it is the ignore event, skip it
				if (event == EVENT_IGNORE) return;

				guard g(lock);
				auto pos = events.find(event);
				if (pos == events.end()) return;

				// register fiber for suspension
				auto fiber = current ? current : getCurrentFiber();
				assert_true(fiber) << "Unable to suspend non-fiber context.";
				events.insert({event, fiber});
				fiber->suspend(lock);
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

			void notifyOne();

			void notifyAll();

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
				{
					guard g(syncLock);
					mux.clear(std::memory_order_release);
				}
				var.notifyOne();
			}

			bool try_lock() {
				return !mux.test_and_set(std::memory_order_acquire);
			}

		};

		/**
		 * Suspends the current fiber and continues execution with the fiber's continuation,
		 * thus the thread context processing the this fiber.
		 *
		 * @pre must be executed within a fiber
		 */
		void suspend();

		/**
		 * Suspends the current fiber and continues execution with the fiber's continuation,
		 * thus the thread context processing the this fiber.
		 *
		 * @pre must be executed within a fiber
		 * @param priority the priority to assign to the fiber while being suspended
		 */
		void suspend(Priority priority);


		/**
		 * Suspends the currently processed fiber until the given event is triggered.
		 * If the event already has been triggered, the thread will not be suspended.
		 *
		 * @pre must be executed within a fiber
		 * @param event the event to wait for
		 */
		void suspend(EventId event);

		/**
		 * Suspends the currently processed fiber until the given event is triggered.
		 * If the event already has been triggered, the thread will not be suspended.
		 *
		 * @pre must be executed within a fiber
		 * @param event the event to wait for
		 * @param priority the priority to assign to the fiber while being suspended
		 */
		void suspend(EventId event, Priority priority);

		/**
		 * The type of queue used to organize runable tasks within a fiber context.
		 */
		class FiberQueue {

			struct fiber_priority_compare {
				bool operator()(Fiber* a, Fiber* b) {
					assert_true(a); assert_true(b);
					return a->priority < b->priority;
				}
			};

			using priority_queue_t = std::priority_queue<
					Fiber*,
					std::vector<Fiber*>,
					fiber_priority_compare
			>;

			using guard = std::lock_guard<spinlock>;


			priority_queue_t runable;

			spinlock runableLock;

			std::condition_variable_any con_var;

		public:

			/**
			 * Inserts a range of fibers into this queue.
			 */
			template<typename Iter>
			void add(const Iter& begin, const Iter& end) {

				// ignore empty lists
				if (begin == end) return;

				// add fibers to queue of runables
				guard g(runableLock);
				for(auto it = begin; it != end; ++it) {
					runable.push(*it);
				}

				// wake up potential blocked threads waiting for tasks
				if (std::distance(begin,end) < 2) {
					con_var.notify_one();
				} else {
					con_var.notify_all();
				}
			}

			Fiber* top(bool blocking, const std::chrono::milliseconds& maxBlockingTime) {

				guard g(runableLock);

				// handle empty queue
				if (runable.empty()) {

					// return if no wait is requested ..
					if (!blocking) return nullptr;

					// wait for entry to show up ..
					con_var.wait_for(runableLock, maxBlockingTime, [&]{ return !runable.empty(); });
				}

				// check if there is something to do
				if (runable.empty()) return nullptr;

				// take fiber with highest priority
				auto fiber = runable.top();
				runable.pop();
				return fiber;

			}

			void unblockAll() {
				con_var.notify_all();
			}

			void suspend(Fiber& fiber) {
				guard g(runableLock);
				runable.push(&fiber);
				con_var.notify_one();
				fiber.suspend(runableLock);
			}

		};

		template<typename Op>
		void runWithPriority(Priority p, Op&& op) {
			// get current fiber
			auto fiber = getCurrentFiber();

			// if not in fiber context, just run operation
			if (!fiber) {
				op();
				return;
			}

			// update priority
			Priority old = fiber->priority;
			fiber->priority = p;

			// set up reset operation
			auto f = allscale::utils::run_finally([&]{
				fiber->priority = old;
			});

			// run the operation
			op();

		}

	} // end namespace fiber



	/**
	 * A fiber context, organizing ...
	 */
	class FiberContext {

		friend class fiber::Fiber;
		friend class fiber::EventRegister;
		friend class fiber::Mutex;
		friend class fiber::ConditionalVariable;

		fiber::Pool pool;

		fiber::FiberQueue runable;

		fiber::EventRegister eventRegister;

	public:

		FiberContext() : pool(*this) {}

		FiberContext(const FiberContext&) = delete;
		FiberContext(FiberContext&&) = delete;

		fiber::EventRegister& getEventRegister() {
			return eventRegister;
		}

		template<typename Fun>
		void start(
				Fun&& lambda,
				const fiber::Priority& priority = fiber::Priority::DEFAULT,
				const fiber::FiberEvents& handler = {}
		) {

			using namespace fiber;

			// get a fresh fiber
			auto fiber = pool.getFreeFiber();
			assert_true(fiber);

			// fix priority
			fiber->priority = priority;

			// install event handler
			fiber->eventHandler = handler;

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

		template<typename Fun>
		std::enable_if_t<std::is_same<std::result_of_t<Fun()>,void>::value,std::result_of_t<Fun()>>
		process(Fun&& fun) {

			// make sure this is called by a thread
			assert_false(fiber::getCurrentFiber())
				<< "Must not be called from within a fiber!";

			// initialize done sync flag
			std::atomic_flag done;
			done.test_and_set();

			start([&]{
				// perform operation
				fun();
				// signal completion
				done.clear();
			});

			// wait until done
			while(done.test_and_set()) {
				yield();	// < donate thread to processing task
			}

		}

		template<typename Fun>
		std::enable_if_t<!std::is_same<std::result_of_t<Fun()>,void>::value,std::result_of_t<Fun()>>
		process(Fun&& fun) {
			using result_t = std::result_of_t<Fun()>;

			// initialize result
			optional<result_t> res;

			// perform the task, get the result
			process([&]{ res = fun(); });

			// result should be done now
			assert_true(bool(res));

			// done
			return std::move(*res);
		}

		/**
		 * Yield the current thread to allow a fiber to be processed, optionally
		 * blocking until either a fiber has been processed, or the given time-out
		 * is reached.
		 *
		 * @param blocking true, if the current thread should be suspended until a fiber is available,
		 *  		false otherwise; in non-blocking mode, if no fiber is available, control will return immediately
		 * @param maxBlockingTime the maximum duration a thread will be blocked before returning
		 * @return true, if a fiber has been processed, false otherwise
		 */
		bool yield(bool blocking = false, const std::chrono::milliseconds& maxBlockingTime = std::chrono::milliseconds(10)) {

			// get a runable fiber
			fiber::Fiber* fiber = runable.top(blocking, maxBlockingTime);

			// if non is available, we are done
			if (!fiber) return false;

			// capture current context
			fiber::ext_ucontext_t local;
			getcontext(&local.context);

			fiber->continuation = &local;

			swap(local,fiber->ucontext);

			return true;
		}

	private:

		friend void fiber::suspend();
		friend void fiber::suspend(Priority);

		void suspend(fiber::Fiber& fiber) {
			assert_eq(&fiber.ctxt,this) << "Wrong context for suspension!";
			runable.suspend(fiber);
		}

		template<typename Iter>
		void resume(const Iter& begin, const Iter& end) {
			// make sure all fibers belong to this context
			assert_true(std::all_of(begin,end,[&](const fiber::Fiber* f){ return &(f->ctxt) == this; }));

			// short-cut for empty list
			if (begin == end) return;

			// re-insert runable tasks
			runable.add(begin,end);

			// TODO: switch to higher-priority task if available
		}

		template<typename List>
		void resume(const List& list) {
			resume(list.begin(),list.end());
		}

		void resume(fiber::Fiber& fiber) {
			std::array<fiber::Fiber*,1> list = { &fiber };
			resume(list);
		}

		static void swap(fiber::ext_ucontext_t& src, fiber::ext_ucontext_t& trg, spinlock* lock = nullptr) {

			// get current context
			auto currentFiber = fiber::getCurrentFiber();

			// run suspension handler
			if (currentFiber) currentFiber->eventHandler.suspend.trigger();

			// record mutex lock
			assert_true(trg.lock == nullptr);
			trg.lock = lock;

			// clear fiber context info
			fiber::resetCurrentFiberInfo();

			// switch context
			int success = swapcontext(&src.context,&trg.context);
			if (success != 0) assert_fail() << "Unable to switch thread context!";

			// process final task of previous context
			src.postSwapHandler.trigger();
			src.postSwapHandler.reset();

			// unlock src-lock (which after the swap is in the source context)
			if (src.lock) {
				src.lock->unlock();
				src.lock = nullptr;
			}

			// reset current fiber information
			fiber::setCurrentFiberInfo(currentFiber);

			// run suspension handler
			if (currentFiber) currentFiber->eventHandler.resume.trigger();

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
			fiber::setCurrentFiberInfo(&info);
			assert_true(fiber::getCurrentFiberInfo());

			{

				// move lambda into this context
				Fun fun(std::move(lambda));

				// process the passed function
				fun();

			} // destruct function context

			// swap back to continuation
			fiber::ext_ucontext_t local;
			fiber::ext_ucontext_t& continuation = *info.continuation;
			info.continuation = nullptr;

			// register terminate handler as a post-swap action
			continuation.postSwapHandler = info.eventHandler.terminate;

			// free info block
			auto& pool = info.ctxt.pool;
			pool.freeListLock.lock();
			pool.free.push_back(&info);

			// remove suspend notifier (should not be triggered upon termination)
			info.eventHandler.suspend.reset();

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

		inline void EventRegister::trigger(EventId event) {
			assert_ne(EVENT_IGNORE,event);

			std::vector<Fiber*> waiting;
			{
				guard g(lock);
				auto range = events.equal_range(event);
				assert_true(range.first != range.second) << "Invalid event: " << event;
				for(auto it = range.first; it != range.second; it++) {
					if (it->second) {
						waiting.push_back(it->second);
					}
				}
				events.erase(range.first,range.second);
			}

			// if there is nothing, there is nothing to do
			if (waiting.empty()) return;

			// resume waiting tasks
			waiting.front()->ctxt.resume(waiting);
		}

		inline void ConditionalVariable::notifyOne() {
			Fiber* f = nullptr;
			{
				guard g(waitingListLock);
				if (waiting.empty()) return;
				f = waiting.back();
				waiting.pop_back();
			}
			f->ctxt.resume(*f);
		}

		inline void ConditionalVariable::notifyAll() {
			std::vector<Fiber*> fibers;
			{
				guard g(waitingListLock);
				if (waiting.empty()) return;
				fibers.swap(waiting);
			}
			fibers.front()->ctxt.resume(fibers);
		}

		inline void suspend() {
			auto fiber = getCurrentFiber();
			assert_true(fiber) << "Error: can not suspend non-fiber context!";
			fiber->ctxt.suspend(*fiber);
		}

		inline void suspend(Priority priority) {
			auto fiber = getCurrentFiber();
			assert_true(fiber) << "Error: can not suspend non-fiber context!";
			auto old = fiber->priority;
			fiber->priority = priority;
			fiber->ctxt.suspend(*fiber);
			fiber->priority = old;
		}

		inline void suspend(EventId event) {
			auto fiber = getCurrentFiber();
			assert_true(fiber) << "Error: can not suspend non-fiber context!";
			fiber->ctxt.getEventRegister().waitFor(event, fiber);
		}

		inline void suspend(EventId event, Priority priority) {
			auto fiber = getCurrentFiber();
			assert_true(fiber) << "Error: can not suspend non-fiber context!";
			auto old = fiber->priority;
			fiber->priority = priority;
			fiber->ctxt.getEventRegister().waitFor(event, fiber);
			fiber->priority = old;
		}

	} // end namespace fiber


} // end namespace utils
} // end namespace allscale
