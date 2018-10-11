#pragma once

#include <cstdint>
#include <stdlib.h>
#include <ucontext.h>
#include <mutex>
#include <vector>

#include "allscale/utils/assert.h"
#include "allscale/utils/optional.h"

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

		// we use 1 MB per fiber stack
		constexpr static std::size_t STACK_SIZE = (1<<20);

		// the definition of a stack for a fiber
		using stack_t = char[STACK_SIZE/sizeof(char)];

		// the management information required per fiber
		struct fiber_info {
			FiberPool& pool;			// < the pool it belongs to
			bool running;				// < the running state
			stack_t* stack;				// < the associated stack memory
			ucontext_t* continuation;	// < an optional continuation to be processed after finishing or suspending a fiber

			fiber_info(FiberPool& pool)
				: pool(pool),
				  running(false),
				  stack((stack_t*)aligned_alloc(STACK_SIZE,STACK_SIZE)),
				  continuation(nullptr) {}

			fiber_info(const fiber_info&) = delete;
			fiber_info(fiber_info&& other) = delete;
			~fiber_info() { free(stack); }
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

		FiberPool(std::size_t initialSize = 20) {
			// create an initial pool of fiber infos
			infos.reserve(initialSize);
			freeInfos.reserve(initialSize);
			for(std::size_t i=0; i<initialSize; i++) {
				infos.push_back(new fiber_info(*this));
				freeInfos.push_back(infos.back());
			}
		}

		FiberPool(const FiberPool&) = delete;
		FiberPool(FiberPool&&) = delete;

		~FiberPool() {
			// clear owned fiber info entries
			for(const auto& cur : infos) {
				// make sure processing has completed!
				assert_false(cur->running);
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
				guard g(lock);

				// re-use an existing fiber or create a new one
				if (!freeInfos.empty()) {
					fiber = freeInfos.back();
					freeInfos.pop_back();
				} else {
					fiber = new fiber_info(*this);
					infos.push_back(fiber);
				}
			}

			assert_true(fiber);

			// make sure the fiber is not running
			assert_false(fiber->running);

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
			target.uc_stack.ss_size = STACK_SIZE;

			// encode pool pointer and pointer to lambda into integers
			static_assert(2*sizeof(int) == sizeof(void*), "Assumption on size of pointer failed!\n");
			auto fiberInfoPtr = reinterpret_cast<std::intptr_t>(fiber);
			auto lambdaPtr = reinterpret_cast<std::intptr_t>(&lambda);

			// set starting point function
			makecontext(&target,(void(*)())&exec<Fun>,4,
					int(fiberInfoPtr >> 32), int(fiberInfoPtr),
					int(lambdaPtr >> 32), int(lambdaPtr)
			);

			// switch to fiber context
			int success = swapcontext(&local,&target);
			if (success != 0) assert_fail() << "Unable to switch thread context to fiber!";

			// determine whether fiber is still running
			if (fiber->running) {
				return fiber;
			}

			// return fiber info to re-use list
			{
				guard g(lock);
				freeInfos.push_back(fiber);
			}

			// fiber is done, nothing to return
			return {};
		}

		/**
		 * Suspends the currently processed fiber. This function
		 * must only be called within the context of a fiber managed
		 * by this pool.
		 */
		static void suspend() {

			// back up current state
			auto backup = getCurrentFiberInfo();

			// make sure this is within a fiber
			assert_true(backup);

			// make sure this fiber is indeed running
			assert_true(backup->running);

			// remove state information
			resetCurrentFiberInfo();

			// capture local state
			ucontext_t local;
			getcontext(&local);

			// get previous continuation
			ucontext_t& continuation = *backup->continuation;

			// make local state the new continuation for this fiber
			backup->continuation = &local;

			// switch to old continuation
			int success = swapcontext(&local,&continuation);
			if (success != 0) assert_fail() << "Unable to switch thread context back to continuation!";

			// after resuming:

			// restore state
			setCurrentFiberInfo(backup);
		}

		/**
		 * Resumes the processing of the given fiber previously suspended.
		 * This must not be called within a fiber.
		 *
		 * @param f the fiber to be continued (must be active)
		 * @return true if the fiber has been again suspended, false otherwise
		 */
		bool resume(Fiber f) {
			// make sure this is not within a fiber
			assert_false(getCurrentFiberInfo());

			// make sure fiber is active
			assert_true(f->running);

			// get local context
			ucontext_t local;
			getcontext(&local);

			// get current fiber continuation
			ucontext_t& continuation = *f->continuation;

			// set local as the next continuation
			f->continuation = &local;

			// switch to context
			int success = swapcontext(&local,&continuation);
			if (success != 0) assert_fail() << "Unable to switch thread context back to fiber!";

			// after return: should not be in a context
			assert_false(getCurrentFiberInfo());

			// determine whether task is complete
			if (f->running) return true;

			// return info object to re-use list
			{
				guard g(lock);
				freeInfos.push_back(f);
			}

			// signal that fiber has completed its task
			return false;
		}

	private:

		static fiber_info*& getCurrentFiberInfo() {
			static thread_local fiber_info* info;
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
			info.running = true;

			{

				// move lambda into this context
				Fun fun(std::move(lambda));

				// process the passed function
				fun();

			} // destruct function context

			// mark fiber as done
			info.running = false;

			// reset thread-local state
			resetCurrentFiberInfo();

			// swap back to continuation
			ucontext_t local;
			ucontext_t& continuation = *info.continuation;
			info.continuation = nullptr;
			int success = swapcontext(&local,&continuation);
			if (success != 0) assert_fail() << "Unable to switch thread context back to continuation!";

			// Note: this code will never be reached!
		}

	};

} // end namespace utils
} // end namespace allscale
