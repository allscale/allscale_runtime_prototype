#pragma once

#include <mutex>

#include <allscale/utils/assert.h>
#include <allscale/utils/spinlock.h>
#include <allscale/utils/optional.h>

#include "allscale/utils/fibers.h"

namespace allscale {
namespace utils {
namespace fiber {

	// -------- Fiber Futures ------------


	template<typename T>
	class Promise;

	template<typename T>
	class Future;

	namespace detail {

		template<typename T>
		class PromiseFutureBase {

			friend class Promise<T>;
			friend class Future<T>;

			/**
			 * A global lock per future type for synchronizing promise/future
			 * connections.
			 */
			static spinlock& getLock() {
				static spinlock lock;
				return lock;
			}

		};

	}

	template<typename T>
	class Future {

		friend class Promise<T>;

		using promise_t = Promise<T>;

		using guard = std::lock_guard<spinlock>;

		FiberContext* ctxt;

		EventId event;

		promise_t* promise;

		allscale::utils::optional<T> value;


		Future(promise_t& promise) : ctxt(promise.ctxt), event(promise.event), promise(&promise) {
			promise.set_future(*this);
		};

	public:

		Future() : ctxt(nullptr), event(), promise(nullptr) {}

		Future(const Future& other) = delete;

		Future(Future&& other) : ctxt(other.ctxt), event(other.event), promise(nullptr) {

			guard g(detail::PromiseFutureBase<T>::getLock());

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

		~Future() {
			guard g(detail::PromiseFutureBase<T>::getLock());
			if (promise) promise->reset_future();
		}

		// support initialization with the result value
		Future(const T& value) : ctxt(nullptr), event(EVENT_IGNORE), promise(nullptr), value(value) {}
		Future(T&& value) : ctxt(nullptr), event(EVENT_IGNORE), promise(nullptr), value(std::move(value)) {}

		Future& operator=(const Future&) = delete;

		Future& operator=(Future&& other) {
			//
			if (this == &other) return *this;

			ctxt = other.ctxt;
			event = other.event;

			{
				guard g(detail::PromiseFutureBase<T>::getLock());

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

			return *this;
		}

		bool valid() const {
			guard g(detail::PromiseFutureBase<T>::getLock());
			return promise || bool(value);
		}

		void wait() const {
			assert_true(valid());
			if (!ctxt) return;
			ctxt->getEventRegister().waitFor(event);
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
		void deliver(const T& result) {
			value = result;
		}

		/**
		 * Delivers the result to this future, returns a potential
		 * fiber waiting for this result to be resumed.
		 */
		void deliver(T&& result) {
			value = std::forward<T>(result);
		}

		// callback from promise on move operation
		void set_promise(Promise<T>& p) {
			promise = &p;
		}

	};


	template<typename T>
	class Promise {

		using future_t = Future<T>;

		friend class Future<T>;

		FiberContext* ctxt;

		// the event used for synchronization
		EventId event;

		// the future the result should be delivered to (at most one)
		future_t* future;

		using guard = std::lock_guard<spinlock>;

	public:

		Promise()
			: ctxt(nullptr), event(EVENT_IGNORE), future(nullptr) {}

		Promise(FiberContext& ctxt)
			: ctxt(&ctxt), event(ctxt.getEventRegister().create()), future(nullptr) {}

		Promise(const Promise&) = delete;

		Promise(Promise&& other) : ctxt(other.ctxt), event(other.event), future(nullptr) {
			{
				guard g(detail::PromiseFutureBase<T>::getLock());
				future = other.future;
				if (future) future->setPromise(this);
			}
			other.ctxt = nullptr;
		}

		~Promise() {
			if (!ctxt) return;
			assert_false(future) << "Promise destroyed without delivery!";
		}

		Promise& operator=(const Promise&) = delete;

		Promise& operator=(Promise&& other) {
			if (this == &other) return *this;

			ctxt = other.ctxt;
			event = other.event;

			{
				guard g(detail::PromiseFutureBase<T>::getLock());
				future = other.future;
				if (future) future->set_promise(*this);
			}

			other.ctxt = nullptr;
			return *this;
		}

		bool valid() {
			return ctxt;
		}

		/**
		 * Obtains a future associated to this promise. This function
		 * must only be called once.
		 */
		future_t get_future() {
			assert_true(valid());
			assert_true(nullptr == future);
			future_t res(*this);
			return res;
		}

		/**
		 * Delivers the given value to the future associated to this promise.
		 * This function must only be called once.
		 */
		void set_value(const T& value) {
			assert_true(valid());
			// deliver value synced with move operations of future
			{
				guard g(detail::PromiseFutureBase<T>::getLock());
				if (future) {
					future->deliver(value);
					future = nullptr;
				}
			}
			ctxt->getEventRegister().trigger(event);
		}


		/**
		 * Delivers the given value to the future associated to this promise.
		 * This function must only be called once.
		 */
		void set_value(T&& value) {
			assert_true(valid());
			// deliver value synced with move operations of future
			{
				guard g(detail::PromiseFutureBase<T>::getLock());
				if (future) {
					future->deliver(std::forward<T>(value));
					future = nullptr;
				}
			}
			ctxt->getEventRegister().trigger(event);
		}

	private:

		// callback from future on move operation
		void set_future(Future<T>& f) {
			future = &f;
		}

		// callback form future on destruction
		void reset_future() {
			future = nullptr;
		}

	};


}
}
}
