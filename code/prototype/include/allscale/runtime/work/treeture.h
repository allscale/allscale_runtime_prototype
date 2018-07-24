/*
 * The prototype version of a treeture.
 *
 *  Created on: Jul 24, 2018
 *      Author: herbert
 */

#pragma once

#include <atomic>
#include <memory>
#include <thread>

namespace allscale {
namespace runtime {
namespace work {

	// forward declaration (implemented by worker.h/cpp)
	void yield();


	namespace detail {

		/**
		 * The shared state of treetures, maintaining the result once the computation is done.
		 */
		template<typename R>
		class treeture_state {

			// null while not ready, set when ready
			std::unique_ptr<R> value;

		public:

			treeture_state() {}

			treeture_state(R&& value) : value(std::make_unique<R>(std::move(value))) {}

			bool isDone() const {
				return (bool)value;
			}

			void set(R&& newValue) {
				value = std::make_unique<R>(std::move(newValue));
			}

			R&& getValue() {
				return std::move(*value);
			}
		};

		/**
		 * A specialization of the shared treeture state for void values.
		 */
		template<>
		class treeture_state<void> {

			// a flag indicating whether the task is done
			std::atomic<bool> done;

		public:

			treeture_state(bool done = false) : done(done) {}

			void set() {
				done = true;
			}

			bool isDone() const {
				return done;
			}
		};

		// the type of handler used to reference to treeture results
		template<typename R>
		using treeture_state_handle = std::shared_ptr<treeture_state<R>>;

		template<typename R>
		treeture_state_handle<R> make_complete_state(R&& value) {
			return std::make_shared<treeture_state<R>>(std::move(value));
		}

		inline treeture_state_handle<void> make_complete_state() {
			return std::make_shared<treeture_state<void>>(true);
		}

		template<typename R>
		treeture_state_handle<R> make_incomplete_state() {
			return std::make_shared<treeture_state<R>>();
		}

		inline treeture_state_handle<void> make_incomplete_state() {
			return std::make_shared<treeture_state<void>>(false);
		}
	}


	/**
	 * A treeture is a handle to an asynchronously, recursively computed value.
	 */
	template<typename R>
	class treeture {

		detail::treeture_state_handle<R> state;

	public:

		treeture() {}

		treeture(const detail::treeture_state_handle<R>& state) : state(state) {}

		treeture(R&& value) : state(detail::make_complete_state(std::move(value))) {}

		bool isDone() const {
			return !state || state->isDone();
		}

		R&& get_result() const {
			assert_true(state);
			wait();
			return state->getValue();
		}

		void wait() const {
			while (!isDone()) {
				yield();
			}
		}

	};

	/**
	 * A treeture is a handle to an asynchronously, recursively computed value.
	 */
	template<>
	class treeture<void> {

		detail::treeture_state_handle<void> state;

	public:

		treeture() {}

		treeture(detail::treeture_state_handle<void>& state) : state(state) {}

		bool isDone() const {
			return !state || state->isDone();
		}

		void wait() const {
			while (!isDone()) {
				yield();
			}
		}

	};


} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
