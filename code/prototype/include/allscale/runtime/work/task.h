/*
 * The prototype version of a task.
 *
 *  Created on: Jul 24, 2018
 *      Author: herbert
 */

#pragma once

#include <atomic>
#include <memory>
#include <ostream>

#include "allscale/utils/assert.h"
#include "allscale/api/core/impl/reference/task_id.h"

namespace allscale {
namespace runtime {
namespace work {

	// we are reusing the reference implementations task ID
	using TaskID = allscale::api::core::impl::reference::TaskID;

	/**
	 * An abstract base class of all tasks.
	 */
	class Task {

		// the various states a task could be in
		enum State {
			// Blocked,	// < still has dependencies to wait for -- TODO: support those once necessary
			Ready, 		// < ready to run
			Running, 	// < currently running
			Finished	// < completed
		};

		// the id of this task
		TaskID id;

		// indicates the completion state of this task
		std::atomic<State> state;

	public:

		// creates a new task, not completed yet
		Task(TaskID id) : id(id), state(Ready) {}

		// tasks are not copy nor moveable
		Task(const Task&) = delete;
		Task(Task&&) = delete;

		virtual ~Task() {
			// make sure all tasks are processed
			assert_eq(state,Finished) << "Destroying incomplete task!";
		}

		// ----- observer -----

		const TaskID& getId() const {
			return id;
		}

		bool isReady() const {
			return state == Ready;
		}

		bool isDone() const {
			return state == Finished;
		}

		// ----- task interface -----

		// tests whether the given task is splittable
		virtual bool isSplitable() const =0;

		// processes this task (non-split variant)
		void process();

		// processes this task (split variant)
		void split();

		// TODO: add get dependencies

		// ----- utilities -----

		// support printing of task states
		friend std::ostream& operator<<(std::ostream&,const State&);

		// support printing of tasks
		friend std::ostream& operator<<(std::ostream&,const Task&);

	protected:

		// the process variant to be overloaded by task implementations
		virtual void processInternal() =0;

		// the split variant to be overloaded by task implementations
		virtual void splitInternal() =0;

	};

	// a pointer type for tasks
	using TaskPtr = std::unique_ptr<Task>;

	/**
	 * A factory function for task pointer.
	 */
	template<typename T, typename ... Args>
	TaskPtr make_task(const TaskID& id, Args&& ... args) {
		return std::make_unique<T>(id,std::forward<Args>(args)...);
	}

	/**
	 * A simple task wrapping up a lambda for processing some task asynchronously.
	 */
	template<typename Op>
	class LambdaTask : public Task {
		Op op;
	public:
		LambdaTask(const TaskID& id, Op&& op) : Task(id), op(std::forward<Op>(op)) {}
		virtual bool isSplitable() const override { return false; }
		virtual void processInternal() override { op(); };
		virtual void splitInternal() override {
			assert_fail() << "Invalid call!";
		};
	};

	/**
	 * A factory function for a lambda task pointer.
	 */
	template<typename Op>
	TaskPtr make_lambda_task(const TaskID& id, Op&& op) {
		return make_task<LambdaTask<Op>>(id,std::forward<Op>(op));
	}

	/**
	 * A no-op task for testing.
	 */
	class NullTask : public Task {
	public:
		NullTask(const TaskID& id) : Task(id) {}
		virtual bool isSplitable() const override { return false; }
		virtual void processInternal() override {};
		virtual void splitInternal() override {};
	};

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
