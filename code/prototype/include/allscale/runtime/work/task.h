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
#include "allscale/utils/serializer.h"

#include "allscale/api/core/impl/reference/task_id.h"

#include "allscale/runtime/data/data_item_requirement.h"

#include "allscale/runtime/work/treeture.h"
#include "allscale/runtime/work/work_item.h"


namespace allscale {
namespace runtime {
namespace work {

	// we are reusing the reference implementations task ID
	using TaskID = allscale::api::core::impl::reference::TaskID;

	// obtains a child ID for a newly spawned task (only valid if triggered during the execution of a parent task)
	TaskID getNewChildId();

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

		friend TaskID getNewChildId();

		// a counter for the number of child tasks
		int num_children = 0;

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

		// tests whether this task can be distributed among nodes
		virtual bool canBeDistributed() const { return false; };

		// tests whether the given task is splittable
		virtual bool isSplitable() const =0;

		// obtain the process dependencies of this task
		virtual data::DataItemRequirements getProcessRequirements() const;

		// processes this task (non-split variant)
		void process();

		// obtain the split dependencies of this task
		virtual data::DataItemRequirements getSplitRequirements() const;

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
	 * A serializable wrapper around task pointer.
	 *
	 * Since the prototype here does not (yet) support actual task serialization, this
	 * wrapper is integrated for that purpose.
	 *
	 * The serialization removes ownership, while deserialization acquires it.
	 * Each instance may only be once serialized and deserialized. Also the
	 * de-serialization must take place while the original is still alive.
	 */
	class TaskReference {

		// the referenced task (in a shared pointer to support value semantic for the reference)
		std::shared_ptr<TaskPtr> task;

	public:

		// creates a new task reference owning the task
		TaskReference(TaskPtr&& task) : task(std::make_shared<TaskPtr>(std::move(task))) {
			assert_true(bool(this->task)) << "Cannot create task reference without actual task.";
		};

		// make copyable
		TaskReference(const TaskReference&) = default;

		// make moveable
		TaskReference(TaskReference&&) = default;

		// provides access to the task
		Task& operator*() {
			assert_true(bool(*task)) << "Invalid reference state.";
			return **task;
		}

		// provides access to the task
		Task* operator->() {
			assert_true(bool(*task)) << "Invalid reference state.";
			return &**task;
		}

		// conversion back into a task
		TaskPtr toTask() && {
			assert_true(bool(*task)) << "Cannot extract task multiple times.";
			return std::move(*task);
		}

		// support implicit conversion to task
		operator TaskPtr() && {
			return std::move(*this).toTask();
		}

		void store(allscale::utils::ArchiveWriter& out) const {
			// we save a memory location of the shared task pointer
			out.write<std::intptr_t>(std::intptr_t(task.get()));
		}

		static TaskReference load(allscale::utils::ArchiveReader& in) {
			// get the reference to the source
			TaskPtr* src = (TaskPtr*)in.read<std::intptr_t>();
			assert_true(bool(*src)) << "Cannot deserialize same object multiple times!";
			// transfer ownership
			return std::move(*src);
		}
	};

	/**
	 * A factory function for task pointer.
	 */
	template<typename T, typename ... Args>
	std::unique_ptr<T> make_task(const TaskID& id, Args&& ... args) {
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

	namespace detail {

		// a utility to link the result of an operation to a treeture state

		template<typename R>
		struct set_result {
			template<typename Op>
			void operator()(detail::treeture_state_handle<R>& state, const Op& op) {
				assert_true(state);
				state->set(op().get_result());
			}
		};

		template<>
		struct set_result<void> {
			template<typename Op>
			void operator()(detail::treeture_state_handle<void>& state, const Op& op) {
				assert_true(state);
				op();
				state->set();
			}
		};

	} // end namespace detail


	/**
	 * A task derived from a work item description that cannot be distributed.
	 */
	template<typename WorkItemDesc, typename Closure>
	class WorkItemTask : public Task {

		using closure_type = Closure;
		using result_type = typename WorkItemDesc::result_type;

		// the closure parameterizing this work item task
		closure_type closure;

		// the state of the promise - for all handed out treetures
		detail::treeture_state_handle<result_type> state;

	public:

		WorkItemTask(const TaskID& id, closure_type&& closure)
			: Task(id), closure(std::move(closure)), state(detail::make_incomplete_state<result_type>()) {}

		virtual bool isSplitable() const override {
			return WorkItemDesc::can_spit_test::call(closure);
		}

		virtual void processInternal() override {
			detail::set_result<result_type>()(state,[&](){
				return WorkItemDesc::process_variant::execute(closure);
			});
		}

		virtual void splitInternal() override {
			detail::set_result<result_type>()(state,[&](){
				return WorkItemDesc::split_variant::execute(closure);
			});
		}

		// obtains the treeture referencing the value produced by this task
		treeture<result_type> getTreeture() const {
			return state;
		}

	};

	/**
	 * A task derived from a work item description that can be distributed.
	 */
	template <
		typename Result,				// the result type of this work item
		typename Name,					// a struct producing the name of this work item
		typename SplitVariant,			// the split variant implementation
		typename ProcessVariant,		// the process variant implementation
		typename CanSplitTest,			// the can-split test
		typename Closure				// the closure of this task
	>
	class WorkItemTask<work_item_description<Result, Name, do_serialization, SplitVariant, ProcessVariant, CanSplitTest>,Closure> : public Task {

		using closure_type = Closure;
		using result_type = Result;

		// the closure parameterizing this work item task
		closure_type closure;

		// the state of the promise - for all handed out treetures
		detail::treeture_state_handle<result_type> state;

	public:

		WorkItemTask(const TaskID& id, closure_type&& closure)
			: Task(id), closure(std::move(closure)), state(detail::make_incomplete_state<result_type>()) {}

		virtual bool canBeDistributed() const override {
			return true;
		}

		virtual bool isSplitable() const override {
			return CanSplitTest::call(closure);
		}

		virtual data::DataItemRequirements getProcessRequirements() const override {
			return data::DataItemRequirements::fromTuple(ProcessVariant::get_requirements(closure));
		}

		virtual void processInternal() override {
			detail::set_result<result_type>()(state,[&](){
				return ProcessVariant::execute(closure);
			});
		}

		virtual data::DataItemRequirements getSplitRequirements() const override {
			return data::DataItemRequirements::fromTuple(SplitVariant::get_requirements(closure));
		}

		virtual void splitInternal() override {
			detail::set_result<result_type>()(state,[&](){
				return SplitVariant::execute(closure);
			});
		}

		// obtains the treeture referencing the value produced by this task
		treeture<result_type> getTreeture() const {
			return state;
		}

	};

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
