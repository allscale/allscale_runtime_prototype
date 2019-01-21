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
#include <type_traits>

#include "allscale/utils/assert.h"
#include "allscale/utils/serializer.h"
#include "allscale/utils/serializer/tuple.h"
#include "allscale/utils/serializer/functions.h"

#include "allscale/runtime/data/data_item_requirement.h"

#include "allscale/runtime/work/task_id.h"
#include "allscale/runtime/work/task_dependency.h"
#include "allscale/runtime/work/treeture.h"
#include "allscale/runtime/work/work_item.h"

#include "allscale/runtime/log/logger.h"

namespace allscale {
namespace runtime {
namespace work {

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
		TaskRef ref;

		// the list of tasks to be completed before starting processing this task
		TaskDependencies dependencies;

		// indicates the completion state of this task
		std::atomic<State> state;

		friend TaskID getNewChildId();

		// a counter for the number of child tasks
		int num_children = 0;

		// a buffer for backing up processing nodes while suspending tasks
		com::Node* processingNode = nullptr;

	public:

		// creates a new task, based on the given ingredients
		Task(TaskRef ref, TaskDependencies&& dependencies)
			: ref(ref), dependencies(std::move(dependencies)), state(Ready) {}

		// tasks are not copy nor moveable
		Task(const Task&) = delete;
		Task(Task&&) = delete;

		virtual ~Task() {
			// make sure all tasks are processed
			assert_eq(state,Finished) << "Destroying incomplete task: " << getId();
		}

		// ----- observer -----

		const TaskID& getId() const {
			return ref.getTaskID();
		}

		com::rank_t getOwner() const {
			return ref.getOwner();
		}

		const TaskRef& getTaskRef() const {
			return ref;
		}

		const TaskDependencies& getDependencies() const {
			return dependencies;
		}

		bool isReady() const {
			return state == Ready;
		}

		bool isDone() const {
			return state == Finished;
		}

		void cancel() {
			assert_eq(Ready,state);
			state = Finished;
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


		// ----- utilities -----

		// obtains a pointer to the currently processed task, or null if there is none
		static Task* getCurrent();

		// support printing of task states
		friend std::ostream& operator<<(std::ostream&,const State&);

		// support printing of tasks
		friend std::ostream& operator<<(std::ostream&,const Task&);

		// a function to be called when resuming a task
		static void notifyResume(Task* task);

		// ----- serialization -----

		// the type of the load function to be provided by implementations
		using load_fun_t = std::unique_ptr<Task>(*)(const TaskRef&,TaskDependencies&&,allscale::utils::ArchiveReader&);

		void store(allscale::utils::ArchiveWriter& out) const {
			out.write(getTaskRef());
			out.write(getDependencies());
			out.write(getLoadFunction());
			storeInternal(out);
		}

		static std::unique_ptr<Task> load(allscale::utils::ArchiveReader& in) {
			auto ref = in.read<TaskRef>();
			auto deps = in.read<TaskDependencies>();
			load_fun_t load = in.read<load_fun_t>();
			return load(ref,std::move(deps),in);
		}

	protected:

		// the process variant to be overloaded by task implementations
		virtual void processInternal() =0;

		// the split variant to be overloaded by task implementations
		virtual void splitInternal() =0;

		// saves a copy of the task to the given stream
		virtual void storeInternal(allscale::utils::ArchiveWriter& out) const =0;

		// retrieves the function capable of de-serializing a task instance
		virtual load_fun_t getLoadFunction() const =0;

	};

	// a pointer type for tasks
	using TaskPtr = std::unique_ptr<Task>;

	/**
	 * A serializable wrapper around task pointer.
	 *
	 * Typically, tasks are passed around using task pointer, yet those are not serializable.
	 * Task references take ownership of a task and can be serialized. By doing so, ownership is lost.
	 * Through de-serialization, ownership is gained on a new instance.
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
			(*task)->store(out);
			(*task)->cancel();
		}

		static TaskReference load(allscale::utils::ArchiveReader& in) {
			// restore task
			return Task::load(in);
		}
	};

	/**
	 * A factory function for task pointer including dependencies.
	 */
	template<typename T, typename ... Args>
	std::unique_ptr<T> make_task(const TaskID& id, TaskDependencies&& deps, Args&& ... args) {
		return std::make_unique<T>(TaskRef(id,com::Node::getLocalRank()),std::move(deps), std::forward<Args>(args)...);
	}

	/**
	 * A factory function for task pointer.
	 */
	template<typename T, typename ... Args>
	std::unique_ptr<T> make_task(const TaskID& id, Args&& ... args) {
		return make_task<T,Args...>(id,TaskDependencies(),std::forward<Args>(args)...);
	}

	/**
	 * An abstract task type computing a value.
	 */
	template<typename R>
	class ComputeTask : public Task {

		using Task::load_fun_t;

	public:

		// the first flag indicates whether this task is created the first time or is the result of a copy/clone/serialization
		ComputeTask(const TaskRef& ref, TaskDependencies&& deps, bool first = false) : Task(ref, std::move(deps)) {
			// register this task for the treeture service
			if (first) TreetureStateService::getLocal().registerTask<R>(getId());
		}

		// obtains the treeture referencing the value produced by this task
		treeture<R> getTreeture() const {
			return getTaskRef();
		}

		void setResult(R&& value) {
			// mark this task as done
			TreetureStateService::getLocal().setDone<R>(getTaskRef(),std::move(value));
		}
	};

	// a specialization for void
	template<>
	class ComputeTask<void> : public Task {

		using Task::load_fun_t;

	public:

		ComputeTask(const TaskRef& ref, TaskDependencies&& deps, bool first = false) : Task(ref,std::move(deps)) {
			// register this task for the treeture service
			if (first) TreetureStateService::getLocal().registerTask<void>(getId());
		}

		// obtains the treeture referencing the value produced by this task
		treeture<void> getTreeture() const {
			return getTaskRef();
		}

		void setDone() {
			// mark this task as done
			TreetureStateService::getLocal().setDone(getTaskRef());
		}
	};


	namespace detail {

		// a utility to link the result of an operation to a treeture state

		template<typename R>
		struct set_result {
			template<typename Op>
			void operator()(ComputeTask<R>& task, const Op& op) {
				task.setResult(op().get_result());
			}
		};

		template<>
		struct set_result<void> {
			template<typename Op>
			void operator()(ComputeTask<void>& task, const Op& op) {
				op();
				task.setDone();
			}
		};

	} // end namespace detail


	/**
	 * A task derived from a work item description that cannot be distributed.
	 */
	template<typename WorkItemDesc, typename Closure>
	class WorkItemTask : public ComputeTask<typename WorkItemDesc::result_type> {

		using closure_type = Closure;
		using result_type = typename WorkItemDesc::result_type;
		using load_fun_t = Task::load_fun_t;

		// the closure parameterizing this work item task
		closure_type closure;

	public:

		WorkItemTask(const TaskRef& ref, TaskDependencies&& deps, closure_type&& closure, bool first = true)
			: ComputeTask<result_type>(ref,std::move(deps),first), closure(std::move(closure)) {}

		virtual bool isSplitable() const override {
			return WorkItemDesc::can_spit_test::call(closure);
		}

		virtual void processInternal() override {
			detail::set_result<result_type>()(*this,[&](){
				return WorkItemDesc::process_variant::execute(closure);
			});
		}

		virtual void splitInternal() override {
			detail::set_result<result_type>()(*this,[&](){
				return WorkItemDesc::split_variant::execute(closure);
			});
		}

		// saves a copy of the task to the given stream
		virtual void storeInternal(allscale::utils::ArchiveWriter&) const override {
			assert_fail() << "No serialization supported for this task.";
		}

		static std::unique_ptr<Task> load(const TaskRef&, TaskDependencies&&, allscale::utils::ArchiveReader&) {
			assert_fail() << "No serialization supported for this task.";
			return {};
		}

		// retrieves the function capable of de-serializing a task instance
		virtual load_fun_t getLoadFunction() const override {
			return &load;
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
		typename ... ClosureArgs		// the elements of the closure
	>
	class WorkItemTask<work_item_description<Result, Name, do_serialization, SplitVariant, ProcessVariant, CanSplitTest>,std::tuple<ClosureArgs...>> : public ComputeTask<Result> {

		using closure_type = std::tuple<ClosureArgs...>;
		using result_type = Result;
		using load_fun_t = Task::load_fun_t;

		// the closure parameterizing this work item task
		closure_type closure;

	public:

		WorkItemTask(const TaskRef& ref, TaskDependencies&& deps, closure_type&& closure, bool first = true)
			: ComputeTask<result_type>(ref,std::move(deps),first), closure(std::move(closure)) {}

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
			detail::set_result<result_type>()(*this,[&](){
				return ProcessVariant::execute(closure);
			});
			DLOG << "Task " << this->getId() << " processing completed!\n";
		}

		virtual data::DataItemRequirements getSplitRequirements() const override {
			return data::DataItemRequirements::fromTuple(SplitVariant::get_requirements(closure));
		}

		virtual void splitInternal() override {
			detail::set_result<result_type>()(*this,[&](){
				return SplitVariant::execute(closure);
			});
			DLOG << "Task " << this->getId() << " splitting completed!\n";
		}

		// saves a copy of the task to the given stream
		virtual void storeInternal(allscale::utils::ArchiveWriter& out) const override {
			out.write<closure_type>(closure);
		}

		static std::unique_ptr<Task> load(const TaskRef& ref, TaskDependencies&& deps, allscale::utils::ArchiveReader& in) {
			return std::make_unique<WorkItemTask>(ref,std::move(deps),in.read<closure_type>(),false);
		}

		// retrieves the function capable of de-serializing a task instance
		virtual load_fun_t getLoadFunction() const override {
			return &load;
		}

	};


	/**
	 * A factory function for a lambda task pointer.
	 */
	template<typename Op>
	std::enable_if_t<!std::is_void<std::result_of_t<Op()>>::value,std::unique_ptr<ComputeTask<std::result_of_t<Op()>>>>
	make_lambda_task(const TaskID& id, Op&& op) {

		using result_type = std::result_of_t<Op()>;
		using treeture_type = treeture<result_type>;

		struct name {};

		struct split {
			static treeture_type execute(const Op& op) {
				assert_fail();
				return op();
			}
		};

		struct process {
			static treeture_type execute(const Op& op) {
				return op();
			}
		};

		struct no_split {
			static bool call(const Op&) {
				return false;
			}
		};

		using desc = work_item_description<
			std::result_of_t<Op()>,
			name,
			no_serialization,
			split,
			process,
			no_split
		>;

		return std::make_unique<WorkItemTask<desc,Op>>(TaskRef(id,com::Node::getLocalRank()),TaskDependencies(),std::forward<Op>(op));
	}

	/**
	 * A factory function for a lambda task pointer producing a void.
	 */
	template<typename Op>
	std::enable_if_t<std::is_void<std::result_of_t<Op()>>::value,std::unique_ptr<ComputeTask<void>>>
	make_lambda_task(const TaskID& id, Op&& op) {

		using treeture_type = treeture<void>;

		struct name {};

		struct split {
			static treeture_type execute(const Op& op) {
				assert_fail();
				op();
				return true;
			}
		};

		struct process {
			static treeture_type execute(const Op& op) {
				op();
				return true;
			}
		};

		struct no_split {
			static bool call(const Op&) {
				return false;
			}
		};

		using desc = work_item_description<
			void,
			name,
			no_serialization,
			split,
			process,
			no_split
		>;

		return std::make_unique<WorkItemTask<desc,Op>>(TaskRef(id,com::Node::getLocalRank()),TaskDependencies(),std::forward<Op>(op));
	}

} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale
