/*
 * The prototype version of a treeture.
 *
 *  Created on: Jul 24, 2018
 *      Author: herbert
 */

#pragma once

#include <atomic>
#include <array>
#include <bitset>
#include <map>
#include <unordered_map>
#include <memory>
#include <mutex>

#include "allscale/utils/assert.h"
#include "allscale/utils/optional.h"
#include "allscale/utils/serializer.h"
#include "allscale/utils/serializer/optionals.h"
#include "allscale/utils/fibers.h"

#include "allscale/runtime/com/node.h"
#include "allscale/runtime/com/network.h"
#include "allscale/runtime/work/task_id.h"
#include "allscale/runtime/work/fiber_service.h"

#include "allscale/utils/printer/vectors.h"

namespace allscale {
namespace runtime {
namespace work {



	// -- setup --

	// start up the treeture service within the given network
	void installTreetureStateService(com::Network&);


	// -- treetures --

	// forward declaration (implemented by worker.h/cpp)
	void yield();


	namespace detail {

		using TaskPath = allscale::api::core::impl::reference::TaskPath;

		// the maximum level of tasks
		static constexpr unsigned MAX_TASK_LEVELS = 20;



		inline std::size_t toPosition(const TaskPath& path) {

			// get length and path
			auto l = path.getLength();
			auto p = path.getPath();

			// limit length to max_depth
			assert_lt(l,MAX_TASK_LEVELS);

			// compute result
			return (decltype(p)(1) << l) | p;
		}


		class TreetureStateServiceBase {

		public:
			virtual ~TreetureStateServiceBase() {}

		};

		template<typename R>
		class TreetureStateService : public TreetureStateServiceBase {

			mutable allscale::utils::spinlock lock;

			using guard = std::lock_guard<allscale::utils::spinlock>;

			allscale::utils::fiber::EventRegister& eventRegister;

			// the list of completed tasks and their results
			std::unordered_map<TaskPath,R> results;

			// lazy-generated events used to sync on sub-treetures
			std::unordered_map<TaskPath,allscale::utils::fiber::EventId> events;

		public:

			TreetureStateService(allscale::utils::fiber::EventRegister& reg) : eventRegister(reg) {}

			void setResult(const TaskPath& path, R&& value) {

				guard g(lock);

				// the task must not be done yet (can only trigger once)
				assert_false(isDone(path));

				// record the result
				results.emplace( path, std::move(value) );

				// trigger potential registered events of this task or sub-tasks
				for(auto it = events.begin(), last = events.end(); it != last;) {
					if (isSubPath(path,it->first)) {
						eventRegister.trigger(it->second);
						it = events.erase(it);
					} else {
						++it;
					}
				}

			}

			allscale::utils::fiber::EventId getSyncEvent(const TaskPath& path) {

				guard g(lock);

				// test whether the task has completed by now
				if (isDone(path)) return allscale::utils::fiber::EVENT_IGNORE;

				// test whether there is already a event waiting for this task
				auto pos = events.find(path);
				if (pos != events.end()) return pos->second;

				// register a new event
				return events[path] = eventRegister.create();
			}

			R getResult(const TaskPath& path) {
				guard g(lock);
				auto pos = results.find(path);
				assert_true(pos != results.end());
				R res = std::move(pos->second);
				results.erase(pos);
				return std::move(res);
			}

		private:

			static bool isSubPath(const TaskPath& parent, const TaskPath& child) {
				return parent == child || parent.isPrefixOf(child);
			}

			bool isDone(const TaskPath& path) const {
				return results.find(path) != results.end();
			}

		};

		template<>
		class TreetureStateService<void> : public TreetureStateServiceBase {

			mutable allscale::utils::spinlock lock;

			using guard = std::lock_guard<allscale::utils::spinlock>;

			allscale::utils::fiber::EventRegister& eventRegister;

			// the list of completed tasks
			std::vector<TaskPath> completedTasks;

			// lazy-generated events used to sync on sub-treetures
			std::unordered_map<TaskPath,allscale::utils::fiber::EventId> events;

		public:

			TreetureStateService(allscale::utils::fiber::EventRegister& reg) : eventRegister(reg) {}


			void setDone(const TaskPath& path) {

				guard g(lock);

				// the event must not be done yet
				assert_false(isDone(path)) << "Path " << path << " already done.\nCompleted: " << completedTasks << "\n";

				// mark as completed
				markDone(path);

				// trigger potential registered events of this task or sub-tasks
				for(auto it = events.begin(), last = events.end(); it != last;) {
					if (isSubPath(path,it->first)) {
						eventRegister.trigger(it->second);
						it = events.erase(it);
					} else {
						++it;
					}
				}

			}


			allscale::utils::fiber::EventId getEvent(const TaskPath& path) {

				guard g(lock);

				// test whether the task has completed by now
				if (isDone(path)) return allscale::utils::fiber::EVENT_IGNORE;

				// test whether there is already a event waiting for this task
				auto pos = events.find(path);
				if (pos != events.end()) return pos->second;

				// register a new event
				return events[path] = eventRegister.create();
			}

		private:

			static bool isSubPath(const TaskPath& parent, const TaskPath& child) {
				return parent == child || parent.isPrefixOf(child);
			}

			bool isDone(const TaskPath& path) const {
				// see whether this path or a parent path is done
				for(const auto& cur : completedTasks) {
					if (isSubPath(cur,path)) return true;
				}
				return false;
			}

			void markDone(const TaskPath& path) {

				// filter out sub-tasks (to keep list of completed tasks short)
				completedTasks.erase(std::remove_if(completedTasks.begin(), completedTasks.end(), [&](const TaskPath& cur){
					return isSubPath(path,cur);
				}), completedTasks.end());

				// add newly completed path
				completedTasks.push_back(path);
			}

		};

	} // end namespace detail


	class TreetureStateService {

		// the lock type to be utilized to protect internal state
		using lock_t = std::mutex;

		// a lock to sync concurrent accesses
		mutable lock_t lock;

		// maintains the list of treeture states of tasks managed by this service
		std::unordered_map<std::uint64_t,std::unique_ptr<detail::TreetureStateServiceBase>> states;

		// the guard type to sync concurrent accesses
		using guard = std::lock_guard<lock_t>;

		// the network being part of
		com::Network& network;

		// the rank of the node this service is running on
		com::rank_t myRank;

		// the context to be utilized for synchronization operations
		allscale::utils::fiber::EventRegister& eventRegister;

	public:

		// the service constructor
		TreetureStateService(com::Node& node)
			: network(com::Network::getNetwork()), myRank(node.getRank()), eventRegister(node.getService<FiberContextService>().getContext().getEventRegister()) {}

		static TreetureStateService& getLocal() {
			return com::Node::getLocalService<TreetureStateService>();
		}

		// -- treeture side interface --

		// tests non-blocking whether the referenced task is done
		bool wait(com::rank_t owner, const TaskID& id) {

			// test whether this is the right one
			if (myRank != owner) {
				// query remote
				return network.getRemoteProcedure(owner,&TreetureStateService::wait)(owner,id).get();
			}

			allscale::utils::fiber::EventId syncEvent;
			detail::TreetureStateService<void>* service;
			{
				guard g(lock);
				auto pos = states.find(id.getRootID());
				if (pos == states.end()) return true;
				assert_true(dynamic_cast<detail::TreetureStateService<void>*>(pos->second.get()));
				syncEvent = static_cast<detail::TreetureStateService<void>&>(*pos->second).getEvent(id.getPath());
				service = dynamic_cast<detail::TreetureStateService<void>*>(pos->second.get());
			}

			// see that processed fiber is on same event register as this treeture state service
			assert_eq(
				&allscale::utils::fiber::getCurrentFiber()->ctxt.getEventRegister(),
				&eventRegister
			);

			// suspend fiber
			allscale::utils::fiber::suspend(syncEvent);

			// make sure task is done now
			assert_decl({
				guard g(lock);
				auto pos = states.find(id.getRootID());
				assert_false(pos == states.end());
				assert_true(dynamic_cast<detail::TreetureStateService<void>*>(pos->second.get()));
				auto newService = dynamic_cast<detail::TreetureStateService<void>*>(pos->second.get());
				auto newSyncEvent = static_cast<detail::TreetureStateService<void>&>(*pos->second).getEvent(id.getPath());
				assert_eq(newSyncEvent, allscale::utils::fiber::EVENT_IGNORE)
					<< "Task: " << id << "\n"
					<< "Events: " << syncEvent << " vs " << newSyncEvent << "\n"
					<< "Services: " << service << " vs " << newService << " (" << (service == newService) << ")\n";
			});

			// free resources
			guard g(lock);
			freeTaskStateInternal(id);
			return true;
		}

		// obtains the result of the corresponding task, or nothing if not yet available
		template<typename R>
		R getResult(com::rank_t owner, const TaskID& id) {

			// test whether this is the right one
			if (myRank != owner) {
				// query remote
				return network.getRemoteProcedure(owner,&TreetureStateService::getResult<R>)(owner,id).get();
			}

			detail::TreetureStateService<R>* service;

			{
				guard g(lock);

				// retrieve the current result state
				auto pos = states.find(id.getRootID());
				assert_true(pos != states.end()) << "Invalid state: targeted task not registered!";
				service = &static_cast<detail::TreetureStateService<R>&>(*pos->second);
			}

			// wait for result to arrive
			allscale::utils::fiber::suspend(service->getSyncEvent(id.getPath()));

			// collect result
			auto res = service->getResult(id.getPath());

			// if this result is present, this is the last thing we will here from this task
			guard g(lock);
			freeTaskStateInternal(id);

			// done
			return res;
		}

		// signals that any current or future result value for this task can be dropped
		void freeTaskState(const TaskID& id) {
			guard g(lock);
			freeTaskStateInternal(id);
		}

	private:

		// an internal version, not performing the locking
		void freeTaskStateInternal(const TaskID&) {
//			auto pos = states.find(id.getRootID());
//			assert_true(pos != states.end()) << "Unregistering treeture for unregistered task.";

			// TODO: finish this
		}

	public:


		// -- task-side interface --

		template<typename R>
		void registerTask(const TaskID& id) {
			guard g(lock);

			// obtain the corresponding state manager
			auto& state = states[id.getRootID()];

			// see whether it has been regisered before
			if (state) return;

			// if not, create a new state manager
			state = std::make_unique<detail::TreetureStateService<R>>(eventRegister);
		}

		template<typename R>
		void setDone(com::rank_t owner, const TaskID& id, R&& value) {

			// check whether this is the intended target
			if (myRank != owner) {
				// send result to remote
				network.getRemoteProcedure(owner,&TreetureStateService::setDone<R>)(owner,id,std::move(value));
				return;
			}

			guard g(lock);

			// lookup state
			auto pos = states.find(id.getRootID());
			assert_true(pos != states.end())
				<< "Invalid state: task either not registered or already fully consumed.";

			// check that types are used consistently
			assert_true(dynamic_cast<detail::TreetureStateService<R>*>(pos->second.get()));

			// update value
			detail::TreetureStateService<R>& service = static_cast<detail::TreetureStateService<R>&>(*pos->second);
			service.setResult(id.getPath(),std::move(value));
		}

		void setDone(com::rank_t owner, const TaskID& id) {

			// check whether this is the intended target
			if (myRank != owner) {
				// send result to remote
				void(TreetureStateService::* trg)(com::rank_t,const TaskID&) = &TreetureStateService::setDone;
				network.getRemoteProcedure(owner,trg)(owner,id);
				return;
			}

			guard g(lock);

			// lookup state
			auto pos = states.find(id.getRootID());
			assert_true(pos != states.end())
				<< "Invalid state: task either not registered or already fully consumed.";

			// check that types are used consistently
			assert_true(dynamic_cast<detail::TreetureStateService<void>*>(pos->second.get()));

			// update value
			detail::TreetureStateService<void>& service = static_cast<detail::TreetureStateService<void>&>(*pos->second);
			service.setDone(id.getPath());
		}

	};

	/**
	 * A treeture is a handle to an asynchronously, recursively computed value.
	 */
	template<typename R>
	class treeture {

		// the type of the optional value stored internally
		using value_opt_t = allscale::utils::optional<R>;

		// the ID of the task this treeture is associated to
		TaskID id;

		// the rank the associated treeture state is maintained by
		com::rank_t owner;

		// the value produced produced by the source
		mutable value_opt_t value;

		// determines whether this treeture is the owning instance of the associated state
		mutable bool owning;

		// a locally cached reference to the local treeture state service
		mutable TreetureStateService* service = nullptr;

	public:

		treeture() : owner(0), owning(false) {}

		treeture(R&& value) : owner(0), value(std::move(value)), owning(false) {}

		treeture(const treeture&) = delete;

		treeture(treeture&& other) : id(other.id), owner(other.owner), value(std::move(other.value)), owning(other.owning) {
			other.owning = false;
		}

	private:

		treeture(com::rank_t owner, const TaskID& id, value_opt_t&& value)
			: id(id), owner(owner), value(std::move(value)), owning(!bool(value)) {}

	public:

		// creates a treeture owning the result of the given task
		treeture(com::rank_t owner, const TaskID& id) : id(id), owner(owner), owning(true) {}

		~treeture() {
			release();
		}

		treeture& operator=(const treeture&) = delete;

		treeture& operator=(treeture&& other) {
			// test
			if (this == &other) return *this;

			// release this state
			release();

			// copy the id
			id = other.id;

			// copy the owner
			owner = other.owner;

			// take the ownership of the other
			owning = other.owning;

			// also the value, if there is one
			value = std::move(other.value);

			// release the other
			other.release();

			// done
			return *this;
		}

		bool valid() const {
			return owning || bool(value);
		}

		bool isDone() const {
			return bool(value);
		}

		R&& get_result() const {
			wait();
			assert_true(bool(value));
			return std::move(*value);
		}

		void wait() const {
			retrieveValue();
		}

		void store(allscale::utils::ArchiveWriter& out) const {

			// start with valid flag
			out.write<bool>(valid());

			// special case for invalid treetures
			if (!valid()) return;

			// store the value and lose ownership
			out.write(owner);
			out.write(id);
			out.write(value);

			// this is basically a move out ...
			owning = false;
		}

		static treeture<R> load(allscale::utils::ArchiveReader& in) {
			// test the valid flag
			bool valid = in.read<bool>();
			if (!valid) return {};

			// restore a valid treeture
			auto owner = in.read<com::rank_t>();
			auto id = in.read<TaskID>();
			auto value = in.read<allscale::utils::optional<R>>();
			return { owner, id, std::move(value) };
		}

	private:

		void retrieveValue() const {
			// if we have the value, there is nothing to do any more
			if (bool(value)) return;

			// only the owning instance may retrieve the value
			assert_true(owning);

			// retrieve the value
			value = getStateService().template getResult<R>(owner,id);

			// if we got it, we implicitly lost ownership
			if (bool(value)) owning = false;
		}

		void release() {
			if (!owning) return;
			getStateService().freeTaskState(id);
		}

		TreetureStateService& getStateService() const {
			if (!service) service = &TreetureStateService::getLocal();
			return *service;
		}

	};

	/**
	 * A treeture is a handle to an asynchronously, recursively computed value.
	 */
	template<>
	class treeture<void> {

		// the ID of the task this treeture is associated to
		allscale::utils::optional<TaskID> id;

		// the rank the associated treeture state is maintained by
		com::rank_t owner;

		// flag indicating whether the task associated to this treeture is done or not
		mutable bool done;

		// a locally cached reference to the local treeture state service
		mutable TreetureStateService* service = nullptr;

	public:

		treeture(bool done = false) : owner(0), done(done) {}

		treeture(const treeture&) = default;

		treeture(treeture&& other) = default;

		// creates a treeture owning the result of the given task
		treeture(com::rank_t owner, const TaskID& id, bool done = false) : id(id), owner(owner), done(done) {}

		treeture& operator=(const treeture&) = default;

		treeture& operator=(treeture&& other) = default;

		bool valid() const {
			return done || bool(id);
		}

		bool isDone() const {
			return done;
		}

		void wait() const {
			retrieveValue();
		}

		treeture<void> get_left_child() const {
			// TODO: no way yet to determine owner of child
			return *this;
//			if (!valid()) return *this;
//			return { (*id).getLeftChild(), done };
		}

		treeture<void> get_right_child() const {
			// TODO: no way yet to determine owner of child
			return *this;
//			if (!valid()) return *this;
//			return { (*id).getRightChild(), done };
		}

		void store(allscale::utils::ArchiveWriter& out) const {

			// start with valid flag
			out.write<bool>(valid());

			// special case for invalid treetures
			if (!valid()) return;

			// store the value and lose ownership
			out.write(*id);
			out.write(owner);
			out.write(done);
		}

		static treeture<void> load(allscale::utils::ArchiveReader& in) {
			// test the valid flag
			bool valid = in.read<bool>();
			if (!valid) return {};

			// restore a valid treeture
			auto id = in.read<TaskID>();
			auto owner = in.read<com::rank_t>();
			auto done = in.read<bool>();
			return { owner, id, done };
		}

	private:

		void retrieveValue() const {
			assert_true(valid());

			// if we have the value, there is nothing to do any more
			if (done) return;

			// retrieve the value
			getStateService().wait(owner,*id);
			done = true;
		}

		TreetureStateService& getStateService() const {
			if (!service) service = &TreetureStateService::getLocal();
			return *service;
		}

	};

	/**
	 * A connector for treetures producing a void treeture waiting for the completion of the given treetures.
	 */
	treeture<void> treeture_parallel(treeture<void>&& a, treeture<void>&& b);

	template<typename A, typename B, typename Comp>
	treeture<std::result_of_t<Comp(A,B)>> treeture_combine(treeture<A>&& a, treeture<B>&& b, const Comp& comp) {
		// wait eagerly
		auto va = a.get_result();
		auto vb = b.get_result();
		return comp(std::move(va),std::move(vb));
	}

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
