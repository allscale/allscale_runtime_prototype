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
#include <memory>
#include <mutex>

#include "allscale/utils/assert.h"
#include "allscale/utils/optional.h"
#include "allscale/utils/serializer.h"
#include "allscale/utils/serializer/optionals.h"

#include "allscale/runtime/com/node.h"
#include "allscale/runtime/com/network.h"
#include "allscale/runtime/work/task_id.h"

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

			using opt_result_t = allscale::utils::optional<R>;

			// we support a decomposition of up to level
			std::array<opt_result_t,(1<<MAX_TASK_LEVELS)> results;

		public:

			void setResult(const TaskPath& path, R&& value) {
				assert_lt(toPosition(path),(1<<MAX_TASK_LEVELS));
				assert_false(bool(results[toPosition(path)]));
				results[toPosition(path)] = std::move(value);
			}

			opt_result_t getResult(const TaskPath& path) {
				assert_lt(toPosition(path),(1<<MAX_TASK_LEVELS));
				assert_lt(path.getLength(),MAX_TASK_LEVELS) << "Deeper levels not supported!";
				return std::move(results[toPosition(path)]);
			}

		};

		template<>
		class TreetureStateService<void> : public TreetureStateServiceBase {

			std::bitset<(1<<MAX_TASK_LEVELS)> states;

		public:

			void setDone(const TaskPath& path) {
				assert_lt(toPosition(path),(1<<MAX_TASK_LEVELS));
				assert_false(isDone(path));
				states.set(toPosition(path));
			}

			bool isDone(const TaskPath& path) const {
				assert_lt(toPosition(path),(1<<MAX_TASK_LEVELS));
				assert_lt(path.getLength(),MAX_TASK_LEVELS) << "Deeper levels not supported!";
				return states.test(toPosition(path));
			}

		};

	} // end namespace detail


	class TreetureStateService {

		// a lock to sync concurrent accesses
		mutable std::mutex lock;

		// maintains the list of treeture states of tasks managed by this service
		std::map<std::uint64_t,std::unique_ptr<detail::TreetureStateServiceBase>> states;

		// the guard type to sync concurrent accesses
		using guard = std::lock_guard<std::mutex>;

		// the network being part of
		com::Network& network;

		// the rank of the node this service is running on
		com::rank_t myRank;

	public:

		// the service constructor
		TreetureStateService(com::Node& node)
			: network(com::Network::getNetwork()), myRank(node.getRank()) {}

		static TreetureStateService& getLocal() {
			return com::Node::getLocalService<TreetureStateService>();
		}

		// -- treeture side interface --

		// tests non-blocking whether the referenced task is done
		bool isDone(com::rank_t owner, const TaskID& id) {

			// test whether this is the right one
			if (myRank != owner) {
				// query remote
				return network.getRemoteProcedure(owner,&TreetureStateService::isDone)(owner,id);
			}

			guard g(lock);
			auto pos = states.find(id.getRootID());
			if (pos == states.end()) return true;

			// check consistent accesses
			assert_true(dynamic_cast<detail::TreetureStateService<void>*>(pos->second.get()));
			bool res = static_cast<detail::TreetureStateService<void>&>(*pos->second).isDone(id.getPath());
			if (res) freeTaskStateInternal(id);
			return res;
		}

		// obtains the result of the corresponding task, or nothing if not yet available
		template<typename R>
		allscale::utils::optional<R> getResult(com::rank_t owner, const TaskID& id) {

			// test whether this is the right one
			if (myRank != owner) {
				// query remote
				return network.getRemoteProcedure(owner,&TreetureStateService::getResult<R>)(owner,id);
			}

			guard g(lock);

			// retrieve the current result state
			auto pos = states.find(id.getRootID());
			assert_true(pos != states.end()) << "Invalid state: targeted task not registered!";
			auto res = static_cast<detail::TreetureStateService<R>&>(*pos->second).getResult(id.getPath());

			// if this result is present, this is the last thing we will here from this task
			if (bool(res)) freeTaskStateInternal(id);

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
			state = std::make_unique<detail::TreetureStateService<R>>();
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
			retrieveValue();
			return bool(value);
		}

		R&& get_result() const {
			wait();
			assert_true(bool(value));
			return std::move(*value);
		}

		void wait() const {
			// TODO: consider an exponential back-off
			while(!isDone()) {
				yield();
			}
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
			return TreetureStateService::getLocal();
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
			retrieveValue();
			return done;
		}

		void wait() const {
			// TODO: consider an exponential back-off
			while(!isDone()) {
				yield();
			}
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
			done = getStateService().isDone(owner,*id);
		}

		TreetureStateService& getStateService() const {
			return TreetureStateService::getLocal();
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
