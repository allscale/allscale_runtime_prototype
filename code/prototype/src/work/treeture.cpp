
#include "allscale/runtime/work/treeture.h"

#include "allscale/runtime/com/network.h"
#include "allscale/runtime/work/scheduler.h"

namespace allscale {
namespace runtime {
namespace work {

	void installTreetureStateService(com::Network& net) {
		net.installServiceOnNodes<TreetureStateService>();
	}

	treeture<void> treeture_parallel(treeture<void>&& a, treeture<void>&& b) {
		// wait eagerly
		a.wait();
		b.wait();
		return treeture<void>(true);
	}

	bool TreetureStateService::wait(const TaskRef ref) {

		// get owner
		auto owner = ref.getOwner();

		// if owner is unknown, locate it using the scheduler
		if (owner == TaskRef::UNKNOWN_OWNER) {
			owner = estimateLocationOf(ref.getTaskID());
			//std::cout << "Resolved location of " << ref << " to " << owner << "\n";
		}

		// test whether this is the right one
		if (myRank != owner) {
			// query remote
			return network.getRemoteProcedure(owner,&TreetureStateService::wait)(ref).get();
		}

		auto& id = ref.getTaskID();
		allscale::utils::fiber::EventId syncEvent;
		{
			guard g(lock);
			auto pos = states.find(id.getRootID());

			// while this task is not registered, wait and repeat lookup
			while(pos == states.end()) {
				taskRegisterConVar.wait(lock);
				pos = states.find(id.getRootID());
			}

			assert_true(pos != states.end());
			syncEvent = pos->second->getSyncEvent(id.getPath());
		}

		// see that processed fiber is on same event register as this treeture state service
		assert_eq(
			&allscale::utils::fiber::getCurrentFiber()->ctxt.getEventRegister(),
			&eventRegister
		);

		// suspend fiber
		allscale::utils::fiber::suspend(syncEvent, allscale::utils::fiber::Priority::HIGH);

		// make sure task is done now
		assert_decl({
			guard g(lock);
			auto pos = states.find(id.getRootID());
			assert_false(pos == states.end());
			auto newSyncEvent = pos->second->getSyncEvent(id.getPath());
			assert_eq(newSyncEvent, allscale::utils::fiber::EVENT_IGNORE)
				<< "Task: " << id << "\n"
				<< "Events: " << syncEvent << " vs " << newSyncEvent << "\n";
		});

		// free resources
		guard g(lock);
		freeTaskStateInternal(id);
		return true;
	}

	allscale::utils::fiber::Future<bool> TreetureStateService::getWaitHandle(const TaskRef ref) {
		allscale::utils::fiber::Future<bool> res;
		fiberContext.start([&]{
			allscale::utils::fiber::Promise<bool> promise(fiberContext);
			res = promise.get_future();
			promise.set_value(wait(ref)); // < this may block
		});
		return std::move(res);
	}

	void TreetureStateService::taskFinished(const TaskID& id) {
		assert_true(id.getPath().isRoot());

		// register task if necessary
		registerTask<void>(id);

		// signal task completion
		guard g(lock);
		auto pos = states.find(id.getRootID());
		assert_true(pos != states.end());
		pos->second->taskFinished();

	}

} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale
