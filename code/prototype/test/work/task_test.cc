#include <gtest/gtest.h>

#include "allscale/runtime/com/network.h"
#include "allscale/runtime/work/task.h"

namespace allscale {
namespace runtime {
namespace work {

	TEST(Task, Serialization) {

		// tasks are not serializable
		EXPECT_FALSE(allscale::utils::is_serializable<Task>::value);

		// task ptr are also not serializable
		EXPECT_FALSE(allscale::utils::is_serializable<TaskPtr>::value);

		// but task references are
		EXPECT_TRUE(allscale::utils::is_serializable<TaskReference>::value);

	}

	TEST(DISABLED_TaskReference,Serialization) {

		// create a task
		TaskPtr task = make_lambda_task(0,[]{});
		Task& t = *task;

		EXPECT_TRUE(bool(task));

		// create a task reference
		TaskReference ref = std::move(task);
		EXPECT_FALSE(bool(task));

		// serialize task
		auto a = allscale::utils::serialize(ref);

		// deserialize task
		TaskReference trg = allscale::utils::deserialize<TaskReference>(a);

		// extract task
		auto trgTask = std::move(trg).toTask();

		EXPECT_TRUE(bool(trgTask));
		EXPECT_EQ(&t,trgTask.get());

		// run task (before it gets destructed)
		t.process();
	}

	template<typename Op>
	void runInNode(const Op& op) {

		// get a network, not important how large
		auto network = com::Network::create();
		assert_true(network);

		auto& net = *network;
		installFiberContextService(net);
		installTreetureStateService(net);

		net.runOn(0,[&](com::Node& node){
			auto& ctxt = node.getService<FiberContextService>().getContext();
			std::atomic<bool> done(false);
			ctxt.start([&]{
				op();
				done = true;
			});
			while (!done) ctxt.yield();
		});
	}

	TEST(Task, Execution) {

		runInNode([]{

			// let's create a task
			auto task = make_lambda_task(getFreshId(),[]{ return 1; });

			// retrieve treeture
			auto t = task->getTreeture();

			EXPECT_FALSE(t.isDone());

			task->process();

			EXPECT_EQ(1,t.get_result());
			EXPECT_TRUE(t.isDone());

		});

	}

	TEST(Task, ExecutionVoid) {

		runInNode([]{

			int x = 0;

			// let's create a task
			auto task = make_lambda_task(getFreshId(),[&]{ x = 1; });

			// retrieve treeture
			auto t = task->getTreeture();

			EXPECT_FALSE(t.isDone());

			task->process();

			t.wait();
			EXPECT_TRUE(t.isDone());

		});

	}

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
