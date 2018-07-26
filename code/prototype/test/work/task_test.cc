#include <gtest/gtest.h>

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

	TEST(TaskReference,Serialization) {

		// create a task
		TaskPtr task = make_lambda_task(0,[]{});
		Task& t = *task;

		EXPECT_TRUE(bool(task));

		// create a task reference
		TaskReference ref = std::move(task);
		EXPECT_FALSE(bool(task));

		// serialize task
		auto a = allscale::utils::serialize(ref);

		// now the task should still be owned by the original ref

		// deserialize task
		TaskReference trg = allscale::utils::deserialize<TaskReference>(a);

		// extract task
		auto trgTask = std::move(trg).toTask();

		EXPECT_TRUE(bool(trgTask));
		EXPECT_EQ(&t,trgTask.get());

		// run task (before it gets destructed)
		t.process();
	}

} // end of namespace com
} // end of namespace runtime
} // end of namespace allscale
