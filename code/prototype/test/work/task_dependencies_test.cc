#include <gtest/gtest.h>

#include "allscale/runtime/work/task_dependency.h"

namespace allscale {
namespace runtime {
namespace work {

	TEST(TaskDependency, Serialization) {

		// dependencies are copy and moveable
		EXPECT_TRUE(std::is_copy_constructible<TaskDependency>::value);
		EXPECT_TRUE(std::is_copy_assignable<TaskDependency>::value);
		EXPECT_TRUE(std::is_move_constructible<TaskDependency>::value);
		EXPECT_TRUE(std::is_move_assignable<TaskDependency>::value);

		// is default constructable
		EXPECT_TRUE(std::is_default_constructible<TaskDependency>::value);

		// dependencies are serializable
		EXPECT_TRUE(allscale::utils::is_serializable<TaskDependency>::value);

	}


	TEST(TaskDependencies, Serialization) {

		// dependencies are copy and moveable
		EXPECT_TRUE(std::is_copy_constructible<TaskDependencies>::value);
		EXPECT_TRUE(std::is_copy_assignable<TaskDependencies>::value);
		EXPECT_TRUE(std::is_move_constructible<TaskDependencies>::value);
		EXPECT_TRUE(std::is_move_assignable<TaskDependencies>::value);

		// is default constructable
		EXPECT_TRUE(std::is_default_constructible<TaskDependencies>::value);

		// dependencies are serializable
		EXPECT_TRUE(allscale::utils::is_serializable<TaskDependencies>::value);

	}


} // end of namespace work
} // end of namespace runtime
} // end of namespace allscale
