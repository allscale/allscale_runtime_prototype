#include <gtest/gtest.h>

#include "allscale_runtime_prototype/example/answer.h"

using namespace allscale_runtime_prototype::example;

TEST(AnswerTest, Basic) {
	ASSERT_EQ(42, answer());
}
