#include <gtest/gtest.h>

#include "allscale/utils/serializer/functions.h"

#include "allscale/runtime/work/task_id.h"

namespace allscale {
namespace utils {

	struct A {};

	TEST(Serializer, Functions) {

		// functions are not serializable
		EXPECT_FALSE(is_serializable<void()>::value);
		EXPECT_FALSE(is_serializable<int(int)>::value);

		// but function pointers are
		EXPECT_TRUE(is_serializable<void(*)()>::value);
		EXPECT_TRUE(is_serializable<int(*)(int)>::value);
		EXPECT_TRUE(is_serializable<int(A::*)(int)>::value);
		EXPECT_TRUE(is_serializable<int(A::*)(int) const>::value);
		EXPECT_TRUE(is_serializable<int(A::*)(int) volatile>::value);
		EXPECT_TRUE(is_serializable<int(A::*)(int) const volatile>::value);

		// but they are not trivially serialicable
		EXPECT_FALSE(is_trivially_serializable<void(*)()>::value);
		EXPECT_FALSE(is_trivially_serializable<int(*)(int)>::value);
		EXPECT_FALSE(is_trivially_serializable<int(A::*)(int)>::value);
		EXPECT_FALSE(is_trivially_serializable<int(A::*)(int) const>::value);
		EXPECT_FALSE(is_trivially_serializable<int(A::*)(int) volatile>::value);
		EXPECT_FALSE(is_trivially_serializable<int(A::*)(int) const volatile>::value);

	}


	int f(int x) {
		return x + 1;
	}

	int g(int x) {
		return x * 2;
	}


	TEST(Serialization, FunctionPointer) {

		using fun_t = int(*)(int);

		fun_t p = f;

		// test the pointer
		EXPECT_EQ(4,p(3));
		p = g;
		EXPECT_EQ(6,p(3));


		// serialize the pointer
		p = f;
		auto a = serialize(p);
		auto r = deserialize<fun_t>(a);

		EXPECT_EQ(r,p);


		// serialize the other pointer
		p = g;
		a = serialize(p);
		r = deserialize<fun_t>(a);

		EXPECT_EQ(r,p);

	}


	TEST(Serialization, FunctionPointerFromLibrary) {

		using fun_t = runtime::work::TaskID(*)();

		fun_t p = runtime::work::getFreshId;

		// serialize the pointer
		auto a = serialize(p);
		auto r = deserialize<fun_t>(a);

		EXPECT_EQ(r,p);

	}


	struct B {
		int f(int x) {
			return x + 3;
		}
	};

	TEST(Serialization, MemberFunctionPtr) {

		using fun_t = int(B::*)(int);

		fun_t p = &B::f;

		// serialize the pointer
		auto a = serialize(p);
		auto r = deserialize<fun_t>(a);

		EXPECT_EQ(r,p);
		EXPECT_EQ(6,(B().*p)(3));

	}

	TEST(Serialization, MemberFunctionPtrFromLibrary) {
		using namespace allscale::runtime::work;

		using fun_t = std::uint8_t(TaskID::*)() const;

		fun_t p = &TaskID::getDepth;

		// serialize the pointer
		auto a = serialize(p);
		auto r = deserialize<fun_t>(a);

		EXPECT_EQ(r,p);
		EXPECT_EQ(0,(getFreshId().*p)());

	}
} // end namespace utils
} // end namespace allscale
