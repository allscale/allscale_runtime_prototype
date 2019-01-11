#include "allscale/utils/fibers.h"

namespace allscale {
namespace utils {
namespace fiber {

	__attribute__ ((noinline))
	Fiber*& getCurrentFiberInfo() {
		static thread_local Fiber* fiber = nullptr;
		asm(""); // for the compiler, this changes everything :)
		return fiber;
	}

	void setCurrentFiberInfo(Fiber* info) {
		auto& context = getCurrentFiberInfo();
		context = info;
	}

	void resetCurrentFiberInfo() {
		auto& context = getCurrentFiberInfo();
		context = nullptr;
	}


	Fiber* getCurrentFiber() {
		return getCurrentFiberInfo();
	}

} // end namespace fiber
} // end namespace utils
} // end namespace allscale
