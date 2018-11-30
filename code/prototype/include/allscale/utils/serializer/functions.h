#pragma once

#include <type_traits>
#include <dlfcn.h>

#include "allscale/utils/assert.h"
#include "allscale/utils/serializer.h"
#include "allscale/utils/serializer/strings.h"

extern "C" {
	int _init();
}

namespace allscale {
namespace utils {

	// ----------------------------------------------------------------
	//			Utilities for Serializing Function Pointers
	// ----------------------------------------------------------------


	// Functions are serialized by transferring the name of their associated symbol.
	// This only works for pointer to non-static functions.


	namespace detail {

		/**
		 * A utility to compute the base address of the executable shared object.
		 */
		static inline void* getExecutableBase() {
			Dl_info info;
			if (!dladdr((void*)&_init,&info)) {
				assert_fail() << "Error obtaining executable shared object information.";
			}
			return info.dli_fbase;
		}

		/**
		 * Add support for serializing / de-serializing strings.
		 */
		template<typename FP>
		struct function_pointer_serializer {

			enum Mode {
				Offset = 0,
				Indirect = 1
			};

			#pragma GCC diagnostic push
			#pragma GCC diagnostic ignored "-Wstrict-aliasing"

			static void store(ArchiveWriter& writer, const FP& value) {
				static const void* base = getExecutableBase();

				// get name of symbol addressed by the pointer
				void* addr = *(void**)(&value);

				// get symbol information
				Dl_info info;
				if (!dladdr(addr,&info)) {
					assert_fail() << "Error obtaining symbol information: " << dlerror();
				}

				// if the name could not be resolved
				if (!info.dli_sname) {

					// make sure the un-resolvable symbol is part of the executable
					assert_eq(base,info.dli_fbase) << "Unable to handle function pointer not linked to symbol name and not part of executable.";

					// we serialize the function pointer directly (the function is not in any library but in the main)
					writer.write(Offset);
					writer.write<std::intptr_t>(std::intptr_t(addr) - std::intptr_t(base));
				} else {
					// we send the symbol name since it is in a library
					writer.write(Indirect);
					writer.write<std::string>(info.dli_sname);
				}
			}

			static FP load(ArchiveReader& reader) {
				static const void* base = getExecutableBase();

				// get the mode
				auto mode = reader.read<Mode>();

				// if direct, just use the pointer
				if (mode == Offset) {
					char data[sizeof(FP)] = {};
					reinterpret_cast<std::intptr_t&>(data) = std::intptr_t(base) + reader.read<std::intptr_t>();
					return reinterpret_cast<FP&>(*data);
				}

				// if indirect, use the symbol
				auto name = reader.read<std::string>();

				dlerror(); // clear old errors
				auto res = dlsym(RTLD_DEFAULT,name.c_str());

				// check for resolution issues
				if (auto issue = dlerror()) {
					assert_fail() << "Error resolving symbol name " << name << ": " << issue << "\n";
				}

				// convert to target type
				char data[sizeof(FP)] = {};
				reinterpret_cast<void*&>(data) = res;
				return reinterpret_cast<FP&>(*data);
			}

			#pragma GCC diagnostic pop

		};


	}


	template<typename R, typename ... Args>
	struct serializer<R(*)(Args...),void> : public detail::function_pointer_serializer<R(*)(Args...)> {};

	template<typename R, typename C, typename ... Args>
	struct serializer<R(C::*)(Args...),void> : public detail::function_pointer_serializer<R(C::*)(Args...)> {};

	template<typename R, typename C, typename ... Args>
	struct serializer<R(C::*)(Args...) const,void> : public detail::function_pointer_serializer<R(C::*)(Args...) const> {};

	template<typename R, typename C, typename ... Args>
	struct serializer<R(C::*)(Args...) volatile,void> : public detail::function_pointer_serializer<R(C::*)(Args...) volatile> {};

	template<typename R, typename C, typename ... Args>
	struct serializer<R(C::*)(Args...) const volatile,void> : public detail::function_pointer_serializer<R(C::*)(Args...) const volatile> {};



} // end namespace utils
} // end namespace allscale

