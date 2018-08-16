#pragma once

#include <type_traits>
#include <dlfcn.h>

#include "allscale/utils/assert.h"
#include "allscale/utils/serializer.h"
#include "allscale/utils/serializer/strings.h"

namespace allscale {
namespace utils {

	// ----------------------------------------------------------------
	//			Utilities for Serializing Function Pointers
	// ----------------------------------------------------------------


	// Functions are serialized by transferring the name of their associated symbol.
	// This only works for pointer to non-static functions.


	namespace detail {

		/**
		 * Add support for serializing / de-serializing strings.
		 */
		template<typename FP>
		struct function_pointer_serializer {

			enum Mode {
				Direct = 0,
				Indirect = 1
			};

			#pragma GCC diagnostic push
			#pragma GCC diagnostic ignored "-Wpmf-conversions"
			#pragma GCC diagnostic ignored "-Wstrict-aliasing"

			static void store(ArchiveWriter& writer, const FP& value) {

				// get name of symbol addressed by the pointer
				void* addr = reinterpret_cast<void*>(value);

				// get symbol information
				Dl_info info;
				if (!dladdr(addr,&info)) {
					assert_fail() << "Error obtaining symbol information: " << dlerror();
				}

				// if the name could not be resolved
				if (!info.dli_sname) {
					// we serialize the function pointer directly (the function is not in any library but in the main)
					writer.write(Direct);
					writer.write<std::intptr_t>(std::intptr_t(addr));
				} else {
					// we send the symbol name since it is in a library
					writer.write(Indirect);
					writer.write<std::string>(info.dli_sname);
				}
			}

			static FP load(ArchiveReader& reader) {
				// get the mode
				auto mode = reader.read<Mode>();

				// if direct, just use the pointer
				if (mode == Direct) {
					char data[sizeof(FP)] = {};
					reinterpret_cast<std::intptr_t&>(data) = reader.read<std::intptr_t>();
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

