cmake_minimum_required(VERSION 3.5)
project(allscale_runtime_prototype LANGUAGES C CXX)

# -- Module Path
set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/../cmake ${CMAKE_MODULE_PATH})

# -- Prefix Path
set(THIRD_PARTY_DIR ${CMAKE_BINARY_DIR}/third_party CACHE STRING "Third Party Library Directory")
file(GLOB prefix_paths ${THIRD_PARTY_DIR}/*)
list(APPEND CMAKE_PREFIX_PATH ${prefix_paths})

# -- Project Settings
include(build_settings)
include(coverage)
include(doxygen)

option(USE_LINUX_CPUFREQ "Use a Linux cpufreq-based implementation of DVFS" OFF)
IF(USE_LINUX_CPUFREQ)
    ADD_DEFINITIONS(-DUSE_LINUX_CPUFREQ)
ENDIF(USE_LINUX_CPUFREQ)

# -- Dependency Settings
include(dependencies/pthread)
include(dependencies/googletest)
#include(dependencies/boost)
include(dependencies/valgrind)
include(dependencies/mpi)

# -- CMake Modules
include(add_module)
include(file_globs)
include(msvc_source_group)
include(nproc)

# -- Project Modules
add_subdirectory(prototype)

# -- Config Files
configure_file(../resources/art_flags.mk.in art_flags.mk @ONLY)
