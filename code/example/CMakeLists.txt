add_module_library(example)

#target_link_libraries(example ${Boost_LIBRARIES})
#target_include_directories(example SYSTEM PUBLIC ${Boost_INCLUDE_DIR})

# AllScale API
target_include_directories(example PUBLIC ${PROJECT_SOURCE_DIR}/../api/code/api/include)
target_include_directories(example PUBLIC ${PROJECT_SOURCE_DIR}/../api/code/utils/include)

# attach threads dependency (needed by API)
target_link_libraries(example Threads::Threads)


glob_executables(example_exes src)
foreach(exe ${example_exes})
	add_module_executable(example ${exe})
endforeach(exe)

glob_tests(example_tests test)
foreach(test ${example_tests})
	add_module_unittest(example ${test})
endforeach(test)
