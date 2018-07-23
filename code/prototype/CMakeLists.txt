add_module_library(prototype)

#target_link_libraries(prototype ${Boost_LIBRARIES})
#target_include_directories(prototype SYSTEM PUBLIC ${Boost_INCLUDE_DIR})

# AllScale API
target_include_directories(prototype PUBLIC ${PROJECT_SOURCE_DIR}/../api/code/api/include)
target_include_directories(prototype PUBLIC ${PROJECT_SOURCE_DIR}/../api/code/utils/include)

# attach threads dependency (needed by API)
target_link_libraries(prototype Threads::Threads)


glob_executables(prototype_exes src)
foreach(exe ${prototype_exes})
	add_module_executable(prototype ${exe})
endforeach(exe)

glob_tests(prototype_tests test)
foreach(test ${prototype_tests})
	add_module_unittest(prototype ${test})
endforeach(test)
