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

# add integration tests
glob_executables(prototype_integration_tests test)
foreach(test ${prototype_integration_tests})
	file_name_parts(${test} test test)

    set(tgt "it_${test_subdir_}${test_name}")
    add_executable(${tgt} ${test})
    target_link_libraries(${tgt} prototype)
    add_test(NAME ${tgt} COMMAND $<TARGET_FILE:${tgt}>)
endforeach(test)
