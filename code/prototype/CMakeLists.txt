add_module_library(prototype)

#target_link_libraries(prototype ${Boost_LIBRARIES})
#target_include_directories(prototype SYSTEM PUBLIC ${Boost_INCLUDE_DIR})

# AllScale API
target_include_directories(prototype PUBLIC ${PROJECT_SOURCE_DIR}/../api/code/api/include)
target_include_directories(prototype PUBLIC ${PROJECT_SOURCE_DIR}/../api/code/utils/include)

# attach threads dependency (needed by API)
target_link_libraries(prototype Threads::Threads)

# add dependendeny to dl library
target_link_libraries(prototype "-ldl")

# MPI
target_include_directories(prototype PUBLIC ${MPI_CXX_INCLUDE_PATH})
target_compile_options(prototype PUBLIC ${MPI_CXX_COMPILE_FLAGS})
target_link_libraries(prototype ${MPI_CXX_LIBRARIES})


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
    add_test(NAME ${tgt}_uniform COMMAND ${CMAKE_COMMAND} -E env ART_SCHEDULER=uniform $<TARGET_FILE:${tgt}>)
    add_test(NAME ${tgt}_random  COMMAND ${CMAKE_COMMAND} -E env ART_SCHEDULER=random  $<TARGET_FILE:${tgt}>)
    add_test(NAME ${tgt}_dynamic COMMAND ${CMAKE_COMMAND} -E env ART_SCHEDULER=dynamic $<TARGET_FILE:${tgt}>)

    # add additional node/policy combination tests
    foreach( num_nodes 2 3 4 )
        foreach( policy uniform dynamic random )
            add_test(NAME ${tgt}_N${num_nodes}_${policy} COMMAND ${CMAKE_COMMAND} -E env ART_NUM_NODES=${num_nodes} ART_SCHEDULER=${policy} $<TARGET_FILE:${tgt}>)
        endforeach(policy)
    endforeach(num_nodes)

    # add additional node/policy combination tests for higher number of nodes
    foreach( num_nodes 5 15 16 )
        foreach( policy uniform dynamic )
            add_test(NAME ${tgt}_N${num_nodes}_${policy} COMMAND ${CMAKE_COMMAND} -E env ART_NUM_NODES=${num_nodes} ART_SCHEDULER=${policy} $<TARGET_FILE:${tgt}>)
        endforeach(policy)
    endforeach(num_nodes)

endforeach(test)

# copy amdados config file
configure_file(${PROJECT_SOURCE_DIR}/prototype/test/examples/pilot/amdados.conf ${CMAKE_CURRENT_BINARY_DIR}/amdados.conf COPYONLY)
