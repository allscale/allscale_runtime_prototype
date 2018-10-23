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

    # add additional node/policy combination tests
    foreach( num_nodes 2 3 4 )
        foreach( policy uniform balanced random )
            if (USE_MPI)
                add_test(NAME ${tgt}_N${num_nodes}_${policy} COMMAND ${CMAKE_COMMAND} -E env ART_SCHEDULER=${policy} ${MPIEXEC} ${MPIEXEC_NUMPROC_FLAG} ${num_nodes} $<TARGET_FILE:${tgt}>)
            else()
                add_test(NAME ${tgt}_N${num_nodes}_${policy} COMMAND ${CMAKE_COMMAND} -E env ART_SCHEDULER=${policy} ART_NUM_NODES=${num_nodes} $<TARGET_FILE:${tgt}>)
            endif()
        endforeach(policy)
    endforeach(num_nodes)

    # add additional node/policy combination tests for higher number of nodes
    foreach( num_nodes 5 15 16 )
        foreach( policy uniform )
            if (USE_MPI)
                add_test(NAME ${tgt}_N${num_nodes}_${policy} COMMAND ${CMAKE_COMMAND} -E env ART_SCHEDULER=${policy} ${MPIEXEC} ${MPIEXEC_NUMPROC_FLAG} ${num_nodes} $<TARGET_FILE:${tgt}>)
            else()
                add_test(NAME ${tgt}_N${num_nodes}_${policy} COMMAND ${CMAKE_COMMAND} -E env ART_SCHEDULER=${policy} ART_NUM_NODES=${num_nodes} $<TARGET_FILE:${tgt}>)
            endif()
        endforeach(policy)
    endforeach(num_nodes)

endforeach(test)

# copy amdados config file
configure_file(${PROJECT_SOURCE_DIR}/prototype/test/examples/pilot/amdados.conf ${CMAKE_CURRENT_BINARY_DIR}/amdados.conf COPYONLY)
