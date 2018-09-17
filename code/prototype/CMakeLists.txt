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
#    add_test(NAME ${tgt}_random  COMMAND ${CMAKE_COMMAND} -E env ART_SCHEDULER=random  $<TARGET_FILE:${tgt}>)
    add_test(NAME ${tgt}_dynamic COMMAND ${CMAKE_COMMAND} -E env ART_SCHEDULER=dynamic $<TARGET_FILE:${tgt}>)

    add_test(NAME ${tgt}_N1_uniform COMMAND ${CMAKE_COMMAND} -E env ART_NUM_NODES=1 ART_SCHEDULER=uniform $<TARGET_FILE:${tgt}>)
#    add_test(NAME ${tgt}_N1_random  COMMAND ${CMAKE_COMMAND} -E env ART_NUM_NODES=1 ART_SCHEDULER=random  $<TARGET_FILE:${tgt}>)
    add_test(NAME ${tgt}_N1_dynamic COMMAND ${CMAKE_COMMAND} -E env ART_NUM_NODES=1 ART_SCHEDULER=dynamic $<TARGET_FILE:${tgt}>)

    add_test(NAME ${tgt}_N2_uniform COMMAND ${CMAKE_COMMAND} -E env ART_NUM_NODES=2 ART_SCHEDULER=uniform $<TARGET_FILE:${tgt}>)
#    add_test(NAME ${tgt}_N2_random  COMMAND ${CMAKE_COMMAND} -E env ART_NUM_NODES=2 ART_SCHEDULER=random  $<TARGET_FILE:${tgt}>)
    add_test(NAME ${tgt}_N2_dynamic COMMAND ${CMAKE_COMMAND} -E env ART_NUM_NODES=2 ART_SCHEDULER=dynamic $<TARGET_FILE:${tgt}>)

    add_test(NAME ${tgt}_N3_uniform COMMAND ${CMAKE_COMMAND} -E env ART_NUM_NODES=3 ART_SCHEDULER=uniform $<TARGET_FILE:${tgt}>)
#    add_test(NAME ${tgt}_N3_random  COMMAND ${CMAKE_COMMAND} -E env ART_NUM_NODES=3 ART_SCHEDULER=random  $<TARGET_FILE:${tgt}>)
    add_test(NAME ${tgt}_N3_dynamic COMMAND ${CMAKE_COMMAND} -E env ART_NUM_NODES=3 ART_SCHEDULER=dynamic $<TARGET_FILE:${tgt}>)

    add_test(NAME ${tgt}_N5_uniform COMMAND ${CMAKE_COMMAND} -E env ART_NUM_NODES=5 ART_SCHEDULER=uniform $<TARGET_FILE:${tgt}>)
#    add_test(NAME ${tgt}_N5_random  COMMAND ${CMAKE_COMMAND} -E env ART_NUM_NODES=5 ART_SCHEDULER=random  $<TARGET_FILE:${tgt}>)
    add_test(NAME ${tgt}_N5_dynamic COMMAND ${CMAKE_COMMAND} -E env ART_NUM_NODES=5 ART_SCHEDULER=dynamic $<TARGET_FILE:${tgt}>)

    add_test(NAME ${tgt}_N15_uniform COMMAND ${CMAKE_COMMAND} -E env ART_NUM_NODES=15 ART_SCHEDULER=uniform $<TARGET_FILE:${tgt}>)
#    add_test(NAME ${tgt}_N15_random  COMMAND ${CMAKE_COMMAND} -E env ART_NUM_NODES=15 ART_SCHEDULER=random  $<TARGET_FILE:${tgt}>)
    add_test(NAME ${tgt}_N15_dynamic COMMAND ${CMAKE_COMMAND} -E env ART_NUM_NODES=15 ART_SCHEDULER=dynamic $<TARGET_FILE:${tgt}>)

    add_test(NAME ${tgt}_N16_uniform COMMAND ${CMAKE_COMMAND} -E env ART_NUM_NODES=16 ART_SCHEDULER=uniform $<TARGET_FILE:${tgt}>)
#    add_test(NAME ${tgt}_N16_random  COMMAND ${CMAKE_COMMAND} -E env ART_NUM_NODES=16 ART_SCHEDULER=random  $<TARGET_FILE:${tgt}>)
    add_test(NAME ${tgt}_N16_dynamic COMMAND ${CMAKE_COMMAND} -E env ART_NUM_NODES=16 ART_SCHEDULER=dynamic $<TARGET_FILE:${tgt}>)

endforeach(test)

# copy amdados config file
configure_file(${PROJECT_SOURCE_DIR}/prototype/test/examples/pilot/amdados.conf ${CMAKE_CURRENT_BINARY_DIR}/amdados.conf COPYONLY)
