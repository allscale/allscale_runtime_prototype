
if(USE_MPI)

    find_package(MPI REQUIRED)

    if(MPI_CXX_FOUND) 

        # activate corresponding source paths
        add_definitions("-DENABLE_MPI")

    endif()

endif()
