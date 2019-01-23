if(USE_MPI)
    find_package(MPI REQUIRED)

    if(MPI_CXX_FOUND) 
        # activate corresponding source paths
        add_definitions("-DENABLE_MPI")
        set(ART_DEFINITIONS "-DENABLE_MPI ${ART_DEFINITIONS}")
    endif()
endif()
