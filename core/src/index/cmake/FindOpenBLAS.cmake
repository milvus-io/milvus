
if (OpenBLAS_FOUND) # the git version propose a OpenBLASConfig.cmake
    message(STATUS "OpenBLASConfig found")
    set(OpenBLAS_INCLUDE_DIR ${OpenBLAS_INCLUDE_DIRS})
else()
    message("OpenBLASConfig not found")
    unset(OpenBLAS_DIR CACHE)
    set(OpenBLAS_INCLUDE_SEARCH_PATHS
            /usr/local/openblas/include
            /usr/include
            /usr/include/openblas
            /usr/include/openblas-base
            /usr/local/include
            /usr/local/include/openblas
            /usr/local/include/openblas-base
            /opt/OpenBLAS/include
            /usr/local/opt/openblas/include
            $ENV{OpenBLAS_HOME}
            $ENV{OpenBLAS_HOME}/include
            )

    set(OpenBLAS_LIB_SEARCH_PATHS
            /usr/local/openblas/lib
            /lib/
            /lib/openblas-base
            /lib64/
            /usr/lib
            /usr/lib/openblas-base
            /usr/lib64
            /usr/local/lib
            /usr/local/lib64
            /usr/local/opt/openblas/lib
            /opt/OpenBLAS/lib
            $ENV{OpenBLAS}
            $ENV{OpenBLAS}/lib
            $ENV{OpenBLAS_HOME}
            $ENV{OpenBLAS_HOME}/lib
            )
    set(DEFAULT_OpenBLAS_LIB_PATH
            /usr/local/openblas/lib
            ${OPENBLAS_PREFIX}/lib)

    message("DEFAULT_OpenBLAS_LIB_PATH: ${DEFAULT_OpenBLAS_LIB_PATH}")
    find_path(OpenBLAS_INCLUDE_DIR NAMES openblas_config.h lapacke.h PATHS ${OpenBLAS_INCLUDE_SEARCH_PATHS})
    find_library(OpenBLAS_LIB NAMES openblas PATHS ${DEFAULT_OpenBLAS_LIB_PATH} NO_DEFAULT_PATH)
    find_library(OpenBLAS_LIB NAMES openblas PATHS ${OpenBLAS_LIB_SEARCH_PATHS})
    # mostly for debian
    find_library(Lapacke_LIB NAMES lapacke PATHS ${DEFAULT_OpenBLAS_LIB_PATH} NO_DEFAULT_PATH)
    find_library(Lapacke_LIB NAMES lapacke PATHS ${OpenBLAS_LIB_SEARCH_PATHS})

    set(OpenBLAS_FOUND ON)

    #    Check include files
    if(NOT OpenBLAS_INCLUDE_DIR)
        set(OpenBLAS_FOUND OFF)
        message(STATUS "Could not find OpenBLAS include. Turning OpenBLAS_FOUND off")
    else()
        message(STATUS "find OpenBLAS include:${OpenBLAS_INCLUDE_DIR} ")
    endif()

    #    Check libraries
    if(NOT OpenBLAS_LIB)
        set(OpenBLAS_FOUND OFF)
        message(STATUS "Could not find OpenBLAS lib. Turning OpenBLAS_FOUND off")
    else()
        message(STATUS "find OpenBLAS lib:${OpenBLAS_LIB} ")
    endif()

    if (OpenBLAS_FOUND)
        set(OpenBLAS_LIBRARIES ${OpenBLAS_LIB})
        STRING(REGEX REPLACE "/libopenblas.so" "" OpenBLAS_LIB_DIR ${OpenBLAS_LIBRARIES})
        message(STATUS "find OpenBLAS libraries:${OpenBLAS_LIBRARIES} ")
        if (Lapacke_LIB)
            set(OpenBLAS_LIBRARIES ${OpenBLAS_LIBRARIES} ${Lapacke_LIB})
        endif()
        if (NOT OpenBLAS_FIND_QUIETLY)
            message(STATUS "Found OpenBLAS libraries: ${OpenBLAS_LIBRARIES}")
            message(STATUS "Found OpenBLAS include: ${OpenBLAS_INCLUDE_DIR}")
        endif()
    else()
        if (OpenBLAS_FIND_REQUIRED)
            message(FATAL_ERROR "Could not find OpenBLAS")
        endif()
    endif()
endif()

mark_as_advanced(
        OpenBLAS_INCLUDE_DIR
        OpenBLAS_LIBRARIES
        OpenBLAS_LIB_DIR
)
