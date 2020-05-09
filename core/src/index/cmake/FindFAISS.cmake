set(FAISS_STATIC_LIB_NAME ${CMAKE_STATIC_LIBRARY_PREFIX}faiss${CMAKE_STATIC_LIBRARY_SUFFIX})

# First, find via if specified FAISS_ROOT
if (FAISS_ROOT)
    find_library(FAISS_STATIC_LIB
            NAMES ${FAISS_STATIC_LIB_NAME}
            PATHS ${FAISS_ROOT}
            PATH_SUFFIXES "lib"
            NO_DEFAULT_PATH
            )
    find_path(FAISS_INCLUDE_DIR
            NAMES "faiss/Index.h"
            PATHS ${FAISS_ROOT}
            NO_DEFAULT_PATH
            PATH_SUFFIXES "include"
            )
endif ()

find_package_handle_standard_args(FAISS REQUIRED_VARS FAISS_STATIC_LIB FAISS_INCLUDE_DIR)

if (FAISS_FOUND)
    if (NOT TARGET faiss)
        add_library(faiss STATIC IMPORTED)

        set_target_properties(
                faiss
                PROPERTIES
                IMPORTED_LOCATION "${FAISS_STATIC_LIB}"
                INTERFACE_INCLUDE_DIRECTORIES "${FAISS_INCLUDE_DIR}"
        )

        if (FAISS_WITH_MKL)
            set_target_properties(
                    faiss
                    PROPERTIES
                    INTERFACE_LINK_LIBRARIES "${MKL_LIBS}")
        else ()
            set_target_properties(
                    faiss
                    PROPERTIES
                    INTERFACE_LINK_LIBRARIES ${OpenBLAS_LIBRARIES})
        endif ()
    endif ()
endif ()
