# OpenBLAS provides the LAPACK symbols required by Knowhere's Faiss build.
# Keep the legacy FindLAPACK interface while linking Conan's OpenBLAS target.
find_package(OpenBLAS REQUIRED)

set(LAPACK_FOUND TRUE)
set(LAPACK_LIBRARIES OpenBLAS::OpenBLAS)

if(NOT TARGET LAPACK::LAPACK)
    add_library(LAPACK::LAPACK INTERFACE IMPORTED)
    set_target_properties(LAPACK::LAPACK PROPERTIES
        INTERFACE_LINK_LIBRARIES OpenBLAS::OpenBLAS)
endif()
