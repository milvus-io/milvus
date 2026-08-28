# Knowhere v2.6.18 uses find_package(BLAS), while OpenBLAS is provided by
# Conan.  Preserve the conventional FindBLAS variables and target by mapping
# them to Conan's OpenBLAS target.
find_package(OpenBLAS REQUIRED)

set(BLAS_FOUND TRUE)
set(BLAS_LIBRARIES OpenBLAS::OpenBLAS)

if(NOT TARGET BLAS::BLAS)
    add_library(BLAS::BLAS INTERFACE IMPORTED)
    set_target_properties(BLAS::BLAS PROPERTIES
        INTERFACE_LINK_LIBRARIES OpenBLAS::OpenBLAS)
endif()
