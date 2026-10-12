# libsais — linear-time suffix array / BWT construction (SA-IS).
# Ilya Grebnov, Apache-2.0 (see LICENSE in this directory). Vendored for the
# FM-index scalar index (internal/core/src/index/fmindex). Built single-threaded;
# LIBSAIS_OPENMP is intentionally NOT defined here (per-segment builds run
# single-threaded and parallelize across segments).
add_library(libsais STATIC
    libsais.c
    libsais64.c)
target_include_directories(libsais PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
# Linked into the milvus_core shared library, so the static objects need PIC.
set_target_properties(libsais PROPERTIES POSITION_INDEPENDENT_CODE ON)
