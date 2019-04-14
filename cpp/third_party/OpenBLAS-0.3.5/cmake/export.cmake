
#Only generate .def for dll on MSVC
if(MSVC)

set_source_files_properties(${OpenBLAS_DEF_FILE} PROPERTIES GENERATED 1)

if (NOT DEFINED ARCH)
  set(ARCH_IN "x86_64")
else()
  set(ARCH_IN ${ARCH})
endif()

if (${CORE} STREQUAL "generic")
  set(ARCH_IN "GENERIC")
endif ()

if (NOT DEFINED EXPRECISION)
  set(EXPRECISION_IN 0)
else()
  set(EXPRECISION_IN ${EXPRECISION})
endif()

if (NOT DEFINED NO_CBLAS)
  set(NO_CBLAS_IN 0)
else()
  set(NO_CBLAS_IN ${NO_CBLAS})
endif()

if (NOT DEFINED NO_LAPACK)
  set(NO_LAPACK_IN 0)
else()
  set(NO_LAPACK_IN ${NO_LAPACK})
endif()

if (NOT DEFINED NO_LAPACKE)
  set(NO_LAPACKE_IN 0)
else()
  set(NO_LAPACKE_IN ${NO_LAPACKE})
endif()

if (NOT DEFINED NEED2UNDERSCORES)
  set(NEED2UNDERSCORES_IN 0)
else()
  set(NEED2UNDERSCORES_IN ${NEED2UNDERSCORES})
endif()

if (NOT DEFINED ONLY_CBLAS)
  set(ONLY_CBLAS_IN 0)
else()
  set(ONLY_CBLAS_IN ${ONLY_CBLAS})
endif()

add_custom_command(
  OUTPUT ${PROJECT_BINARY_DIR}/openblas.def
  #TARGET ${OpenBLAS_LIBNAME} PRE_LINK
  COMMAND perl 
  ARGS "${PROJECT_SOURCE_DIR}/exports/gensymbol" "win2k" "${ARCH_IN}" "dummy" "${EXPRECISION_IN}" "${NO_CBLAS_IN}" "${NO_LAPACK_IN}" "${NO_LAPACKE_IN}" "${NEED2UNDERSCORES_IN}" "${ONLY_CBLAS_IN}" "${SYMBOLPREFIX}" "${SYMBOLSUFFIX}" > "${PROJECT_BINARY_DIR}/openblas.def"
  COMMENT "Create openblas.def file"
  VERBATIM)

endif()