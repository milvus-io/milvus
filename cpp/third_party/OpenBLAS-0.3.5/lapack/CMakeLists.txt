
include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_BINARY_DIR})


set(LAPACK_SOURCES
  getrf/getrf_single.c
  potrf/potrf_U_single.c
  potrf/potrf_L_single.c
  lauum/lauum_U_single.c
  lauum/lauum_L_single.c
)

# add a 'z' to filename for complex version
set(LAPACK_MANGLED_SOURCES
  getf2/getf2_k.c
  lauu2/lauu2_U.c
  lauu2/lauu2_L.c
  potf2/potf2_U.c
  potf2/potf2_L.c
)

# sources that need TRANS set
# this has a 'z' version
set(TRANS_SOURCES
  getrs/getrs_single.c
)

# sources that need UNIT set
# these do NOT have a z version
set(UNIT_SOURCES
  trtri/trtri_U_single.c
  trtri/trtri_L_single.c
)

# these have a 'z' version
set(UNIT_SOURCES2
  trti2/trti2_U.c
  trti2/trti2_L.c
)

GenerateNamedObjects("${LAPACK_SOURCES}")
GenerateNamedObjects("${LAPACK_MANGLED_SOURCES}" "" "" false "" "" false 3)

GenerateNamedObjects("laswp/generic/laswp_k_4.c" "" "laswp_plus" false "" ""  false 3)
GenerateNamedObjects("laswp/generic/laswp_k_4.c" "MINUS" "laswp_minus" false "" ""  false 3)

# dynamic_arch laswp needs arch specific code ?
#foreach(TARGET_CORE ${DYNAMIC_CORE})
#      set(TSUFFIX "_${TARGET_CORE}")
#
#GenerateNamedObjects("laswp/generic/laswp_k_4.c" "" "laswp_plus" false "" ${TSUFFIX}  false 3)
#GenerateNamedObjects("laswp/generic/laswp_k_4.c" "MINUS" "laswp_minus" false "" ${TSUFFIX}  false 3)
#endforeach ()

if (USE_THREAD)

  if (USE_OPENMP)
    set(GETRF_SRC getrf/getrf_parallel_omp.c)
  else ()
    set(GETRF_SRC getrf/getrf_parallel.c)
  endif ()

  # these do not have 'z' versions
  set(PARALLEL_SOURCES
    ${GETRF_SRC}
    lauum/lauum_U_parallel.c
    lauum/lauum_L_parallel.c
    potrf/potrf_U_parallel.c
    potrf/potrf_L_parallel.c
  )

  # this has a z version
  list(APPEND TRANS_SOURCES
    getrs/getrs_parallel.c
  )

  # these do NOT have a z version
  list(APPEND UNIT_SOURCES
    trtri/trtri_U_parallel.c
    trtri/trtri_L_parallel.c
  )

  GenerateNamedObjects("${PARALLEL_SOURCES}")
endif ()

foreach (float_type ${FLOAT_TYPES})
  if (${float_type} STREQUAL "COMPLEX" OR ${float_type} STREQUAL "ZCOMPLEX")
    foreach (trans_src ${TRANS_SOURCES})
      string(REGEX MATCH "[a-z]/([a-z]+_)([a-z]+)" op_name ${trans_src})
      string(REPLACE "/" "/z" ztrans_src ${trans_src})
      GenerateNamedObjects("${ztrans_src}" "TRANS=1" "${CMAKE_MATCH_1}N_${CMAKE_MATCH_2}" false "" "" false ${float_type})
      GenerateNamedObjects("${ztrans_src}" "TRANS=2" "${CMAKE_MATCH_1}T_${CMAKE_MATCH_2}" false "" "" false ${float_type})
      GenerateNamedObjects("${ztrans_src}" "TRANS=3" "${CMAKE_MATCH_1}R_${CMAKE_MATCH_2}" false "" "" false ${float_type})
      GenerateNamedObjects("${ztrans_src}" "TRANS=4" "${CMAKE_MATCH_1}C_${CMAKE_MATCH_2}" false "" "" false ${float_type})
    endforeach ()
  else ()
    GenerateCombinationObjects("${TRANS_SOURCES}" "TRANS" "N" "" 4 "" false ${float_type})
  endif ()
endforeach ()

GenerateCombinationObjects("${UNIT_SOURCES}" "UNIT" "N" "" 4)
GenerateCombinationObjects("${UNIT_SOURCES2}" "UNIT" "N" "" 0 "" "" 3)

add_library(lapack OBJECT ${OPENBLAS_SRC})

