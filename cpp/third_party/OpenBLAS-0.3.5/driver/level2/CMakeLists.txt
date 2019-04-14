
include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_BINARY_DIR})

# sources that need to be compiled twice, once with no flags and once with LOWER
set(UL_SOURCES
  sbmv_k.c
  spmv_k.c
  spr_k.c
  spr2_k.c
  syr_k.c
  syr2_k.c
)

# sources that need to be compiled several times, for UNIT, TRANSA
set(U_SOURCES
  trmv_U.c
  tbmv_U.c
  tbsv_U.c
  tpmv_U.c
  tpsv_U.c
  trsv_U.c
)

set(L_SOURCES
  trmv_L.c
  tbmv_L.c
  tbsv_L.c
  tpmv_L.c
  tpsv_L.c
  trsv_L.c
)

set(UL_SMP_SOURCES
  symv_thread.c
  syr_thread.c
  syr2_thread.c
  spr_thread.c
  spr2_thread.c
  spmv_thread.c
  sbmv_thread.c
)

set(NU_SMP_SOURCES
  trmv_thread.c
  tpmv_thread.c
  tbmv_thread.c
)

set(ULVM_COMPLEX_SOURCES
  hbmv_k.c
  hpmv_k.c
  hpr_k.c
  hpr2_k.c
  her_k.c
  her2_k.c
)

# objects that need LOWER set
GenerateCombinationObjects("${UL_SOURCES}" "LOWER" "U" "" 1 "" "" 3)

# gbmv uses a lowercase n and t
GenerateNamedObjects("gbmv_k.c" "" "gbmv_n" false "" "" "" 3)
GenerateNamedObjects("gbmv_k.c" "TRANS" "gbmv_t" false "" "" "" 3)
# c/zgbmv 
GenerateNamedObjects("zgbmv_k.c" "CONJ" "gbmv_r" false "" "" "" 2)
GenerateNamedObjects("zgbmv_k.c" "TRANS;CONJ" "gbmv_c" false "" "" "" 2)
GenerateNamedObjects("zgbmv_k.c" "XCONJ" "gbmv_o" false "" "" "" 2)
GenerateNamedObjects("zgbmv_k.c" "TRANS;XCONJ" "gbmv_u" false "" "" "" 2)
GenerateNamedObjects("zgbmv_k.c" "CONJ;XCONJ" "gbmv_s" false "" "" "" 2)
GenerateNamedObjects("zgbmv_k.c" "TRANS;CONJ;XCONJ" "gbmv_d" false "" "" "" 2)

# special defines for complex
foreach (float_type ${FLOAT_TYPES})

  if (USE_THREAD)
    GenerateNamedObjects("gemv_thread.c" "" "gemv_thread_n" false "" "" false ${float_type})
    GenerateNamedObjects("gemv_thread.c" "TRANSA" "gemv_thread_t" false "" "" false ${float_type})

    GenerateNamedObjects("gbmv_thread.c" "" "gbmv_thread_n" false "" "" false ${float_type})
    GenerateNamedObjects("gbmv_thread.c" "TRANSA" "gbmv_thread_t" false "" "" false ${float_type})
  endif ()

  if (${float_type} STREQUAL "COMPLEX" OR ${float_type} STREQUAL "ZCOMPLEX")

    foreach (u_source ${U_SOURCES})
      string(REGEX MATCH "[a-z]+" op_name ${u_source})
      GenerateCombinationObjects("z${u_source}" "UNIT" "N" "TRANSA=1" 0 "${op_name}_NU" false ${float_type})
      GenerateCombinationObjects("z${u_source}" "UNIT" "N" "TRANSA=2" 0 "${op_name}_TL" false ${float_type})
      GenerateCombinationObjects("z${u_source}" "UNIT" "N" "TRANSA=3" 0 "${op_name}_RU" false ${float_type})
      GenerateCombinationObjects("z${u_source}" "UNIT" "N" "TRANSA=4" 0 "${op_name}_CL" false ${float_type})
    endforeach ()

    foreach (l_source ${L_SOURCES})
      string(REGEX MATCH "[a-z]+" op_name ${l_source})
      GenerateCombinationObjects("z${l_source}" "UNIT" "N" "TRANSA=1" 0 "${op_name}_NL" false ${float_type})
      GenerateCombinationObjects("z${l_source}" "UNIT" "N" "TRANSA=2" 0 "${op_name}_TU" false ${float_type})
      GenerateCombinationObjects("z${l_source}" "UNIT" "N" "TRANSA=3" 0 "${op_name}_RL" false ${float_type})
      GenerateCombinationObjects("z${l_source}" "UNIT" "N" "TRANSA=4" 0 "${op_name}_CU" false ${float_type})
    endforeach ()

    foreach (ulvm_source ${ULVM_COMPLEX_SOURCES})
      string(REGEX MATCH "[a-z0-9]+" op_name ${ulvm_source})
      GenerateNamedObjects("z${ulvm_source}" "" "${op_name}_U" false "" "" false ${float_type})
      GenerateNamedObjects("z${ulvm_source}" "LOWER" "${op_name}_L" false "" "" false ${float_type})
      GenerateNamedObjects("z${ulvm_source}" "HEMVREV" "${op_name}_V" false "" "" false ${float_type})
      GenerateNamedObjects("z${ulvm_source}" "LOWER;HEMVREV" "${op_name}_M" false "" "" false ${float_type})
    endforeach()

    if (USE_THREAD)

      GenerateNamedObjects("gemv_thread.c" "CONJ" "gemv_thread_r" false "" "" false ${float_type})
      GenerateNamedObjects("gemv_thread.c" "CONJ;TRANSA" "gemv_thread_c" false "" "" false ${float_type})
      GenerateNamedObjects("gemv_thread.c" "XCONJ" "gemv_thread_o" false "" "" false ${float_type})
      GenerateNamedObjects("gemv_thread.c" "XCONJ;TRANSA" "gemv_thread_u" false "" "" false ${float_type})
      GenerateNamedObjects("gemv_thread.c" "XCONJ;CONJ" "gemv_thread_s" false "" "" false ${float_type})
      GenerateNamedObjects("gemv_thread.c" "XCONJ;CONJ;TRANSA" "gemv_thread_d" false "" "" false ${float_type})

      GenerateNamedObjects("gbmv_thread.c" "CONJ" "gbmv_thread_r" false "" "" false ${float_type})
      GenerateNamedObjects("gbmv_thread.c" "CONJ;TRANSA" "gbmv_thread_c" false "" "" false ${float_type})
      GenerateNamedObjects("gbmv_thread.c" "XCONJ" "gbmv_thread_o" false "" "" false ${float_type})
      GenerateNamedObjects("gbmv_thread.c" "XCONJ;TRANSA" "gbmv_thread_u" false "" "" false ${float_type})
      GenerateNamedObjects("gbmv_thread.c" "XCONJ;CONJ" "gbmv_thread_s" false "" "" false ${float_type})
      GenerateNamedObjects("gbmv_thread.c" "XCONJ;CONJ;TRANSA" "gbmv_thread_d" false "" "" false ${float_type})

      GenerateNamedObjects("ger_thread.c" "" "ger_thread_U" false "" "" false ${float_type})
      GenerateNamedObjects("ger_thread.c" "CONJ" "ger_thread_C" false "" "" false ${float_type})
      GenerateNamedObjects("ger_thread.c" "XCONJ" "ger_thread_V" false "" "" false ${float_type})
      GenerateNamedObjects("ger_thread.c" "XCONJ;CONJ" "ger_thread_D" false "" "" false ${float_type})

      GenerateNamedObjects("sbmv_thread.c" "HEMV" "hbmv_thread_U" false "" "" false ${float_type})
      GenerateNamedObjects("sbmv_thread.c" "HEMV;LOWER" "hbmv_thread_L" false "" "" false ${float_type})
      GenerateNamedObjects("sbmv_thread.c" "HEMVREV" "hbmv_thread_V" false "" "" false ${float_type})
      GenerateNamedObjects("sbmv_thread.c" "LOWER;HEMVREV" "hbmv_thread_M" false "" "" false ${float_type})

      GenerateNamedObjects("spmv_thread.c" "HEMV" "hpmv_thread_U" false "" "" false ${float_type})
      GenerateNamedObjects("spmv_thread.c" "HEMV;LOWER" "hpmv_thread_L" false "" "" false ${float_type})
      GenerateNamedObjects("spmv_thread.c" "HEMVREV" "hpmv_thread_V" false "" "" false ${float_type})
      GenerateNamedObjects("spmv_thread.c" "LOWER;HEMVREV" "hpmv_thread_M" false "" "" false ${float_type})

      GenerateNamedObjects("spr_thread.c" "HEMV" "hpr_thread_U" false "" "" false ${float_type})
      GenerateNamedObjects("spr_thread.c" "HEMV;LOWER" "hpr_thread_L" false "" "" false ${float_type})
      GenerateNamedObjects("spr_thread.c" "HEMVREV" "hpr_thread_V" false "" "" false ${float_type})
      GenerateNamedObjects("spr_thread.c" "LOWER;HEMVREV" "hpr_thread_M" false "" "" false ${float_type})

      GenerateNamedObjects("spr2_thread.c" "HEMV" "hpr2_thread_U" false "" "" false ${float_type})
      GenerateNamedObjects("spr2_thread.c" "HEMV;LOWER" "hpr2_thread_L" false "" "" false ${float_type})
      GenerateNamedObjects("spr2_thread.c" "HEMVREV" "hpr2_thread_V" false "" "" false ${float_type})
      GenerateNamedObjects("spr2_thread.c" "LOWER;HEMVREV" "hpr2_thread_M" false "" "" false ${float_type})

      GenerateNamedObjects("symv_thread.c" "HEMV" "hemv_thread_U" false "" "" false ${float_type})
      GenerateNamedObjects("symv_thread.c" "HEMV;LOWER" "hemv_thread_L" false "" "" false ${float_type})
      GenerateNamedObjects("symv_thread.c" "HEMVREV" "hemv_thread_V" false "" "" false ${float_type})
      GenerateNamedObjects("symv_thread.c" "LOWER;HEMVREV" "hemv_thread_M" false "" "" false ${float_type})

      GenerateNamedObjects("syr_thread.c" "HER" "her_thread_U" false "" "" false ${float_type})
      GenerateNamedObjects("syr_thread.c" "HER;LOWER" "her_thread_L" false "" "" false ${float_type})
      GenerateNamedObjects("syr_thread.c" "HERREV" "her_thread_V" false "" "" false ${float_type})
      GenerateNamedObjects("syr_thread.c" "LOWER;HERREV" "her_thread_M" false "" "" false ${float_type})

      GenerateNamedObjects("syr2_thread.c" "HER" "her2_thread_U" false "" "" false ${float_type})
      GenerateNamedObjects("syr2_thread.c" "HER;LOWER" "her2_thread_L" false "" "" false ${float_type})
      GenerateNamedObjects("syr2_thread.c" "HERREV" "her2_thread_V" false "" "" false ${float_type})
      GenerateNamedObjects("syr2_thread.c" "LOWER;HERREV" "her2_thread_M" false "" "" false ${float_type})

      foreach (nu_smp_src ${NU_SMP_SOURCES})
        string(REGEX MATCH "[a-z]+_[a-z]+" op_name ${nu_smp_src})
        GenerateCombinationObjects("${nu_smp_src}" "LOWER;UNIT" "U;N" "TRANSA=1" 0 "${op_name}_N" false ${float_type})
        GenerateCombinationObjects("${nu_smp_src}" "LOWER;UNIT" "U;N" "TRANSA=2" 0 "${op_name}_T" false ${float_type})
        GenerateCombinationObjects("${nu_smp_src}" "LOWER;UNIT" "U;N" "TRANSA=3" 0 "${op_name}_R" false ${float_type})
        GenerateCombinationObjects("${nu_smp_src}" "LOWER;UNIT" "U;N" "TRANSA=4" 0 "${op_name}_C" false ${float_type})
      endforeach ()
    endif ()

  else ()
    # For real number functions
    foreach (u_source ${U_SOURCES})
      string(REGEX MATCH "[a-z]+" op_name ${u_source})
      GenerateCombinationObjects("${u_source}" "UNIT" "N" "" 0 "${op_name}_NU" false ${float_type})
      GenerateCombinationObjects("${u_source}" "UNIT" "N" "TRANSA" 0 "${op_name}_TL" false ${float_type})
    endforeach ()

    foreach (l_source ${L_SOURCES})
      string(REGEX MATCH "[a-z]+" op_name ${l_source})
      GenerateCombinationObjects("${l_source}" "UNIT" "N" "" 0 "${op_name}_NL" false ${float_type})
      GenerateCombinationObjects("${l_source}" "UNIT" "N" "TRANSA" 0 "${op_name}_TU" false ${float_type})
    endforeach ()

    if (USE_THREAD)
      GenerateNamedObjects("ger_thread.c" "" "" false "" "" false ${float_type})
      foreach(nu_smp_source ${NU_SMP_SOURCES})
        string(REGEX MATCH "[a-z]+_[a-z]+" op_name ${nu_smp_source})
        GenerateCombinationObjects("${nu_smp_source}" "LOWER;UNIT" "U;N" "" 0 "${op_name}_N" false ${float_type})
        GenerateCombinationObjects("${nu_smp_source}" "LOWER;UNIT" "U;N" "TRANSA" 0 "${op_name}_T" false ${float_type})
      endforeach()
    endif ()
  endif ()
endforeach ()

if (USE_THREAD)
  GenerateCombinationObjects("${UL_SMP_SOURCES}" "LOWER" "U" "" 2)
endif ()

add_library(driver_level2 OBJECT ${OPENBLAS_SRC})
