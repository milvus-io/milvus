
include_directories(${PROJECT_SOURCE_DIR})

# Makefile
function (build_core TARGET_CORE KDIR TSUFFIX KERNEL_DEFINITIONS)
    set (OPENBLAS_SRC "")
    set (ARCH_SUFFIX "")
    include("${PROJECT_SOURCE_DIR}/cmake/kernel.cmake")
    if (${DYNAMIC_ARCH})
        include("${PROJECT_SOURCE_DIR}/cmake/system.cmake")
    endif ()
    SetDefaultL1()
    SetDefaultL2()
    SetDefaultL3()
    ParseMakefileVars("${KERNELDIR}/KERNEL")
    ParseMakefileVars("${KERNELDIR}/KERNEL.${TARGET_CORE}")

    set(KERNEL_INTERFACE common_level1.h common_level2.h common_level3.h)
    if(NOT NO_LAPACK)
        set(KERNEL_INTERFACE ${KERNEL_INTERFACE} common_lapack.h)
    endif ()

    if (${ADD_COMMONOBJS})
        if (X86)
        if (NOT "${CMAKE_C_COMPILER_ID}" STREQUAL "MSVC")
          GenerateNamedObjects("${KERNELDIR}/cpuid.S" "" "" false "" "" true)
        else()
          GenerateNamedObjects("${KERNELDIR}/cpuid_win.c" "" "" false "" "" true)
        endif()
        endif ()

        # don't use float type name mangling here
        GenerateNamedObjects("${KERNELDIR}/${LSAME_KERNEL}" "F_INTERFACE" "lsame" false "" "" true "")
        GenerateNamedObjects("${KERNELDIR}/${SCABS_KERNEL}" "COMPLEX;F_INTERFACE" "scabs1" false "" "" true "")
        GenerateNamedObjects("${KERNELDIR}/${DCABS_KERNEL}" "DOUBLE;COMPLEX;F_INTERFACE" "dcabs1" false "" "" true "")
    endif ()

    # Run with no ARCH_SUFFIX for above
    set (ARCH_SUFFIX "${TSUFFIX}")
    # Makefile.L1
    foreach (float_type ${FLOAT_TYPES})
      # a bit of metaprogramming here to pull out the appropriate KERNEL var
      string(SUBSTRING ${float_type} 0 1 float_char)
      GenerateNamedObjects("${KERNELDIR}/${${float_char}AMAXKERNEL}" "USE_ABS" "amax_k" false "" "" false ${float_type})
      GenerateNamedObjects("${KERNELDIR}/${${float_char}AMINKERNEL}" "USE_ABS;USE_MIN" "amin_k" false "" "" false ${float_type})
      if (DEFINED ${float_char}MAXKERNEL)
        GenerateNamedObjects("${KERNELDIR}/${${float_char}MAXKERNEL}" "" "max_k" false "" "" false ${float_type})
      endif ()
      if (DEFINED ${float_char}MINKERNEL)
        GenerateNamedObjects("${KERNELDIR}/${${float_char}MINKERNEL}" "" "min_k" false "" "" false ${float_type})
      endif ()
      GenerateNamedObjects("${KERNELDIR}/${I${float_char}AMAXKERNEL}" "USE_ABS" "i*amax_k" false "" "" false ${float_type})
      GenerateNamedObjects("${KERNELDIR}/${I${float_char}AMINKERNEL}" "USE_ABS;USE_MIN" "i*amin_k" false "" "" false ${float_type})
      if (DEFINED I${float_char}MAXKERNEL)
        GenerateNamedObjects("${KERNELDIR}/${I${float_char}MAXKERNEL}" "" "i*max_k" false "" "" false ${float_type})
      endif ()
      if (DEFINED I${float_char}MINKERNEL)
        GenerateNamedObjects("${KERNELDIR}/${I${float_char}MINKERNEL}" "" "i*min_k" false "" "" false ${float_type})
      endif ()
      GenerateNamedObjects("${KERNELDIR}/${${float_char}ASUMKERNEL}" "" "asum_k" false "" "" false ${float_type})
      GenerateNamedObjects("${KERNELDIR}/${${float_char}AXPYKERNEL}" "" "axpy_k" false "" "" false ${float_type})
      GenerateNamedObjects("${KERNELDIR}/${${float_char}COPYKERNEL}" "C_INTERFACE" "copy_k" false "" "" false ${float_type})
      GenerateNamedObjects("${KERNELDIR}/${${float_char}NRM2KERNEL}" "" "nrm2_k" false "" "" false ${float_type})
      GenerateNamedObjects("${KERNELDIR}/${${float_char}ROTKERNEL}" "" "rot_k" false "" "" false ${float_type})
      GenerateNamedObjects("${KERNELDIR}/${${float_char}SCALKERNEL}" "" "scal_k" false "" "" false ${float_type})
      GenerateNamedObjects("${KERNELDIR}/${${float_char}SWAPKERNEL}" "" "swap_k" false "" "" false ${float_type})
      GenerateNamedObjects("${KERNELDIR}/${${float_char}AXPBYKERNEL}" "" "axpby_k" false "" "" false ${float_type})

      if (${float_type} STREQUAL "COMPLEX" OR ${float_type} STREQUAL "ZCOMPLEX")
        GenerateNamedObjects("${KERNELDIR}/${${float_char}AXPYKERNEL}" "CONJ" "axpyc_k" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}DOTKERNEL}" "" "dotu_k" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}DOTKERNEL}" "CONJ" "dotc_k" false "" "" false ${float_type})
      else ()
        GenerateNamedObjects("${KERNELDIR}/${${float_char}DOTKERNEL}" "" "dot_k" false "" "" false ${float_type})
      endif ()

      if (${float_type} STREQUAL "COMPLEX")
      GenerateNamedObjects("${KERNELDIR}/${${float_char}ROTKERNEL}" "" "srot_k" false "" "" false ${float_type})
      endif()
      if (${float_type} STREQUAL "ZCOMPLEX")
      GenerateNamedObjects("${KERNELDIR}/${${float_char}ROTKERNEL}" "" "drot_k" false "" "" false ${float_type})
      endif()

    endforeach ()

    #dsdot,sdsdot
    GenerateNamedObjects("${KERNELDIR}/${DSDOTKERNEL}" "DSDOT" "d*dot_k" false "" "" false "SINGLE")
    GenerateNamedObjects("${KERNELDIR}/${DSDOTKERNEL}" "DSDOT" "dsdot_k" false "" "" false "SINGLE")

    # Makefile.L2
    GenerateCombinationObjects("generic/symv_k.c" "LOWER" "U" "" 1 "" "" 3)
    GenerateNamedObjects("generic/ger.c" "" "ger_k" false "" "" "" 3)
    foreach (float_type ${FLOAT_TYPES})
      string(SUBSTRING ${float_type} 0 1 float_char)
      if (${float_type} STREQUAL "COMPLEX" OR ${float_type} STREQUAL "ZCOMPLEX")
        GenerateNamedObjects("${KERNELDIR}/${${float_char}GERUKERNEL}" "" "geru_k" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}GERCKERNEL}" "CONJ" "gerc_k" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}GERUKERNEL}" "XCONJ" "gerv_k" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}GERCKERNEL}" "CONJ;XCONJ" "gerd_k" false "" "" false ${float_type})

        GenerateNamedObjects("${KERNELDIR}/${${float_char}GEMVNKERNEL}" "" "gemv_n" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}GEMVTKERNEL}" "TRANSA" "gemv_t" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}GEMVNKERNEL}" "CONJ" "gemv_r" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}GEMVTKERNEL}" "CONJ;TRANSA" "gemv_c" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}GEMVNKERNEL}" "XCONJ" "gemv_o" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}GEMVTKERNEL}" "XCONJ;TRANSA" "gemv_u" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}GEMVNKERNEL}" "XCONJ;CONJ" "gemv_s" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}GEMVTKERNEL}" "XCONJ;CONJ;TRANSA" "gemv_d" false "" "" false ${float_type})

        GenerateNamedObjects("${KERNELDIR}/${${float_char}HEMV_U_KERNEL}" "HEMV" "hemv_U" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}HEMV_L_KERNEL}" "HEMV;LOWER" "hemv_L" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}HEMV_V_KERNEL}" "HEMV;HEMVREV" "hemv_V" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}HEMV_M_KERNEL}" "HEMV;HEMVREV;LOWER" "hemv_M" false "" "" false ${float_type})

      else ()
        GenerateNamedObjects("${KERNELDIR}/${${float_char}GEMVNKERNEL}" "" "gemv_n" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}GEMVTKERNEL}" "TRANS" "gemv_t" false "" "" false ${float_type})
      endif ()
    endforeach ()

    # Makefile.L3
    set(USE_TRMM false)

    if (ARM OR ARM64 OR "${TARGET_CORE}" STREQUAL "LONGSOON3B" OR "${TARGET_CORE}" STREQUAL "GENERIC" OR "${CORE}" STREQUAL "generic" OR "${TARGET_CORE}" STREQUAL "HASWELL" OR "${CORE}" STREQUAL "haswell" OR "${CORE}" STREQUAL "zen" OR "${TARGET_CORE}" STREQUAL "SKYLAKEX" OR "${CORE}" STREQUAL "skylakex")
      set(USE_TRMM true)
    endif ()

    foreach (float_type SINGLE DOUBLE)
      string(SUBSTRING ${float_type} 0 1 float_char)
      GenerateNamedObjects("${KERNELDIR}/${${float_char}GEMMKERNEL}" "" "gemm_kernel" false "" "" false ${float_type})
    endforeach()

    foreach (float_type ${FLOAT_TYPES})
      string(SUBSTRING ${float_type} 0 1 float_char)
      if (${float_char}GEMMINCOPY)
        GenerateNamedObjects("${KERNELDIR}/${${float_char}GEMMINCOPY}" "${float_type}" "${${float_char}GEMMINCOPYOBJ}" false "" "" true ${float_type})
      endif ()

      if (${float_char}GEMMITCOPY)
        GenerateNamedObjects("${KERNELDIR}/${${float_char}GEMMITCOPY}" "${float_type}" "${${float_char}GEMMITCOPYOBJ}" false "" "" true ${float_type})
      endif ()

      if (${float_char}GEMMONCOPY)
        GenerateNamedObjects("${KERNELDIR}/${${float_char}GEMMONCOPY}" "${float_type}" "${${float_char}GEMMONCOPYOBJ}" false "" "" true ${float_type})
      endif ()

      if (${float_char}GEMMOTCOPY)
        GenerateNamedObjects("${KERNELDIR}/${${float_char}GEMMOTCOPY}" "${float_type}" "${${float_char}GEMMOTCOPYOBJ}" false "" "" true ${float_type})
      endif ()

      GenerateNamedObjects("${KERNELDIR}/${${float_char}GEMM_BETA}" "" "gemm_beta" false "" "" false ${float_type})

      if (USE_TRMM)
        set(TRMM_KERNEL "${${float_char}TRMMKERNEL}")
      else ()
        set(TRMM_KERNEL "${${float_char}GEMMKERNEL}")
      endif ()

      if (${float_type} STREQUAL "COMPLEX" OR ${float_type} STREQUAL "ZCOMPLEX")

        # just enumerate all these. there is an extra define for these indicating which side is a conjugate (e.g. CN NC NN) that I don't really want to work into GenerateCombinationObjects

        GenerateNamedObjects("${KERNELDIR}/${${float_char}GEMMKERNEL}" "NN" "gemm_kernel_n" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}GEMMKERNEL}" "CN" "gemm_kernel_l" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}GEMMKERNEL}" "NC" "gemm_kernel_r" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}GEMMKERNEL}" "CC" "gemm_kernel_b" false "" "" false ${float_type})

        GenerateNamedObjects("${KERNELDIR}/${TRMM_KERNEL}" "TRMMKERNEL;LEFT;NN" "trmm_kernel_LN" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${TRMM_KERNEL}" "TRMMKERNEL;LEFT;TRANSA;NN" "trmm_kernel_LT" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${TRMM_KERNEL}" "TRMMKERNEL;LEFT;CONJ;CN" "trmm_kernel_LR" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${TRMM_KERNEL}" "TRMMKERNEL;LEFT;TRANSA;CONJ;CN" "trmm_kernel_LC" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${TRMM_KERNEL}" "TRMMKERNEL;NN" "trmm_kernel_RN" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${TRMM_KERNEL}" "TRMMKERNEL;TRANSA;NN" "trmm_kernel_RT" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${TRMM_KERNEL}" "TRMMKERNEL;CONJ;NC" "trmm_kernel_RR" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${TRMM_KERNEL}" "TRMMKERNEL;TRANSA;CONJ;NC" "trmm_kernel_RC" false "" "" false ${float_type})

        GenerateNamedObjects("${KERNELDIR}/${${float_char}TRSMKERNEL_LN}" "UPPER;LN;TRSMKERNEL;CONJ" "trsm_kernel_LR" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}TRSMKERNEL_LT}" "LT;TRSMKERNEL;CONJ" "trsm_kernel_LC" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}TRSMKERNEL_RN}" "UPPER;RN;TRSMKERNEL;CONJ" "trsm_kernel_RR" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}TRSMKERNEL_RT}" "RT;TRSMKERNEL;CONJ" "trsm_kernel_RC" false "" "" false ${float_type})


        #hemm
      GenerateNamedObjects("generic/zhemm_utcopy_${${float_char}GEMM_UNROLL_M}.c" "" "hemm_iutcopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/zhemm_ltcopy_${${float_char}GEMM_UNROLL_M}.c" "LOWER" "hemm_iltcopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/zhemm_utcopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER" "hemm_outcopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/zhemm_ltcopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;LOWER" "hemm_oltcopy" false "" "" false ${float_type})

      # symm for c and z
      GenerateNamedObjects("generic/zsymm_ucopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER" "symm_outcopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/zsymm_ucopy_${${float_char}GEMM_UNROLL_M}.c" "" "symm_iutcopy" false "" "" false ${float_type})

      GenerateNamedObjects("generic/zsymm_lcopy_${${float_char}GEMM_UNROLL_N}.c" "LOWER;OUTER" "symm_oltcopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/zsymm_lcopy_${${float_char}GEMM_UNROLL_M}.c" "LOWER" "symm_iltcopy" false "" "" false ${float_type})

      GenerateNamedObjects("generic/ztrmm_uncopy_${${float_char}GEMM_UNROLL_M}.c" "UNIT" "trmm_iunucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrmm_uncopy_${${float_char}GEMM_UNROLL_M}.c" "" "trmm_iunncopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrmm_uncopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;UNIT" "trmm_ounucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrmm_uncopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER" "trmm_ounncopy" false "" "" false ${float_type})

      GenerateNamedObjects("generic/ztrmm_lncopy_${${float_char}GEMM_UNROLL_M}.c" "LOWER;UNIT" "trmm_ilnucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrmm_lncopy_${${float_char}GEMM_UNROLL_M}.c" "LOWER" "trmm_ilnncopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrmm_lncopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;LOWER;UNIT" "trmm_olnucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrmm_lncopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;LOWER" "trmm_olnncopy" false "" "" false ${float_type})

      GenerateNamedObjects("generic/ztrmm_utcopy_${${float_char}GEMM_UNROLL_M}.c" "UNIT" "trmm_iutucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrmm_utcopy_${${float_char}GEMM_UNROLL_M}.c" "" "trmm_iutncopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrmm_utcopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;UNIT" "trmm_outucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrmm_utcopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER" "trmm_outncopy" false "" "" false ${float_type})

      GenerateNamedObjects("generic/ztrmm_ltcopy_${${float_char}GEMM_UNROLL_M}.c" "LOWER;UNIT" "trmm_iltucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrmm_ltcopy_${${float_char}GEMM_UNROLL_M}.c" "LOWER" "trmm_iltncopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrmm_ltcopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;LOWER;UNIT" "trmm_oltucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrmm_ltcopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;LOWER" "trmm_oltncopy" false "" "" false ${float_type})

      GenerateNamedObjects("generic/ztrsm_uncopy_${${float_char}GEMM_UNROLL_M}.c" "UNIT" "trsm_iunucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrsm_uncopy_${${float_char}GEMM_UNROLL_M}.c" "" "trsm_iunncopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrsm_uncopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;UNIT" "trsm_ounucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrsm_uncopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER" "trsm_ounncopy" false "" "" false ${float_type})

      GenerateNamedObjects("generic/ztrsm_lncopy_${${float_char}GEMM_UNROLL_M}.c" "LOWER;UNIT" "trsm_ilnucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrsm_lncopy_${${float_char}GEMM_UNROLL_M}.c" "LOWER" "trsm_ilnncopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrsm_lncopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;LOWER;UNIT" "trsm_olnucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrsm_lncopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;LOWER" "trsm_olnncopy" false "" "" false ${float_type})

      GenerateNamedObjects("generic/ztrsm_utcopy_${${float_char}GEMM_UNROLL_M}.c" "UNIT" "trsm_iutucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrsm_utcopy_${${float_char}GEMM_UNROLL_M}.c" "" "trsm_iutncopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrsm_utcopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;UNIT" "trsm_outucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrsm_utcopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER" "trsm_outncopy" false "" "" false ${float_type})

      GenerateNamedObjects("generic/ztrsm_ltcopy_${${float_char}GEMM_UNROLL_M}.c" "LOWER;UNIT" "trsm_iltucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrsm_ltcopy_${${float_char}GEMM_UNROLL_M}.c" "LOWER" "trsm_iltncopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrsm_ltcopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;LOWER;UNIT" "trsm_oltucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/ztrsm_ltcopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;LOWER" "trsm_oltncopy" false "" "" false ${float_type})

      #gemm3m
      if (USE_GEMM3M)
        GenerateNamedObjects("${KERNELDIR}/${${float_char}GEMM3MKERNEL}" "NN" "gemm3m_kernel" false "" "" false ${float_type})

        GenerateNamedObjects("generic/zgemm3m_ncopy_${${float_char}GEMM3M_UNROLL_N}.c" "USE_ALPHA" "gemm3m_oncopyb" false "" "" false ${float_type})
        GenerateNamedObjects("generic/zgemm3m_ncopy_${${float_char}GEMM3M_UNROLL_N}.c" "USE_ALPHA;REAL_ONLY" "gemm3m_oncopyr" false "" "" false ${float_type})
        GenerateNamedObjects("generic/zgemm3m_ncopy_${${float_char}GEMM3M_UNROLL_N}.c" "USE_ALPHA;IMAGE_ONLY" "gemm3m_oncopyi" false "" "" false ${float_type})

        GenerateNamedObjects("generic/zgemm3m_tcopy_${${float_char}GEMM3M_UNROLL_N}.c" "USE_ALPHA" "gemm3m_otcopyb" false "" "" false ${float_type})
        GenerateNamedObjects("generic/zgemm3m_tcopy_${${float_char}GEMM3M_UNROLL_N}.c" "USE_ALPHA;REAL_ONLY" "gemm3m_otcopyr" false "" "" false ${float_type})
        GenerateNamedObjects("generic/zgemm3m_tcopy_${${float_char}GEMM3M_UNROLL_N}.c" "USE_ALPHA;IMAGE_ONLY" "gemm3m_otcopyi" false "" "" false ${float_type})

        GenerateNamedObjects("generic/zgemm3m_ncopy_${${float_char}GEMM3M_UNROLL_M}.c" "ICOPY" "gemm3m_incopyb" false "" "" false ${float_type})
        GenerateNamedObjects("generic/zgemm3m_ncopy_${${float_char}GEMM3M_UNROLL_M}.c" "ICOPY;REAL_ONLY" "gemm3m_incopyr" false "" "" false ${float_type})
        GenerateNamedObjects("generic/zgemm3m_ncopy_${${float_char}GEMM3M_UNROLL_M}.c" "ICOPY;IMAGE_ONLY" "gemm3m_incopyi" false "" "" false ${float_type})

        GenerateNamedObjects("generic/zgemm3m_tcopy_${${float_char}GEMM3M_UNROLL_M}.c" "ICOPY" "gemm3m_itcopyb" false "" "" false ${float_type})
        GenerateNamedObjects("generic/zgemm3m_tcopy_${${float_char}GEMM3M_UNROLL_M}.c" "ICOPY;REAL_ONLY" "gemm3m_itcopyr" false "" "" false ${float_type})
        GenerateNamedObjects("generic/zgemm3m_tcopy_${${float_char}GEMM3M_UNROLL_M}.c" "ICOPY;IMAGE_ONLY" "gemm3m_itcopyi" false "" "" false ${float_type})

#hemm3m and symm3m
        foreach(name symm3m hemm3m)
          GenerateNamedObjects("generic/z${name}_ucopy_${${float_char}GEMM3M_UNROLL_M}.c" "USE_ALPHA" "${name}_oucopyb.c" false "" "" false ${float_type})
          GenerateNamedObjects("generic/z${name}_ucopy_${${float_char}GEMM3M_UNROLL_M}.c" "USE_ALPHA;REAL_ONLY" "${name}_oucopyr.c" false "" "" false ${float_type})
          GenerateNamedObjects("generic/z${name}_ucopy_${${float_char}GEMM3M_UNROLL_M}.c" "USE_ALPHA;IMAGE_ONLY" "${name}_oucopyi.c" false "" "" false ${float_type})
          GenerateNamedObjects("generic/z${name}_lcopy_${${float_char}GEMM3M_UNROLL_M}.c" "USE_ALPHA" "${name}_olcopyb.c" false "" "" false ${float_type})
          GenerateNamedObjects("generic/z${name}_lcopy_${${float_char}GEMM3M_UNROLL_M}.c" "USE_ALPHA;REAL_ONLY" "${name}_olcopyr.c" false "" "" false ${float_type})
          GenerateNamedObjects("generic/z${name}_lcopy_${${float_char}GEMM3M_UNROLL_M}.c" "USE_ALPHA;IMAGE_ONLY" "${name}_olcopyi.c" false "" "" false ${float_type})
          GenerateNamedObjects("generic/z${name}_ucopy_${${float_char}GEMM3M_UNROLL_M}.c" "" "${name}_iucopyb.c" false "" "" false ${float_type})
          GenerateNamedObjects("generic/z${name}_ucopy_${${float_char}GEMM3M_UNROLL_M}.c" "REAL_ONLY" "${name}_iucopyr.c" false "" "" false ${float_type})
          GenerateNamedObjects("generic/z${name}_ucopy_${${float_char}GEMM3M_UNROLL_M}.c" "IMAGE_ONLY" "${name}_iucopyi.c" false "" "" false ${float_type})
          GenerateNamedObjects("generic/z${name}_lcopy_${${float_char}GEMM3M_UNROLL_M}.c" "" "${name}_ilcopyb.c" false "" "" false ${float_type})
          GenerateNamedObjects("generic/z${name}_lcopy_${${float_char}GEMM3M_UNROLL_M}.c" "REAL_ONLY" "${name}_ilcopyr.c" false "" "" false ${float_type})
          GenerateNamedObjects("generic/z${name}_lcopy_${${float_char}GEMM3M_UNROLL_M}.c" "IMAGE_ONLY" "${name}_ilcopyi.c" false "" "" false ${float_type})

        endforeach ()

      endif()

      else ()  #For real
        GenerateCombinationObjects("${KERNELDIR}/${TRMM_KERNEL}" "LEFT;TRANSA" "R;N" "TRMMKERNEL" 2 "trmm_kernel" false ${float_type})

        # symm for s and d
        GenerateNamedObjects("generic/symm_ucopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER" "symm_outcopy" false "" "" false ${float_type})
        GenerateNamedObjects("generic/symm_ucopy_${${float_char}GEMM_UNROLL_M}.c" "" "symm_iutcopy" false "" "" false ${float_type})

        GenerateNamedObjects("generic/symm_lcopy_${${float_char}GEMM_UNROLL_N}.c" "LOWER;OUTER" "symm_oltcopy" false "" "" false ${float_type})
        GenerateNamedObjects("generic/symm_lcopy_${${float_char}GEMM_UNROLL_M}.c" "LOWER" "symm_iltcopy" false "" "" false ${float_type})

      # These don't use a scheme that is easy to iterate over - the filenames have part of the DEFINE codes in them, for UPPER/TRANS but not for UNIT/OUTER. Also TRANS is not passed in as a define.
      # Could simplify it a bit by pairing up by -UUNIT/-DUNIT.

      GenerateNamedObjects("generic/trmm_uncopy_${${float_char}GEMM_UNROLL_M}.c" "UNIT" "trmm_iunucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trmm_uncopy_${${float_char}GEMM_UNROLL_M}.c" "" "trmm_iunncopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trmm_uncopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;UNIT" "trmm_ounucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trmm_uncopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER" "trmm_ounncopy" false "" "" false ${float_type})

      GenerateNamedObjects("generic/trmm_lncopy_${${float_char}GEMM_UNROLL_M}.c" "LOWER;UNIT" "trmm_ilnucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trmm_lncopy_${${float_char}GEMM_UNROLL_M}.c" "LOWER" "trmm_ilnncopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trmm_lncopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;LOWER;UNIT" "trmm_olnucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trmm_lncopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;LOWER" "trmm_olnncopy" false "" "" false ${float_type})

      GenerateNamedObjects("generic/trmm_utcopy_${${float_char}GEMM_UNROLL_M}.c" "UNIT" "trmm_iutucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trmm_utcopy_${${float_char}GEMM_UNROLL_M}.c" "" "trmm_iutncopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trmm_utcopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;UNIT" "trmm_outucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trmm_utcopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER" "trmm_outncopy" false "" "" false ${float_type})

      GenerateNamedObjects("generic/trmm_ltcopy_${${float_char}GEMM_UNROLL_M}.c" "LOWER;UNIT" "trmm_iltucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trmm_ltcopy_${${float_char}GEMM_UNROLL_M}.c" "LOWER" "trmm_iltncopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trmm_ltcopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;LOWER;UNIT" "trmm_oltucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trmm_ltcopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;LOWER" "trmm_oltncopy" false "" "" false ${float_type})

      GenerateNamedObjects("generic/trsm_uncopy_${${float_char}GEMM_UNROLL_M}.c" "UNIT" "trsm_iunucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trsm_uncopy_${${float_char}GEMM_UNROLL_M}.c" "" "trsm_iunncopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trsm_uncopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;UNIT" "trsm_ounucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trsm_uncopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER" "trsm_ounncopy" false "" "" false ${float_type})

      GenerateNamedObjects("generic/trsm_lncopy_${${float_char}GEMM_UNROLL_M}.c" "LOWER;UNIT" "trsm_ilnucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trsm_lncopy_${${float_char}GEMM_UNROLL_M}.c" "LOWER" "trsm_ilnncopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trsm_lncopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;LOWER;UNIT" "trsm_olnucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trsm_lncopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;LOWER" "trsm_olnncopy" false "" "" false ${float_type})

      GenerateNamedObjects("generic/trsm_utcopy_${${float_char}GEMM_UNROLL_M}.c" "UNIT" "trsm_iutucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trsm_utcopy_${${float_char}GEMM_UNROLL_M}.c" "" "trsm_iutncopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trsm_utcopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;UNIT" "trsm_outucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trsm_utcopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER" "trsm_outncopy" false "" "" false ${float_type})

      GenerateNamedObjects("generic/trsm_ltcopy_${${float_char}GEMM_UNROLL_M}.c" "LOWER;UNIT" "trsm_iltucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trsm_ltcopy_${${float_char}GEMM_UNROLL_M}.c" "LOWER" "trsm_iltncopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trsm_ltcopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;LOWER;UNIT" "trsm_oltucopy" false "" "" false ${float_type})
      GenerateNamedObjects("generic/trsm_ltcopy_${${float_char}GEMM_UNROLL_N}.c" "OUTER;LOWER" "trsm_oltncopy" false "" "" false ${float_type})

      endif ()

      GenerateNamedObjects("${KERNELDIR}/${${float_char}TRSMKERNEL_LN}" "UPPER;LN;TRSMKERNEL" "trsm_kernel_LN" false "" "" false ${float_type})
      GenerateNamedObjects("${KERNELDIR}/${${float_char}TRSMKERNEL_LT}" "LT;TRSMKERNEL" "trsm_kernel_LT" false "" "" false ${float_type})
      GenerateNamedObjects("${KERNELDIR}/${${float_char}TRSMKERNEL_RN}" "UPPER;RN;TRSMKERNEL" "trsm_kernel_RN" false "" "" false ${float_type})
      GenerateNamedObjects("${KERNELDIR}/${${float_char}TRSMKERNEL_RT}" "RT;TRSMKERNEL" "trsm_kernel_RT" false "" "" false ${float_type})



      if (NOT DEFINED ${float_char}OMATCOPY_CN)
        if (${float_char} STREQUAL "Z" OR ${float_char} STREQUAL "C")
          set(${float_char}OMATCOPY_CN ../arm/zomatcopy_cn.c)
        else ()
          set(${float_char}OMATCOPY_CN ../arm/omatcopy_cn.c)
        endif ()
      endif ()
      if (NOT DEFINED ${float_char}OMATCOPY_RN)
        if (${float_char} STREQUAL "Z" OR ${float_char} STREQUAL "C")
          set(${float_char}OMATCOPY_RN ../arm/zomatcopy_rn.c)
        else ()
          set(${float_char}OMATCOPY_RN ../arm/omatcopy_rn.c)
        endif  ()
      endif ()
      if (NOT DEFINED ${float_char}OMATCOPY_CT)
        if (${float_char} STREQUAL "Z" OR ${float_char} STREQUAL "C")
          set(${float_char}OMATCOPY_CT ../arm/zomatcopy_ct.c)
        else ()
          set(${float_char}OMATCOPY_CT ../arm/omatcopy_ct.c)
        endif ()
      endif ()
      if (NOT DEFINED ${float_char}OMATCOPY_RT)
        if (${float_char} STREQUAL "Z" OR ${float_char} STREQUAL "C")
          set(${float_char}OMATCOPY_RT ../arm/zomatcopy_rt.c)
        else ()
          set(${float_char}OMATCOPY_RT ../arm/omatcopy_rt.c)
        endif ()
      endif ()

      GenerateNamedObjects("${KERNELDIR}/${${float_char}OMATCOPY_CN}" "" "omatcopy_k_cn" false "" "" false ${float_type})
      GenerateNamedObjects("${KERNELDIR}/${${float_char}OMATCOPY_RN}" "ROWM" "omatcopy_k_rn" false "" "" false ${float_type})
      GenerateNamedObjects("${KERNELDIR}/${${float_char}OMATCOPY_CT}" "" "omatcopy_k_ct" false "" "" false ${float_type})
      GenerateNamedObjects("${KERNELDIR}/${${float_char}OMATCOPY_RT}" "ROWM" "omatcopy_k_rt" false "" "" false ${float_type})

      if (NOT DEFINED ${float_char}OMATCOPY_CNC)
        if (${float_char} STREQUAL "Z" OR ${float_char} STREQUAL "C")
          set(${float_char}OMATCOPY_CNC ../arm/zomatcopy_cnc.c)
        endif ()
      endif ()
      if (NOT DEFINED ${float_char}OMATCOPY_RNC)
        if (${float_char} STREQUAL "Z" OR ${float_char} STREQUAL "C")
          set(${float_char}OMATCOPY_RNC ../arm/zomatcopy_rnc.c)
        endif  ()
      endif ()
      if (NOT DEFINED ${float_char}OMATCOPY_CTC)
        if (${float_char} STREQUAL "Z" OR ${float_char} STREQUAL "C")
          set(${float_char}OMATCOPY_CTC ../arm/zomatcopy_ctc.c)
        endif ()
      endif ()
      if (NOT DEFINED ${float_char}OMATCOPY_RTC)
        if (${float_char} STREQUAL "Z" OR ${float_char} STREQUAL "C")
          set(${float_char}OMATCOPY_RTC ../arm/zomatcopy_rtc.c)
        endif ()
      endif ()

      if (${float_char} STREQUAL "Z" OR ${float_char} STREQUAL "C")
        GenerateNamedObjects("${KERNELDIR}/${${float_char}OMATCOPY_CNC}" "CONJ" "omatcopy_k_cnc" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}OMATCOPY_RNC}" "CONJ;ROWM" "omatcopy_k_rnc" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}OMATCOPY_CTC}" "CONJ" "omatcopy_k_ctc" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}OMATCOPY_RTC}" "CONJ;ROWM" "omatcopy_k_rtc" false "" "" false ${float_type})
      endif()

      #imatcopy
      if (NOT DEFINED ${float_char}IMATCOPY_CN)
        if (${float_char} STREQUAL "Z" OR ${float_char} STREQUAL "C")
          set(${float_char}IMATCOPY_CN ../generic/zimatcopy_cn.c)
        else ()
          set(${float_char}IMATCOPY_CN ../generic/imatcopy_cn.c)
        endif ()
      endif ()

      if (NOT DEFINED ${float_char}IMATCOPY_RN)
        if (${float_char} STREQUAL "Z" OR ${float_char} STREQUAL "C")
          set(${float_char}IMATCOPY_RN ../generic/zimatcopy_rn.c)
        else ()
          set(${float_char}IMATCOPY_RN ../generic/imatcopy_rn.c)
        endif ()
      endif ()

      if (NOT DEFINED ${float_char}IMATCOPY_CT)
        if (${float_char} STREQUAL "Z" OR ${float_char} STREQUAL "C")
          set(${float_char}IMATCOPY_CT ../generic/zimatcopy_ct.c)
        else ()
          set(${float_char}IMATCOPY_CT ../generic/imatcopy_ct.c)
        endif ()
      endif ()

      if (NOT DEFINED ${float_char}IMATCOPY_RT)
        if (${float_char} STREQUAL "Z" OR ${float_char} STREQUAL "C")
          set(${float_char}IMATCOPY_RT ../generic/zimatcopy_rt.c)
        else ()
          set(${float_char}IMATCOPY_RT ../generic/imatcopy_rt.c)
        endif ()
      endif ()

      GenerateNamedObjects("${KERNELDIR}/${${float_char}IMATCOPY_CN}" "" "imatcopy_k_cn" false "" "" false ${float_type})
      GenerateNamedObjects("${KERNELDIR}/${${float_char}IMATCOPY_RN}" "ROWM" "imatcopy_k_rn" false "" "" false ${float_type})
      GenerateNamedObjects("${KERNELDIR}/${${float_char}IMATCOPY_CT}" "" "imatcopy_k_ct" false "" "" false ${float_type})
      GenerateNamedObjects("${KERNELDIR}/${${float_char}IMATCOPY_RT}" "ROWM" "imatcopy_k_rt" false "" "" false ${float_type})


      if (NOT DEFINED ${float_char}IMATCOPY_CNC)
        if (${float_char} STREQUAL "Z" OR ${float_char} STREQUAL "C")
          set(${float_char}IMATCOPY_CNC ../generic/zimatcopy_cnc.c)
        endif ()
      endif ()
      if (NOT DEFINED ${float_char}IMATCOPY_RNC)
        if (${float_char} STREQUAL "Z" OR ${float_char} STREQUAL "C")
          set(${float_char}IMATCOPY_RNC ../generic/zimatcopy_rnc.c)
        endif  ()
      endif ()
      if (NOT DEFINED ${float_char}IMATCOPY_CTC)
        if (${float_char} STREQUAL "Z" OR ${float_char} STREQUAL "C")
          set(${float_char}IMATCOPY_CTC ../generic/zimatcopy_ctc.c)
        endif ()
      endif ()
      if (NOT DEFINED ${float_char}IMATCOPY_RTC)
        if (${float_char} STREQUAL "Z" OR ${float_char} STREQUAL "C")
          set(${float_char}IMATCOPY_RTC ../generic/zimatcopy_rtc.c)
        endif ()
      endif ()

      if (${float_char} STREQUAL "Z" OR ${float_char} STREQUAL "C")
        GenerateNamedObjects("${KERNELDIR}/${${float_char}IMATCOPY_CNC}" "CONJ" "imatcopy_k_cnc" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}IMATCOPY_RNC}" "CONJ;ROWM" "imatcopy_k_rnc" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}IMATCOPY_CTC}" "CONJ" "imatcopy_k_ctc" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}IMATCOPY_RTC}" "CONJ;ROWM" "imatcopy_k_rtc" false "" "" false ${float_type})
      endif()

      #geadd
      GenerateNamedObjects("${KERNELDIR}/${${float_char}GEADD_KERNEL}" "" "geadd_k" false "" "" false ${float_type})
    endforeach ()

    # Makefile.LA
    if(NOT NO_LAPACK)
      foreach (float_type ${FLOAT_TYPES})
        if (NOT DEFINED ${float_char}NEG_TCOPY)
          if (${float_char} STREQUAL "Z" OR ${float_char} STREQUAL "C" OR ${float_char} STREQUAL "X")
            set(${float_char}NEG_TCOPY ../generic/zneg_tcopy.c)
          else ()
            set(${float_char}NEG_TCOPY ../generic/neg_tcopy.c)
          endif ()
        endif ()

        if (NOT DEFINED ${float_char}LASWP_NCOPY)
          if (${float_char} STREQUAL "Z" OR ${float_char} STREQUAL "C" OR ${float_char} STREQUAL "X")
            set(${float_char}LASWP_NCOPY ../generic/zlaswp_ncopy.c)
          else ()
            set(${float_char}LASWP_NCOPY ../generic/laswp_ncopy.c)
          endif ()
        endif ()
        string(SUBSTRING ${float_type} 0 1 float_char)
        GenerateNamedObjects("${KERNELDIR}/${${float_char}NEG_TCOPY}_${${float_char}GEMM_UNROLL_M}" "" "neg_tcopy" false "" "" false ${float_type})
        GenerateNamedObjects("${KERNELDIR}/${${float_char}LASWP_NCOPY}_${${float_char}GEMM_UNROLL_N}" "" "laswp_ncopy" false "" "" false ${float_type})
      endforeach()
    endif()

    if (${DYNAMIC_ARCH})
        set(SETPARAM_TARGET_DIR ${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY})
        file(READ ${CMAKE_CURRENT_SOURCE_DIR}/setparam-ref.c SETPARAM_REF_CONTENTS)
        string(REPLACE "TS" "${TSUFFIX}" SETPARAM_REF_CONTENTS_NEW "${SETPARAM_REF_CONTENTS}")
        file(WRITE ${SETPARAM_TARGET_DIR}/setparam${TSUFFIX}.tmp "${SETPARAM_REF_CONTENTS_NEW}")
        configure_file(${SETPARAM_TARGET_DIR}/setparam${TSUFFIX}.tmp ${SETPARAM_TARGET_DIR}/setparam${TSUFFIX}.c COPYONLY)
        set(OPENBLAS_SRC ${OPENBLAS_SRC} ${SETPARAM_TARGET_DIR}/setparam${TSUFFIX}.c)
        file(REMOVE ${SETPARAM_TARGET_DIR}/setparam${TSUFFIX}.tmp)

        set(KERNEL_TSUFFIX_CONTENTS "")
        foreach (KERNEL_INTERFACE_H ${KERNEL_INTERFACE})
            file(READ ${PROJECT_SOURCE_DIR}/${KERNEL_INTERFACE_H} KERNEL_INTERFACE_H_CONTENTS)
            string(REGEX REPLACE " *\\(" "${TSUFFIX}(" KERNEL_INTERFACE_H_CONTENTS_NEW "${KERNEL_INTERFACE_H_CONTENTS}")
            set(KERNEL_TSUFFIX_CONTENTS "${KERNEL_TSUFFIX_CONTENTS}\n${KERNEL_INTERFACE_H_CONTENTS_NEW}")
        endforeach()
	    file(WRITE ${SETPARAM_TARGET_DIR}/kernel${TSUFFIX}.tmp "${KERNEL_TSUFFIX_CONTENTS}")
        configure_file(${SETPARAM_TARGET_DIR}/kernel${TSUFFIX}.tmp ${SETPARAM_TARGET_DIR}/kernel${TSUFFIX}.h COPYONLY)
        file(REMOVE ${SETPARAM_TARGET_DIR}/kernel${TSUFFIX}.tmp)

    foreach (float_type ${FLOAT_TYPES})
      # a bit of metaprogramming here to pull out the appropriate KERNEL var
      string(SUBSTRING ${float_type} 0 1 float_char)
      GenerateNamedObjects("generic/neg_tcopy_${${float_char}GEMM_UNROLL_M}.c" "" "neg_tcopy" false "" ${TSUFFIX}  false ${float_type})
      GenerateNamedObjects("generic/laswp_ncopy_${${float_char}GEMM_UNROLL_N}.c" "" "laswp_ncopy" false "" ${TSUFFIX}  false ${float_type})
    endforeach ()


    endif ()

    add_library(kernel${TSUFFIX} OBJECT ${OPENBLAS_SRC})
    set_target_properties(kernel${TSUFFIX} PROPERTIES COMPILE_FLAGS "${KERNEL_DEFINITIONS}")
    get_target_property(KERNEL_INCLUDE_DIRECTORIES kernel${TSUFFIX} INCLUDE_DIRECTORIES)
    set_target_properties(kernel${TSUFFIX} PROPERTIES INCLUDE_DIRECTORIES "${KERNEL_INCLUDE_DIRECTORIES};${TARGET_CONF_DIR}")
endfunction ()


set(ADD_COMMONOBJS 1)
if (${DYNAMIC_ARCH})
  foreach(TARGET_CORE ${DYNAMIC_CORE})
      set(BUILD_KERNEL 1)
      set(KDIR "")
      set(TSUFFIX "_${TARGET_CORE}")
      set(KERNEL_DEFINITIONS "-DBUILD_KERNEL -DTABLE_NAME=gotoblas_${TARGET_CORE} -DTS=${TSUFFIX}")
      build_core("${TARGET_CORE}" "${KDIR}" "${TSUFFIX}" "${KERNEL_DEFINITIONS}")
      set(ADD_COMMONOBJS 0)
  endforeach()
else ()
  set(TARGET_CONF_DIR ${PROJECT_BINARY_DIR})
  set(TARGET_CORE ${CORE})
  set(KDIR "")
  set(TSUFFIX "")
  set(KERNEL_DEFINITIONS "")
  build_core("${TARGET_CORE}" "${KDIR}" "${TSUFFIX}" "${KERNEL_DEFINITIONS}")
endif ()

