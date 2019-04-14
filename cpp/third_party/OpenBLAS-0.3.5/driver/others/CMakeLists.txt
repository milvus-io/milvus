include_directories(${PROJECT_SOURCE_DIR})
include_directories(${PROJECT_BINARY_DIR})

if (${CORE} STREQUAL "PPC440")
  set(MEMORY memory_qalloc.c)
else ()
  set(MEMORY memory.c)
endif ()

if (USE_THREAD)

  if (USE_OPENMP)
    set(BLAS_SERVER blas_server_omp.c)
  elseif (${CMAKE_SYSTEM_NAME} STREQUAL "Windows")
    set(BLAS_SERVER blas_server_win32.c)
  elseif (${CMAKE_SYSTEM_NAME} STREQUAL "WindowsStore")
    set(BLAS_SERVER blas_server_win32.c)
  endif ()

  if (NOT DEFINED BLAS_SERVER)
    set(BLAS_SERVER blas_server.c)
  endif ()

  set(SMP_SOURCES
    ${BLAS_SERVER}
    divtable.c # TODO: Makefile has -UDOUBLE
    blas_l1_thread.c
  )

  if (NOT NO_AFFINITY)
    list(APPEND SMP_SOURCES init.c)
  endif ()
endif ()

set(COMMON_SOURCES
  xerbla.c
  openblas_set_num_threads.c
  openblas_error_handle.c
  openblas_env.c
  openblas_get_num_procs.c
  openblas_get_num_threads.c
)

# these need to have NAME/CNAME set, so use GenerateNamedObjects, but don't use standard name mangling
GenerateNamedObjects("abs.c" "" "c_abs" 0 "" "" 1 )
GenerateNamedObjects("abs.c" "DOUBLE" "z_abs" 0 "" "" 1)
GenerateNamedObjects("openblas_get_config.c;openblas_get_parallel.c" "" "" 0 "" "" 1)

if (DYNAMIC_ARCH)
  if (ARM64)
    list(APPEND COMMON_SOURCES dynamic_arm64.c)
  else ()  
    list(APPEND COMMON_SOURCES dynamic.c)
  endif ()  
else ()
  list(APPEND COMMON_SOURCES parameter.c)
endif ()

#ifdef EXPRECISION
#COMMONOBJS	+= x_abs.$(SUFFIX) qlamch.$(SUFFIX) qlamc3.$(SUFFIX)
#endif
#
#ifdef QUAD_PRECISION
#COMMONOBJS	+= addx.$(SUFFIX) mulx.$(SUFFIX)
#endif
#
#ifdef USE_CUDA
#COMMONOBJS	+= cuda_init.$(SUFFIX)
#endif
#
#ifdef FUNCTION_PROFILE
#COMMONOBJS	+= profile.$(SUFFIX)
#endif

#LIBOTHERS = libothers.$(LIBSUFFIX)

#ifeq ($(DYNAMIC_ARCH), 1)
#HPLOBJS = memory.$(SUFFIX) xerbla.$(SUFFIX) dynamic.$(SUFFIX)
#else
#HPLOBJS = memory.$(SUFFIX) xerbla.$(SUFFIX) parameter.$(SUFFIX)
#endif

add_library(driver_others OBJECT ${OPENBLAS_SRC} ${MEMORY} ${SMP_SOURCES} ${COMMON_SOURCES})
