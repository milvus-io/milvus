# Licensed under the Apache License, Version 2.0.
set(probe_env "TSAN_OPTIONS=halt_on_error=1:exitcode=66")
if(DEFINED ARCHER)
    list(APPEND probe_env "OMP_TOOL=enabled" "OMP_TOOL_LIBRARIES=${ARCHER}"
        "ARCHER_OPTIONS=verbose=1"
        "TSAN_OPTIONS=halt_on_error=1:exitcode=66:ignore_noninstrumented_modules=1")
endif()
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env --unset=LD_PRELOAD
        ${probe_env}
        "${PROBE}" "${MODE}"
    RESULT_VARIABLE result
    OUTPUT_VARIABLE output
    ERROR_VARIABLE error
    TIMEOUT 45)
set(report "${output}\n${error}")
if(DEFINED ARCHER AND NOT report MATCHES "Archer detected OpenMP application with TSan")
    message(FATAL_ERROR "Archer was not activated:\n${report}")
endif()
if(MODE STREQUAL "clean" OR MODE MATCHES "^omp-clean-")
    if(NOT result STREQUAL "0" OR report MATCHES "ThreadSanitizer:")
        message(FATAL_ERROR "Synchronized probe failed (${result}):\n${report}")
    endif()
else()
    if(NOT result STREQUAL "66" OR NOT report MATCHES "ThreadSanitizer: data race")
        message(FATAL_ERROR "Expected a TSan data race and exit 66 (${result}):\n${report}")
    endif()
    if(NOT report MATCHES "(access\\.c|access\\.cpp|openmp\\.cpp):[0-9]+")
        message(FATAL_ERROR "Missing source line information in report:\n${report}")
    endif()
    if(MODE STREQUAL "race-c" AND NOT report MATCHES "tsan_increment_c")
        message(FATAL_ERROR "Missing symbolized C access in report:\n${report}")
    elseif(MODE STREQUAL "race-cxx" AND NOT report MATCHES "tsan_increment_cpp")
        message(FATAL_ERROR "Missing symbolized C++ access in report:\n${report}")
    elseif(MODE STREQUAL "omp-race" AND NOT report MATCHES "tsan_openmp_increment")
        message(FATAL_ERROR "Missing symbolized OpenMP access in report:\n${report}")
    endif()
endif()
message(STATUS "${MODE}: sanitizer report check passed")
