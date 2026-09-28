# Licensed under the Apache License, Version 2.0.
execute_process(
    COMMAND "${CMAKE_COMMAND}" -E env --unset=LD_PRELOAD
        "TSAN_OPTIONS=halt_on_error=1:exitcode=66"
        "${PROBE}" "${MODE}"
    RESULT_VARIABLE result
    OUTPUT_VARIABLE output
    ERROR_VARIABLE error
    TIMEOUT 45)
set(report "${output}\n${error}")
if(MODE STREQUAL "clean")
    if(NOT result STREQUAL "0" OR report MATCHES "ThreadSanitizer:")
        message(FATAL_ERROR "Synchronized probe failed (${result}):\n${report}")
    endif()
else()
    if(NOT result STREQUAL "66" OR NOT report MATCHES "ThreadSanitizer: data race")
        message(FATAL_ERROR "Expected a TSan data race and exit 66 (${result}):\n${report}")
    endif()
    if(MODE STREQUAL "race-c" AND NOT report MATCHES "tsan_increment_c")
        message(FATAL_ERROR "Missing symbolized C access in report:\n${report}")
    elseif(MODE STREQUAL "race-cxx" AND NOT report MATCHES "tsan_increment_cpp")
        message(FATAL_ERROR "Missing symbolized C++ access in report:\n${report}")
    endif()
endif()
message(STATUS "${MODE}: sanitizer report check passed")
