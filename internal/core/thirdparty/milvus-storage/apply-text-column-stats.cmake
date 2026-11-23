# FetchContent runs PATCH_COMMAND in the dependency source directory and may
# rerun it on configure. Never reset local changes to make a patch apply.
find_package(Git REQUIRED)
set(patch "${CMAKE_CURRENT_LIST_DIR}/text-column-stats.patch")
execute_process(COMMAND "${GIT_EXECUTABLE}" apply --reverse --check "${patch}"
                RESULT_VARIABLE already_applied OUTPUT_QUIET ERROR_QUIET)
if(NOT already_applied EQUAL 0)
    execute_process(COMMAND "${GIT_EXECUTABLE}" apply "${patch}"
                    RESULT_VARIABLE patch_result)
    if(NOT patch_result EQUAL 0)
        message(FATAL_ERROR "Cannot apply the milvus-storage TEXT column statistics patch (git exit ${patch_result})")
    endif()
endif()
