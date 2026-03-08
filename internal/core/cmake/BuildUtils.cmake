# Define a function that check last file modification
function(Check_Last_Modify cache_check_lists_file_path working_dir last_modified_commit_id)
    if(EXISTS "${working_dir}")
        if(EXISTS "${cache_check_lists_file_path}")
            set(GIT_LOG_SKIP_NUM 0)
            set(_MATCH_ALL ON CACHE BOOL "Match all")
            set(_LOOP_STATUS ON CACHE BOOL "Whether out of loop")
            file(STRINGS ${cache_check_lists_file_path} CACHE_IGNORE_TXT)
            while(_LOOP_STATUS)
                foreach(_IGNORE_ENTRY ${CACHE_IGNORE_TXT})
                    if(NOT _IGNORE_ENTRY MATCHES "^[^#]+")
                        continue()
                    endif()

                    set(_MATCH_ALL OFF)
                    execute_process(COMMAND git log --no-merges -1 --skip=${GIT_LOG_SKIP_NUM} --name-status --pretty= WORKING_DIRECTORY ${working_dir} OUTPUT_VARIABLE CHANGE_FILES)
                    if(NOT CHANGE_FILES STREQUAL "")
                        string(REPLACE "\n" ";" _CHANGE_FILES ${CHANGE_FILES})
                        foreach(_FILE_ENTRY ${_CHANGE_FILES})
                            string(REGEX MATCH "[^ \t]+$" _FILE_NAME ${_FILE_ENTRY})
                            execute_process(COMMAND sh -c "echo ${_FILE_NAME} | grep ${_IGNORE_ENTRY}" RESULT_VARIABLE return_code)
                            if (return_code EQUAL 0)
                                execute_process(COMMAND git log --no-merges -1 --skip=${GIT_LOG_SKIP_NUM} --pretty=%H WORKING_DIRECTORY ${working_dir} OUTPUT_VARIABLE LAST_MODIFIED_COMMIT_ID)
                                set (${last_modified_commit_id} ${LAST_MODIFIED_COMMIT_ID} PARENT_SCOPE)
                                set(_LOOP_STATUS OFF)
                            endif()
                        endforeach()
                    else()
                        set(_LOOP_STATUS OFF)
                    endif()
                endforeach()

                if(_MATCH_ALL)
                    execute_process(COMMAND git log --no-merges -1 --skip=${GIT_LOG_SKIP_NUM} --pretty=%H WORKING_DIRECTORY ${working_dir} OUTPUT_VARIABLE LAST_MODIFIED_COMMIT_ID)
                    set (${last_modified_commit_id} ${LAST_MODIFIED_COMMIT_ID} PARENT_SCOPE)
                    set(_LOOP_STATUS OFF)
                endif()

                math(EXPR GIT_LOG_SKIP_NUM "${GIT_LOG_SKIP_NUM} + 1")
            endwhile(_LOOP_STATUS)
        else()
            execute_process(COMMAND git log --no-merges -1 --skip=${GIT_LOG_SKIP_NUM} --pretty=%H WORKING_DIRECTORY ${working_dir} OUTPUT_VARIABLE LAST_MODIFIED_COMMIT_ID)
            set (${last_modified_commit_id} ${LAST_MODIFIED_COMMIT_ID} PARENT_SCOPE)
        endif()
    else()
        message(FATAL_ERROR "The directory ${working_dir} does not exist")
    endif()
endfunction()

# Define a function that extracts a cached package
function(ExternalProject_Use_Cache project_name package_file install_path)
    message(STATUS "Will use cached package file: ${package_file}")

    ExternalProject_Add(${project_name}
        DOWNLOAD_COMMAND ${CMAKE_COMMAND} -E echo
            "No download step needed (using cached package)"
        CONFIGURE_COMMAND ${CMAKE_COMMAND} -E echo
            "No configure step needed (using cached package)"
        BUILD_COMMAND ${CMAKE_COMMAND} -E echo
            "No build step needed (using cached package)"
        INSTALL_COMMAND ${CMAKE_COMMAND} -E echo
            "No install step needed (using cached package)"
    )

    # We want our tar files to contain the Install/<package> prefix (not for any
    # very special reason, only for consistency and so that we can identify them
    # in the extraction logs) which means that we must extract them in the
    # binary (top-level build) directory to have them installed in the right
    # place for subsequent ExternalProjects to pick them up. It seems that the
    # only way to control the working directory is with Add_Step!
    ExternalProject_Add_Step(${project_name} extract
        ALWAYS 1
        COMMAND
            ${CMAKE_COMMAND} -E echo
            "Extracting ${package_file} to ${install_path}"
        COMMAND
            ${CMAKE_COMMAND} -E tar xzf ${package_file} ${install_path}
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
    )

    ExternalProject_Add_StepTargets(${project_name} extract)
endfunction()

# Define a function that to create a new cached package
function(ExternalProject_Create_Cache project_name package_file install_path cache_username cache_password cache_path)
    if(EXISTS ${package_file})
        message(STATUS "Removing existing package file: ${package_file}")
        file(REMOVE ${package_file})
    endif()

    string(REGEX REPLACE "(.+)/.+$" "\\1" package_dir ${package_file})
    if(NOT EXISTS ${package_dir})
        file(MAKE_DIRECTORY ${package_dir})
    endif()

    message(STATUS "Will create cached package file: ${package_file}")

    ExternalProject_Add_Step(${project_name} package
        DEPENDEES install
        BYPRODUCTS ${package_file}
        COMMAND ${CMAKE_COMMAND} -E echo "Updating cached package file: ${package_file}"
        COMMAND ${CMAKE_COMMAND} -E tar czvf ${package_file} ${install_path}
        COMMAND ${CMAKE_COMMAND} -E echo "Uploading package file ${package_file} to ${cache_path}"
        COMMAND curl -u${cache_username}:${cache_password} -T ${package_file} ${cache_path}
    )

    ExternalProject_Add_StepTargets(${project_name} package)
endfunction()

function(ADD_THIRDPARTY_LIB LIB_NAME)
    set(options)
    set(one_value_args SHARED_LIB STATIC_LIB)
    set(multi_value_args DEPS INCLUDE_DIRECTORIES)
    cmake_parse_arguments(ARG
            "${options}"
            "${one_value_args}"
            "${multi_value_args}"
            ${ARGN})
    if(ARG_UNPARSED_ARGUMENTS)
        message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
    endif()

    if(ARG_STATIC_LIB AND ARG_SHARED_LIB)
        if(NOT ARG_STATIC_LIB)
            message(FATAL_ERROR "No static or shared library provided for ${LIB_NAME}")
        endif()

        set(AUG_LIB_NAME "${LIB_NAME}_static")
        add_library(${AUG_LIB_NAME} STATIC IMPORTED)
        set_target_properties(${AUG_LIB_NAME}
                PROPERTIES IMPORTED_LOCATION "${ARG_STATIC_LIB}")
        if(ARG_DEPS)
            set_target_properties(${AUG_LIB_NAME}
                    PROPERTIES INTERFACE_LINK_LIBRARIES "${ARG_DEPS}")
        endif()
        message(STATUS "Added static library dependency ${AUG_LIB_NAME}: ${ARG_STATIC_LIB}")
        if(ARG_INCLUDE_DIRECTORIES)
            set_target_properties(${AUG_LIB_NAME}
                    PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                    "${ARG_INCLUDE_DIRECTORIES}")
        endif()

        set(AUG_LIB_NAME "${LIB_NAME}_shared")
        add_library(${AUG_LIB_NAME} SHARED IMPORTED)

        if(WIN32)
            # Mark the ".lib" location as part of a Windows DLL
            set_target_properties(${AUG_LIB_NAME}
                    PROPERTIES IMPORTED_IMPLIB "${ARG_SHARED_LIB}")
        else()
            set_target_properties(${AUG_LIB_NAME}
                    PROPERTIES IMPORTED_LOCATION "${ARG_SHARED_LIB}")
        endif()
        if(ARG_DEPS)
            set_target_properties(${AUG_LIB_NAME}
                    PROPERTIES INTERFACE_LINK_LIBRARIES "${ARG_DEPS}")
        endif()
        message(STATUS "Added shared library dependency ${AUG_LIB_NAME}: ${ARG_SHARED_LIB}")
        if(ARG_INCLUDE_DIRECTORIES)
            set_target_properties(${AUG_LIB_NAME}
                    PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                    "${ARG_INCLUDE_DIRECTORIES}")
        endif()
    elseif(ARG_STATIC_LIB)
        set(AUG_LIB_NAME "${LIB_NAME}_static")
        add_library(${AUG_LIB_NAME} STATIC IMPORTED)
        set_target_properties(${AUG_LIB_NAME}
                PROPERTIES IMPORTED_LOCATION "${ARG_STATIC_LIB}")
        if(ARG_DEPS)
            set_target_properties(${AUG_LIB_NAME}
                    PROPERTIES INTERFACE_LINK_LIBRARIES "${ARG_DEPS}")
        endif()
        message(STATUS "Added static library dependency ${AUG_LIB_NAME}: ${ARG_STATIC_LIB}")
        if(ARG_INCLUDE_DIRECTORIES)
            set_target_properties(${AUG_LIB_NAME}
                    PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                    "${ARG_INCLUDE_DIRECTORIES}")
        endif()
    elseif(ARG_SHARED_LIB)
        set(AUG_LIB_NAME "${LIB_NAME}_shared")
        add_library(${AUG_LIB_NAME} SHARED IMPORTED)

        if(WIN32)
            # Mark the ".lib" location as part of a Windows DLL
            set_target_properties(${AUG_LIB_NAME}
                    PROPERTIES IMPORTED_IMPLIB "${ARG_SHARED_LIB}")
        else()
            set_target_properties(${AUG_LIB_NAME}
                    PROPERTIES IMPORTED_LOCATION "${ARG_SHARED_LIB}")
        endif()
        message(STATUS "Added shared library dependency ${AUG_LIB_NAME}: ${ARG_SHARED_LIB}")
        if(ARG_DEPS)
            set_target_properties(${AUG_LIB_NAME}
                    PROPERTIES INTERFACE_LINK_LIBRARIES "${ARG_DEPS}")
        endif()
        if(ARG_INCLUDE_DIRECTORIES)
            set_target_properties(${AUG_LIB_NAME}
                    PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                    "${ARG_INCLUDE_DIRECTORIES}")
        endif()
    else()
        message(FATAL_ERROR "No static or shared library provided for ${LIB_NAME}")
    endif()
endfunction()

function(MILVUS_ADD_PKG_CONFIG MODULE)
    configure_file(${MODULE}.pc.in "${CMAKE_CURRENT_BINARY_DIR}/${MODULE}.pc" @ONLY)
    install(FILES "${CMAKE_CURRENT_BINARY_DIR}/${MODULE}.pc"
          DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig/")
endfunction()

MACRO (import_mysql_inc)
    find_path (MYSQL_INCLUDE_DIR
        NAMES "mysql.h"
        PATH_SUFFIXES "mysql")

    if (${MYSQL_INCLUDE_DIR} STREQUAL "MYSQL_INCLUDE_DIR-NOTFOUND")
        message(FATAL_ERROR "Could not found MySQL include directory")
    else ()
        include_directories(${MYSQL_INCLUDE_DIR})
    endif ()
ENDMACRO (import_mysql_inc)

MACRO(using_ccache_if_defined MILVUS_USE_CCACHE)
    if (MILVUS_USE_CCACHE)
        find_program(CCACHE_FOUND ccache)
        if (CCACHE_FOUND)
            message(STATUS "Using ccache: ${CCACHE_FOUND}")
            set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ${CCACHE_FOUND})
            set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ${CCACHE_FOUND})
            # let ccache preserve C++ comments, because some of them may be
            # meaningful to the compiler
            set(ENV{CCACHE_COMMENTS} "1")
        else()
            message(WARNING "ccache not found!")
        endif()
    endif ()
ENDMACRO(using_ccache_if_defined)

