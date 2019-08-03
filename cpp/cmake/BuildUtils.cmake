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
            ${CMAKE_COMMAND} -E tar xzvf ${package_file} ${install_path}
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
