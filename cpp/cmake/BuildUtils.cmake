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