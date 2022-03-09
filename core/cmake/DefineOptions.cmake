
macro(set_option_category name)
    set(MILVUS_OPTION_CATEGORY ${name})
    list(APPEND "MILVUS_OPTION_CATEGORIES" ${name})
endmacro()

macro(define_option name description default)
    option(${name} ${description} ${default})
    list(APPEND "MILVUS_${MILVUS_OPTION_CATEGORY}_OPTION_NAMES" ${name})
    set("${name}_OPTION_DESCRIPTION" ${description})
    set("${name}_OPTION_DEFAULT" ${default})
    set("${name}_OPTION_TYPE" "bool")
endmacro()

function(list_join lst glue out)
    if ("${${lst}}" STREQUAL "")
        set(${out} "" PARENT_SCOPE)
        return()
    endif ()

    list(GET ${lst} 0 joined)
    list(REMOVE_AT ${lst} 0)
    foreach (item ${${lst}})
        set(joined "${joined}${glue}${item}")
    endforeach ()
    set(${out} ${joined} PARENT_SCOPE)
endfunction()

macro(define_option_string name description default)
    set(${name} ${default} CACHE STRING ${description})
    list(APPEND "MILVUS_${MILVUS_OPTION_CATEGORY}_OPTION_NAMES" ${name})
    set("${name}_OPTION_DESCRIPTION" ${description})
    set("${name}_OPTION_DEFAULT" "\"${default}\"")
    set("${name}_OPTION_TYPE" "string")

    set("${name}_OPTION_ENUM" ${ARGN})
    list_join("${name}_OPTION_ENUM" "|" "${name}_OPTION_ENUM")
    if (NOT ("${${name}_OPTION_ENUM}" STREQUAL ""))
        set_property(CACHE ${name} PROPERTY STRINGS ${ARGN})
    endif ()
endmacro()

#----------------------------------------------------------------------
set_option_category("Milvus Build Option")

define_option(MILVUS_GPU_VERSION "Build GPU version" OFF)

define_option(MILVUS_FPGA_VERSION "Build FPGA version" OFF)

define_option(MILVUS_APU_VERSION "Build APU version" OFF)

#----------------------------------------------------------------------
set_option_category("Thirdparty")

set(MILVUS_DEPENDENCY_SOURCE_DEFAULT "BUNDLED")

define_option_string(MILVUS_DEPENDENCY_SOURCE
        "Method to use for acquiring MILVUS's build dependencies"
        "${MILVUS_DEPENDENCY_SOURCE_DEFAULT}"
        "AUTO"
        "BUNDLED"
        "SYSTEM")

define_option(MILVUS_USE_CCACHE "Use ccache when compiling (if available)" ON)

define_option(MILVUS_VERBOSE_THIRDPARTY_BUILD
        "Show output from ExternalProjects rather than just logging to files" ON)

define_option(MILVUS_WITH_EASYLOGGINGPP "Build with Easylogging++ library" ON)

define_option(MILVUS_WITH_PROMETHEUS "Build with PROMETHEUS library" ON)

define_option(MILVUS_WITH_SQLITE "Build with SQLite library" ON)

define_option(MILVUS_WITH_MYSQLPP "Build with MySQL++" ON)

define_option(MILVUS_WITH_YAMLCPP "Build with yaml-cpp library" ON)

if (ENABLE_CPU_PROFILING STREQUAL "ON")
    define_option(MILVUS_WITH_LIBUNWIND "Build with libunwind" ON)
    define_option(MILVUS_WITH_GPERFTOOLS "Build with gperftools" ON)
endif ()

define_option(MILVUS_WITH_GRPC "Build with GRPC" ON)

define_option(MILVUS_WITH_ZLIB "Build with zlib compression" ON)

define_option(MILVUS_WITH_OPENTRACING "Build with Opentracing" ON)

define_option(MILVUS_WITH_FIU "Build with fiu" OFF)

define_option(MILVUS_WITH_AWS "Build with aws" ON)

define_option(MILVUS_WITH_OSS "Build with oss" ON)

define_option(MILVUS_WITH_OATPP "Build with oatpp" ON)

#----------------------------------------------------------------------
set_option_category("Test and benchmark")

unset(MILVUS_BUILD_TESTS CACHE)
if (BUILD_UNIT_TEST)
    define_option(MILVUS_BUILD_TESTS "Build the MILVUS googletest unit tests" ON)
else ()
    define_option(MILVUS_BUILD_TESTS "Build the MILVUS googletest unit tests" OFF)
endif (BUILD_UNIT_TEST)

#----------------------------------------------------------------------
macro(config_summary)
    message(STATUS "---------------------------------------------------------------------")
    message(STATUS "MILVUS version:                                 ${MILVUS_VERSION}")
    message(STATUS)
    message(STATUS "Build configuration summary:")

    message(STATUS "  Generator: ${CMAKE_GENERATOR}")
    message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")
    message(STATUS "  Source directory: ${CMAKE_CURRENT_SOURCE_DIR}")
    if (${CMAKE_EXPORT_COMPILE_COMMANDS})
        message(
                STATUS "  Compile commands: ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json")
    endif ()

    foreach (category ${MILVUS_OPTION_CATEGORIES})

        message(STATUS)
        message(STATUS "${category} options:")

        set(option_names ${MILVUS_${category}_OPTION_NAMES})

        set(max_value_length 0)
        foreach (name ${option_names})
            string(LENGTH "\"${${name}}\"" value_length)
            if (${max_value_length} LESS ${value_length})
                set(max_value_length ${value_length})
            endif ()
        endforeach ()

        foreach (name ${option_names})
            if ("${${name}_OPTION_TYPE}" STREQUAL "string")
                set(value "\"${${name}}\"")
            else ()
                set(value "${${name}}")
            endif ()

            set(default ${${name}_OPTION_DEFAULT})
            set(description ${${name}_OPTION_DESCRIPTION})
            string(LENGTH ${description} description_length)
            if (${description_length} LESS 70)
                string(
                        SUBSTRING
                        "                                                                     "
                        ${description_length} -1 description_padding)
            else ()
                set(description_padding "
                ")
            endif ()

            set(comment "[${name}]")

            if ("${value}" STREQUAL "${default}")
                set(comment "[default] ${comment}")
            endif ()

            if (NOT ("${${name}_OPTION_ENUM}" STREQUAL ""))
                set(comment "${comment} [${${name}_OPTION_ENUM}]")
            endif ()

            string(
                    SUBSTRING "${value}                                                             "
                    0 ${max_value_length} value)

            message(STATUS "  ${description} ${description_padding} ${value} ${comment}")
        endforeach ()

    endforeach ()

endmacro()
