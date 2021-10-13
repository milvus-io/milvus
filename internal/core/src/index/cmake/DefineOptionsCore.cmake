#-------------------------------------------------------------------------------
# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License.
#-------------------------------------------------------------------------------

macro(set_option_category name)
    set(KNOWHERE_OPTION_CATEGORY ${name})
    list(APPEND "KNOWHERE_OPTION_CATEGORIES" ${name})
endmacro()

macro(define_option name description default)
    option(${name} ${description} ${default})
    list(APPEND "KNOWHERE_${KNOWHERE_OPTION_CATEGORY}_OPTION_NAMES" ${name})
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
    list(APPEND "KNOWHERE_${KNOWHERE_OPTION_CATEGORY}_OPTION_NAMES" ${name})
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
set_option_category("Thirdparty")

set(KNOWHERE_DEPENDENCY_SOURCE "AUTO")

define_option_string(KNOWHERE_DEPENDENCY_SOURCE
        "Method to use for acquiring KNOWHERE's build dependencies"
        "AUTO"
        "BUNDLED"
        "SYSTEM")


define_option(KNOWHERE_USE_CCACHE "Use ccache when compiling (if available)" ON)

define_option(KNOWHERE_VERBOSE_THIRDPARTY_BUILD
        "Show output from ExternalProjects rather than just logging to files" ON)

define_option(KNOWHERE_BOOST_USE_SHARED "Rely on boost shared libraries where relevant" OFF)

define_option(KNOWHERE_BOOST_VENDORED "Use vendored Boost instead of existing Boost. \
Note that this requires linking Boost statically" OFF)

define_option(KNOWHERE_BOOST_HEADER_ONLY "Use only BOOST headers" OFF)

define_option(KNOWHERE_WITH_ARROW "Build with ARROW" OFF)

define_option(KNOWHERE_WITH_OPENBLAS "Build with OpenBLAS library" ON)

define_option(KNOWHERE_WITH_FAISS "Build with FAISS library" ON)

define_option(KNOWHERE_WITH_FAISS_GPU_VERSION "Build with FAISS GPU version" ON)

define_option(FAISS_WITH_MKL "Build FAISS with MKL" OFF)

define_option(MILVUS_CUDA_ARCH "Build with CUDA arch" "DEFAULT")

#----------------------------------------------------------------------
set_option_category("Test and benchmark")

#----------------------------------------------------------------------
macro(config_summary)
    message(STATUS "---------------------------------------------------------------------")
    message(STATUS "KNOWHERE version:                                 ${KNOWHERE_VERSION}")
    message(STATUS)
    message(STATUS "Build configuration summary:")

    message(STATUS "  Generator: ${CMAKE_GENERATOR}")
    message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")
    message(STATUS "  Source directory: ${CMAKE_CURRENT_SOURCE_DIR}")
    if (${CMAKE_EXPORT_COMPILE_COMMANDS})
        message(
                STATUS "  Compile commands: ${CMAKE_CURRENT_BINARY_DIR}/compile_commands.json")
    endif ()

    foreach (category ${KNOWHERE_OPTION_CATEGORIES})

        message(STATUS)
        message(STATUS "${category} options:")

        set(option_names ${KNOWHERE_${category}_OPTION_NAMES})

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
