#
# ucm.cmake - useful cmake macros
#
# Copyright (c) 2016 Viktor Kirilov
#
# Distributed under the MIT Software License
# See accompanying file LICENSE.txt or copy at
# https://opensource.org/licenses/MIT
#
# The documentation can be found at the library's page:
# https://github.com/onqtam/ucm

cmake_minimum_required(VERSION 2.8.12)

include(CMakeParseArguments)

# optionally include cotire - the git submodule might not be inited (or the user might have already included it)
if(NOT COMMAND cotire)
    include(${CMAKE_CURRENT_LIST_DIR}/../cotire/CMake/cotire.cmake OPTIONAL)
endif()

if(COMMAND cotire AND "1.7.9" VERSION_LESS "${COTIRE_CMAKE_MODULE_VERSION}")
    set(ucm_with_cotire 1)
else()
    set(ucm_with_cotire 0)
endif()

option(UCM_UNITY_BUILD          "Enable unity build for targets registered with the ucm_add_target() macro"                     OFF)
option(UCM_NO_COTIRE_FOLDER     "Do not use a cotire folder in the solution explorer for all unity and cotire related targets"  ON)

# ucm_add_flags
# Adds compiler flags to CMAKE_<LANG>_FLAGS or to a specific config
macro(ucm_add_flags)
    cmake_parse_arguments(ARG "C;CXX;CLEAR_OLD" "" "CONFIG" ${ARGN})

    if(NOT ARG_CONFIG)
        set(ARG_CONFIG " ")
    endif()

    foreach(CONFIG ${ARG_CONFIG})
        # determine to which flags to add
        if(NOT ${CONFIG} STREQUAL " ")
            string(TOUPPER ${CONFIG} CONFIG)
            set(CXX_FLAGS CMAKE_CXX_FLAGS_${CONFIG})
            set(C_FLAGS CMAKE_C_FLAGS_${CONFIG})
        else()
            set(CXX_FLAGS CMAKE_CXX_FLAGS)
            set(C_FLAGS CMAKE_C_FLAGS)
        endif()

        # clear the old flags
        if(${ARG_CLEAR_OLD})
            if("${ARG_CXX}" OR NOT "${ARG_C}")
                set(${CXX_FLAGS} "")
            endif()
            if("${ARG_C}" OR NOT "${ARG_CXX}")
                set(${C_FLAGS} "")
            endif()
        endif()

        # add all the passed flags
        foreach(flag ${ARG_UNPARSED_ARGUMENTS})
            if("${ARG_CXX}" OR NOT "${ARG_C}")
                set(${CXX_FLAGS} "${${CXX_FLAGS}} ${flag}")
            endif()
            if("${ARG_C}" OR NOT "${ARG_CXX}")
                set(${C_FLAGS} "${${C_FLAGS}} ${flag}")
            endif()
        endforeach()
    endforeach()

endmacro()

# ucm_set_flags
# Sets the CMAKE_<LANG>_FLAGS compiler flags or for a specific config
macro(ucm_set_flags)
    ucm_add_flags(CLEAR_OLD ${ARGN})
endmacro()

# ucm_add_linker_flags
# Adds linker flags to CMAKE_<TYPE>_LINKER_FLAGS or to a specific config
macro(ucm_add_linker_flags)
    cmake_parse_arguments(ARG "CLEAR_OLD;EXE;MODULE;SHARED;STATIC" "" "CONFIG" ${ARGN})

    if(NOT ARG_CONFIG)
        set(ARG_CONFIG " ")
    endif()

    foreach(CONFIG ${ARG_CONFIG})
        string(TOUPPER "${CONFIG}" CONFIG)
    
        if(NOT ${ARG_EXE} AND NOT ${ARG_MODULE} AND NOT ${ARG_SHARED} AND NOT ${ARG_STATIC})
            set(ARG_EXE 1)
            set(ARG_MODULE 1)
            set(ARG_SHARED 1)
            set(ARG_STATIC 1)
        endif()
    
        set(flags_configs "")
        if(${ARG_EXE})
            if(NOT "${CONFIG}" STREQUAL " ")
                list(APPEND flags_configs CMAKE_EXE_LINKER_FLAGS_${CONFIG})
            else()
                list(APPEND flags_configs CMAKE_EXE_LINKER_FLAGS)
            endif()
        endif()
        if(${ARG_MODULE})
            if(NOT "${CONFIG}" STREQUAL " ")
                list(APPEND flags_configs CMAKE_MODULE_LINKER_FLAGS_${CONFIG})
            else()
                list(APPEND flags_configs CMAKE_MODULE_LINKER_FLAGS)
            endif()
        endif()
        if(${ARG_SHARED})
            if(NOT "${CONFIG}" STREQUAL " ")
                list(APPEND flags_configs CMAKE_SHARED_LINKER_FLAGS_${CONFIG})
            else()
                list(APPEND flags_configs CMAKE_SHARED_LINKER_FLAGS)
            endif()
        endif()
        if(${ARG_STATIC})
            if(NOT "${CONFIG}" STREQUAL " ")
                list(APPEND flags_configs CMAKE_STATIC_LINKER_FLAGS_${CONFIG})
            else()
                list(APPEND flags_configs CMAKE_STATIC_LINKER_FLAGS)
            endif()
        endif()
    
        # clear the old flags
        if(${ARG_CLEAR_OLD})
            foreach(flags ${flags_configs})
                set(${flags} "")
            endforeach()
        endif()

        # add all the passed flags
        foreach(flag ${ARG_UNPARSED_ARGUMENTS})
            foreach(flags ${flags_configs})
                set(${flags} "${${flags}} ${flag}")
            endforeach()
        endforeach()
    endforeach()
endmacro()

# ucm_set_linker_flags
# Sets the CMAKE_<TYPE>_LINKER_FLAGS linker flags or for a specific config
macro(ucm_set_linker_flags)
    ucm_add_linker_flags(CLEAR_OLD ${ARGN})
endmacro()

# ucm_gather_flags
# Gathers all lists of flags for printing or manipulation
macro(ucm_gather_flags with_linker result)
    set(${result} "")
    # add the main flags without a config
    list(APPEND ${result} CMAKE_C_FLAGS)
    list(APPEND ${result} CMAKE_CXX_FLAGS)
    if(${with_linker})
        list(APPEND ${result} CMAKE_EXE_LINKER_FLAGS)
        list(APPEND ${result} CMAKE_MODULE_LINKER_FLAGS)
        list(APPEND ${result} CMAKE_SHARED_LINKER_FLAGS)
        list(APPEND ${result} CMAKE_STATIC_LINKER_FLAGS)
    endif()
    
    if("${CMAKE_CONFIGURATION_TYPES}" STREQUAL "" AND NOT "${CMAKE_BUILD_TYPE}" STREQUAL "")
        # handle single config generators - like makefiles/ninja - when CMAKE_BUILD_TYPE is set
        string(TOUPPER ${CMAKE_BUILD_TYPE} config)
        list(APPEND ${result} CMAKE_C_FLAGS_${config})
        list(APPEND ${result} CMAKE_CXX_FLAGS_${config})
        if(${with_linker})
            list(APPEND ${result} CMAKE_EXE_LINKER_FLAGS_${config})
            list(APPEND ${result} CMAKE_MODULE_LINKER_FLAGS_${config})
            list(APPEND ${result} CMAKE_SHARED_LINKER_FLAGS_${config})
            list(APPEND ${result} CMAKE_STATIC_LINKER_FLAGS_${config})
        endif()
    else()
        # handle multi config generators (like msvc, xcode)
        foreach(config ${CMAKE_CONFIGURATION_TYPES})
            string(TOUPPER ${config} config)
            list(APPEND ${result} CMAKE_C_FLAGS_${config})
            list(APPEND ${result} CMAKE_CXX_FLAGS_${config})
            if(${with_linker})
                list(APPEND ${result} CMAKE_EXE_LINKER_FLAGS_${config})
                list(APPEND ${result} CMAKE_MODULE_LINKER_FLAGS_${config})
                list(APPEND ${result} CMAKE_SHARED_LINKER_FLAGS_${config})
                list(APPEND ${result} CMAKE_STATIC_LINKER_FLAGS_${config})
            endif()
        endforeach()
    endif()
endmacro()

# ucm_set_runtime
# Sets the runtime (static/dynamic) for msvc/gcc
macro(ucm_set_runtime)
    cmake_parse_arguments(ARG "STATIC;DYNAMIC" "" "" ${ARGN})

    if(ARG_UNPARSED_ARGUMENTS)
        message(FATAL_ERROR "unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
    endif()
    
    if(CMAKE_CXX_COMPILER_ID MATCHES "Clang" STREQUAL "")
        message(AUTHOR_WARNING "ucm_set_runtime() does not support clang yet!")
    endif()
    
    ucm_gather_flags(0 flags_configs)
    
    # add/replace the flags
    # note that if the user has messed with the flags directly this function might fail
    # - for example if with MSVC and the user has removed the flags - here we just switch/replace them
    if("${ARG_STATIC}")
        foreach(flags ${flags_configs})
            if(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
                if(NOT ${flags} MATCHES "-static-libstdc\\+\\+")
                    set(${flags} "${${flags}} -static-libstdc++")
                endif()
                if(NOT ${flags} MATCHES "-static-libgcc")
                    set(${flags} "${${flags}} -static-libgcc")
                endif()
            elseif(MSVC)
                if(${flags} MATCHES "/MD")
                    string(REGEX REPLACE "/MD" "/MT" ${flags} "${${flags}}")
                endif()
            endif()
        endforeach()
    elseif("${ARG_DYNAMIC}")
        foreach(flags ${flags_configs})
            if(CMAKE_CXX_COMPILER_ID MATCHES "GNU")
                if(${flags} MATCHES "-static-libstdc\\+\\+")
                    string(REGEX REPLACE "-static-libstdc\\+\\+" "" ${flags} "${${flags}}")
                endif()
                if(${flags} MATCHES "-static-libgcc")
                    string(REGEX REPLACE "-static-libgcc" "" ${flags} "${${flags}}")
                endif()
            elseif(MSVC)
                if(${flags} MATCHES "/MT")
                    string(REGEX REPLACE "/MT" "/MD" ${flags} "${${flags}}")
                endif()
            endif()
        endforeach()
    endif()
endmacro()

# ucm_print_flags
# Prints all compiler flags for all configurations
macro(ucm_print_flags)
    ucm_gather_flags(1 flags_configs)
    message("")
    foreach(flags ${flags_configs})
        message("${flags}: ${${flags}}")
    endforeach()
    message("")
endmacro()

# ucm_set_xcode_attrib
# Set xcode attributes - name value CONFIG config1 conifg2..
macro(ucm_set_xcode_attrib)
    cmake_parse_arguments(ARG "" "CLEAR" "CONFIG" ${ARGN})

    if(NOT ARG_CONFIG)
        set(ARG_CONFIG " ")
    endif()

    foreach(CONFIG ${ARG_CONFIG})
        # determine to which attributes to add
        if(${CONFIG} STREQUAL " ")
            if(${ARG_CLEAR})
                # clear the old flags
                unset(CMAKE_XCODE_ATTRIBUTE_${ARGV0})
            else()
                set(CMAKE_XCODE_ATTRIBUTE_${ARGV0} ${ARGV1})
            endif()
        else()
            if(${ARG_CLEAR})
                # clear the old flags
                unset(CMAKE_XCODE_ATTRIBUTE_${ARGV0}[variant=${CONFIG}])
            else()
                set(CMAKE_XCODE_ATTRIBUTE_${ARGV0}[variant=${CONFIG}] ${ARGV1})
            endif()
        endif()
    endforeach()
endmacro()

# ucm_count_sources
# Counts the number of source files
macro(ucm_count_sources)
    cmake_parse_arguments(ARG "" "RESULT" "" ${ARGN})
    if(${ARG_RESULT} STREQUAL "")
        message(FATAL_ERROR "Need to pass RESULT and a variable name to ucm_count_sources()")
    endif()
    
    set(result 0)
    foreach(SOURCE_FILE ${ARG_UNPARSED_ARGUMENTS})
        if("${SOURCE_FILE}" MATCHES \\.\(c|C|cc|cp|cpp|CPP|c\\+\\+|cxx|i|ii\)$)
            math(EXPR result "${result} + 1")
        endif()
    endforeach()
    set(${ARG_RESULT} ${result})
endmacro()

# ucm_include_file_in_sources
# Includes the file to the source with compiler flags
macro(ucm_include_file_in_sources)
    cmake_parse_arguments(ARG "" "HEADER" "" ${ARGN})
    if(${ARG_HEADER} STREQUAL "")
        message(FATAL_ERROR "Need to pass HEADER and a header file to ucm_include_file_in_sources()")
    endif()
    
    foreach(src ${ARG_UNPARSED_ARGUMENTS})
        if(${src} MATCHES \\.\(c|C|cc|cp|cpp|CPP|c\\+\\+|cxx\)$)
            # get old flags
            get_source_file_property(old_compile_flags ${src} COMPILE_FLAGS)
            if(old_compile_flags STREQUAL "NOTFOUND")
                set(old_compile_flags "")
            endif()
            
            # update flags
            if(MSVC)
                set_source_files_properties(${src} PROPERTIES COMPILE_FLAGS
                    "${old_compile_flags} /FI\"${CMAKE_CURRENT_SOURCE_DIR}/${ARG_HEADER}\"")
            else()
                set_source_files_properties(${src} PROPERTIES COMPILE_FLAGS
                    "${old_compile_flags} -include \"${CMAKE_CURRENT_SOURCE_DIR}/${ARG_HEADER}\"")
            endif()
        endif()
    endforeach()
endmacro()

# ucm_dir_list
# Returns a list of subdirectories for a given directory
macro(ucm_dir_list thedir result)
    file(GLOB sub-dir "${thedir}/*")
    set(list_of_dirs "")
    foreach(dir ${sub-dir})
        if(IS_DIRECTORY ${dir})
            get_filename_component(DIRNAME ${dir} NAME)
            LIST(APPEND list_of_dirs ${DIRNAME})
        endif()
    endforeach()
    set(${result} ${list_of_dirs})
endmacro()

# ucm_trim_front_words
# Trims X times the front word from a string separated with "/" and removes
# the front "/" characters after that (used for filters for visual studio)
macro(ucm_trim_front_words source out num_filter_trims)
    set(result "${source}")
    set(counter 0)
    while(${counter} LESS ${num_filter_trims})
        MATH(EXPR counter "${counter} + 1")
        # removes everything at the front up to a "/" character
        string(REGEX REPLACE "^([^/]+)" "" result "${result}")
        # removes all consecutive "/" characters from the front
        string(REGEX REPLACE "^(/+)" "" result "${result}")
    endwhile()
    set(${out} ${result})
endmacro()

# ucm_remove_files
# Removes source files from a list of sources (path is the relative path for it to be found)
macro(ucm_remove_files)
    cmake_parse_arguments(ARG "" "FROM" "" ${ARGN})
    
    if("${ARG_UNPARSED_ARGUMENTS}" STREQUAL "")
        message(FATAL_ERROR "Need to pass some relative files to ucm_remove_files()")
    endif()
    if(${ARG_FROM} STREQUAL "")
        message(FATAL_ERROR "Need to pass FROM and a variable name to ucm_remove_files()")
    endif()
    
    foreach(cur_file ${ARG_UNPARSED_ARGUMENTS})
        list(REMOVE_ITEM ${ARG_FROM} ${cur_file})
    endforeach()
endmacro()

# ucm_remove_directories
# Removes all source files from the given directories from the sources list
macro(ucm_remove_directories)
    cmake_parse_arguments(ARG "" "FROM" "MATCHES" ${ARGN})
    
    if("${ARG_UNPARSED_ARGUMENTS}" STREQUAL "")
        message(FATAL_ERROR "Need to pass some relative directories to ucm_remove_directories()")
    endif()
    if(${ARG_FROM} STREQUAL "")
        message(FATAL_ERROR "Need to pass FROM and a variable name to ucm_remove_directories()")
    endif()
    
    foreach(cur_dir ${ARG_UNPARSED_ARGUMENTS})
        foreach(cur_file ${${ARG_FROM}})
            string(REGEX MATCH ${cur_dir} res ${cur_file})
            if(NOT "${res}" STREQUAL "")
                if("${ARG_MATCHES}" STREQUAL "")
                    list(REMOVE_ITEM ${ARG_FROM} ${cur_file})
                else()
                    foreach(curr_ptrn ${ARG_MATCHES})
                        string(REGEX MATCH ${curr_ptrn} res ${cur_file})
                        if(NOT "${res}" STREQUAL "")
                            list(REMOVE_ITEM ${ARG_FROM} ${cur_file})
                            break()
                        endif()
                    endforeach()
                endif()
            endif()
        endforeach()
    endforeach()
endmacro()

# ucm_add_files_impl
macro(ucm_add_files_impl result trim files)
    foreach(cur_file ${files})
        SET(${result} ${${result}} ${cur_file})
        get_filename_component(FILEPATH ${cur_file} PATH)
        ucm_trim_front_words("${FILEPATH}" FILEPATH "${trim}")
        # replacing forward slashes with back slashes so filters can be generated (back slash used in parsing...)
        STRING(REPLACE "/" "\\" FILTERS "${FILEPATH}")
        SOURCE_GROUP("${FILTERS}" FILES ${cur_file})
    endforeach()
endmacro()

# ucm_add_files
# Adds files to a list of sources
macro(ucm_add_files)
    cmake_parse_arguments(ARG "" "TO;FILTER_POP" "" ${ARGN})
    
    if("${ARG_UNPARSED_ARGUMENTS}" STREQUAL "")
        message(FATAL_ERROR "Need to pass some relative files to ucm_add_files()")
    endif()
    if(${ARG_TO} STREQUAL "")
        message(FATAL_ERROR "Need to pass TO and a variable name to ucm_add_files()")
    endif()
    
    if("${ARG_FILTER_POP}" STREQUAL "")
        set(ARG_FILTER_POP 0)
    endif()
    
    ucm_add_files_impl(${ARG_TO} ${ARG_FILTER_POP} "${ARG_UNPARSED_ARGUMENTS}")
endmacro()

# ucm_add_dir_impl
macro(ucm_add_dir_impl result rec trim dirs_in additional_ext)
    set(dirs "${dirs_in}")
    
    # handle the "" and "." cases
    if("${dirs}" STREQUAL "" OR "${dirs}" STREQUAL ".")
        set(dirs "./")
    endif()
    
    foreach(cur_dir ${dirs})
        # to circumvent some linux/cmake/path issues - barely made it work...
        if(cur_dir STREQUAL "./")
            set(cur_dir "")
        else()
            set(cur_dir "${cur_dir}/")
        endif()
        
        # since unix is case sensitive - add these valid extensions too
        # we don't use "UNIX" but instead "CMAKE_HOST_UNIX" because we might be cross
        # compiling (for example emscripten) under windows and UNIX may be set to 1
        # Also OSX is case insensitive like windows...
        set(additional_file_extensions "")
        if(CMAKE_HOST_UNIX AND NOT APPLE)
            set(additional_file_extensions
                "${cur_dir}*.CPP"
                "${cur_dir}*.C"
                "${cur_dir}*.H"
                "${cur_dir}*.HPP"
                )
        endif()
        
        foreach(ext ${additional_ext})
            list(APPEND additional_file_extensions "${cur_dir}*.${ext}")
        endforeach()
        
        # find all sources and set them as result
        FILE(GLOB found_sources RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}"
        # https://gcc.gnu.org/onlinedocs/gcc-4.4.1/gcc/Overall-Options.html#index-file-name-suffix-71
        # sources
            "${cur_dir}*.cpp"
            "${cur_dir}*.cxx"
            "${cur_dir}*.c++"
            "${cur_dir}*.cc"
            "${cur_dir}*.cp"
            "${cur_dir}*.c"
            "${cur_dir}*.i"
            "${cur_dir}*.ii"
        # headers
            "${cur_dir}*.h"
            "${cur_dir}*.h++"
            "${cur_dir}*.hpp"
            "${cur_dir}*.hxx"
            "${cur_dir}*.hh"
            "${cur_dir}*.inl"
            "${cur_dir}*.inc"
            "${cur_dir}*.ipp"
            "${cur_dir}*.ixx"
            "${cur_dir}*.txx"
            "${cur_dir}*.tpp"
            "${cur_dir}*.tcc"
            "${cur_dir}*.tpl"
            ${additional_file_extensions})
        SET(${result} ${${result}} ${found_sources})
        
        # set the proper filters
        ucm_trim_front_words("${cur_dir}" cur_dir "${trim}")
        # replacing forward slashes with back slashes so filters can be generated (back slash used in parsing...)
        STRING(REPLACE "/" "\\" FILTERS "${cur_dir}")
        SOURCE_GROUP("${FILTERS}" FILES ${found_sources})
    endforeach()
    
    if(${rec})
        foreach(cur_dir ${dirs})
            ucm_dir_list("${cur_dir}" subdirs)
            foreach(subdir ${subdirs})
                ucm_add_dir_impl(${result} ${rec} ${trim} "${cur_dir}/${subdir}" "${additional_ext}")
            endforeach()
        endforeach()
    endif()
endmacro()

# ucm_add_dirs
# Adds all files from directories traversing them recursively to a list of sources
# and generates filters according to their location (accepts relative paths only).
# Also this macro trims X times the front word from the filter string for visual studio filters.
macro(ucm_add_dirs)
    cmake_parse_arguments(ARG "RECURSIVE" "TO;FILTER_POP" "ADDITIONAL_EXT" ${ARGN})
    
    if(${ARG_TO} STREQUAL "")
        message(FATAL_ERROR "Need to pass TO and a variable name to ucm_add_dirs()")
    endif()
    
    if("${ARG_FILTER_POP}" STREQUAL "")
        set(ARG_FILTER_POP 0)
    endif()
    
    ucm_add_dir_impl(${ARG_TO} ${ARG_RECURSIVE} ${ARG_FILTER_POP} "${ARG_UNPARSED_ARGUMENTS}" "${ARG_ADDITIONAL_EXT}")
endmacro()

# ucm_add_target
# Adds a target eligible for cotiring - unity build and/or precompiled header
macro(ucm_add_target)
    cmake_parse_arguments(ARG "UNITY" "NAME;TYPE;PCH_FILE;CPP_PER_UNITY" "UNITY_EXCLUDED;SOURCES" ${ARGN})
    
    if(NOT "${ARG_UNPARSED_ARGUMENTS}" STREQUAL "")
        message(FATAL_ERROR "Unrecognized options passed to ucm_add_target()")
    endif()
    if("${ARG_NAME}" STREQUAL "")
        message(FATAL_ERROR "Need to pass NAME and a name for the target to ucm_add_target()")
    endif()
    set(valid_types EXECUTABLE STATIC SHARED MODULE)
    list(FIND valid_types "${ARG_TYPE}" is_type_valid)
    if(${is_type_valid} STREQUAL "-1")
        message(FATAL_ERROR "Need to pass TYPE and the type for the target [EXECUTABLE/STATIC/SHARED/MODULE] to ucm_add_target()")
    endif()
    if("${ARG_SOURCES}" STREQUAL "")
        message(FATAL_ERROR "Need to pass SOURCES and a list of source files to ucm_add_target()")
    endif()
    
    # init with the global unity flag
    set(do_unity ${UCM_UNITY_BUILD})
    
    # check the UNITY argument
    if(NOT ARG_UNITY)
        set(do_unity FALSE)
    endif()
    
    # if target is excluded through the exclusion list
    list(FIND UCM_UNITY_BUILD_EXCLUDE_TARGETS ${ARG_NAME} is_target_excluded)
    if(NOT ${is_target_excluded} STREQUAL "-1")
        set(do_unity FALSE)
    endif()
    
    # unity build only for targets with > 1 source file (otherwise there will be an additional unnecessary target)
    if(do_unity) # optimization
        ucm_count_sources(${ARG_SOURCES} RESULT num_sources)
        if(${num_sources} LESS 2)
            set(do_unity FALSE)
        endif()
    endif()
    
    set(wanted_cotire ${do_unity})
    
    # if cotire cannot be used
    if(do_unity AND NOT ucm_with_cotire)
        set(do_unity FALSE)
    endif()
    
	# inform the developer that the current target might benefit from a unity build
	if(NOT ARG_UNITY AND ${UCM_UNITY_BUILD})
		ucm_count_sources(${ARG_SOURCES} RESULT num_sources)
		if(${num_sources} GREATER 1)
			message(AUTHOR_WARNING "Target '${ARG_NAME}' may benefit from a unity build.\nIt has ${num_sources} sources - enable with UNITY flag")
		endif()
	endif()
    
    # prepare for the unity build
    set(orig_target ${ARG_NAME})
    if(do_unity)
        # the original target will be added with a different name than the requested
        set(orig_target ${ARG_NAME}_ORIGINAL)
        
        # exclude requested files from unity build of the current target
        foreach(excluded_file "${ARG_UNITY_EXCLUDED}")
            set_source_files_properties(${excluded_file} PROPERTIES COTIRE_EXCLUDED TRUE)
        endforeach()
    endif()
    
    # add the original target
    if(${ARG_TYPE} STREQUAL "EXECUTABLE")
        add_executable(${orig_target} ${ARG_SOURCES})
    else()
        add_library(${orig_target} ${ARG_TYPE} ${ARG_SOURCES})
    endif()
    
    if(do_unity)
        # set the number of unity cpp files to be used for the unity target
        if(NOT "${ARG_CPP_PER_UNITY}" STREQUAL "")
            set_property(TARGET ${orig_target} PROPERTY COTIRE_UNITY_SOURCE_MAXIMUM_NUMBER_OF_INCLUDES "${ARG_CPP_PER_UNITY}")
		else()
			set_property(TARGET ${orig_target} PROPERTY COTIRE_UNITY_SOURCE_MAXIMUM_NUMBER_OF_INCLUDES "100")
		endif()
        
        if(NOT "${ARG_PCH_FILE}" STREQUAL "")
            set_target_properties(${orig_target} PROPERTIES COTIRE_CXX_PREFIX_HEADER_INIT "${ARG_PCH_FILE}")
        else()
            set_target_properties(${orig_target} PROPERTIES COTIRE_ENABLE_PRECOMPILED_HEADER FALSE)
        endif()
        # add a unity target for the original one with the name intended for the original
        set_target_properties(${orig_target} PROPERTIES COTIRE_UNITY_TARGET_NAME ${ARG_NAME})
        
        # this is the library call that does the magic
        cotire(${orig_target})
        set_target_properties(clean_cotire PROPERTIES FOLDER "CMakePredefinedTargets")
        
        # disable the original target and enable the unity one
        get_target_property(unity_target_name ${orig_target} COTIRE_UNITY_TARGET_NAME)
        set_target_properties(${orig_target} PROPERTIES EXCLUDE_FROM_ALL 1 EXCLUDE_FROM_DEFAULT_BUILD 1)
        set_target_properties(${unity_target_name} PROPERTIES EXCLUDE_FROM_ALL 0 EXCLUDE_FROM_DEFAULT_BUILD 0)
        
        # also set the name of the target output as the original one
        set_target_properties(${unity_target_name} PROPERTIES OUTPUT_NAME ${ARG_NAME})
        if(UCM_NO_COTIRE_FOLDER)
            # reset the folder property so all unity targets dont end up in a single folder in the solution explorer of VS
            set_target_properties(${unity_target_name} PROPERTIES FOLDER "")
        endif()
        set_target_properties(all_unity PROPERTIES FOLDER "CMakePredefinedTargets")
    elseif(NOT "${ARG_PCH_FILE}" STREQUAL "")
        set(wanted_cotire TRUE)
        if(ucm_with_cotire)
            set_target_properties(${orig_target} PROPERTIES COTIRE_ADD_UNITY_BUILD FALSE)
            set_target_properties(${orig_target} PROPERTIES COTIRE_CXX_PREFIX_HEADER_INIT "${ARG_PCH_FILE}")
            cotire(${orig_target})
            set_target_properties(clean_cotire PROPERTIES FOLDER "CMakePredefinedTargets")
        endif()
    endif()
    
    # print a message if the target was requested to be cotired but it couldn't
    if(wanted_cotire AND NOT ucm_with_cotire)
        if(NOT COMMAND cotire)
            message(AUTHOR_WARNING "Target \"${ARG_NAME}\" not cotired because cotire isn't loaded")
        else()
            message(AUTHOR_WARNING "Target \"${ARG_NAME}\" not cotired because cotire is older than the required version")
        endif()
    endif()
endmacro()
