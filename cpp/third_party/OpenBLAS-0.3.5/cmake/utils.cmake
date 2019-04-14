# Functions to help with the OpenBLAS build

# Reads string from getarch into CMake vars. Format of getarch vars is VARNAME=VALUE
function(ParseGetArchVars GETARCH_IN)
  string(REGEX MATCHALL "[0-9_a-zA-Z]+=[0-9_a-zA-Z]+" GETARCH_RESULT_LIST "${GETARCH_IN}")
  foreach (GETARCH_LINE ${GETARCH_RESULT_LIST})
    # split the line into var and value, then assign the value to a CMake var
    string(REGEX MATCHALL "[0-9_a-zA-Z]+" SPLIT_VAR "${GETARCH_LINE}")
    list(GET SPLIT_VAR 0 VAR_NAME)
    list(GET SPLIT_VAR 1 VAR_VALUE)
    set(${VAR_NAME} ${VAR_VALUE} PARENT_SCOPE)
  endforeach ()
endfunction ()

# Reads a Makefile into CMake vars.
macro(ParseMakefileVars MAKEFILE_IN)
  message(STATUS "Reading vars from ${MAKEFILE_IN}...")
  file(STRINGS ${MAKEFILE_IN} makefile_contents)
  foreach (makefile_line ${makefile_contents})
    string(REGEX MATCH "([0-9_a-zA-Z]+)[ \t]*=[ \t]*(.+)$" line_match "${makefile_line}")
    if (NOT "${line_match}" STREQUAL "")
      set(var_name ${CMAKE_MATCH_1})
      set(var_value ${CMAKE_MATCH_2})
      # check for Makefile variables in the string, e.g. $(TSUFFIX)
      string(REGEX MATCHALL "\\$\\(([0-9_a-zA-Z]+)\\)" make_var_matches ${var_value})
      foreach (make_var ${make_var_matches})
        # strip out Makefile $() markup
        string(REGEX REPLACE "\\$\\(([0-9_a-zA-Z]+)\\)" "\\1" make_var ${make_var})
        # now replace the instance of the Makefile variable with the value of the CMake variable (note the double quote)
        string(REPLACE "$(${make_var})" "${${make_var}}" var_value ${var_value})
      endforeach ()
      set(${var_name} ${var_value})
    else ()
      string(REGEX MATCH "include \\$\\(KERNELDIR\\)/(.+)$" line_match "${makefile_line}")
      if (NOT "${line_match}" STREQUAL "")
        ParseMakefileVars(${KERNELDIR}/${CMAKE_MATCH_1})
      endif ()
    endif ()
  endforeach ()
endmacro ()

# Returns all combinations of the input list, as a list with colon-separated combinations
# E.g. input of A B C returns A B C A:B A:C B:C
# N.B. The input is meant to be a list, and to past a list to a function in CMake you must quote it (e.g. AllCombinations("${LIST_VAR}")).
# #param absent_codes codes to use when an element is absent from a combination. For example, if you have TRANS;UNIT;UPPER you may want the code to be NNL when nothing is present.
# @returns LIST_OUT a list of combinations
#          CODES_OUT a list of codes corresponding to each combination, with N meaning the item is not present, and the first letter of the list item meaning it is presen
function(AllCombinations list_in absent_codes_in)
  list(LENGTH list_in list_count)
  set(num_combos 1)
  # subtract 1 since we will iterate from 0 to num_combos
  math(EXPR num_combos "(${num_combos} << ${list_count}) - 1")
  set(LIST_OUT "")
  set(CODES_OUT "")
  foreach (c RANGE 0 ${num_combos})

    set(current_combo "")
    set(current_code "")

    # this is a little ridiculous just to iterate through a list w/ indices
    math(EXPR last_list_index "${list_count} - 1")
    foreach (list_index RANGE 0 ${last_list_index})
      math(EXPR bit "1 << ${list_index}")
      math(EXPR combo_has_bit "${c} & ${bit}")
      list(GET list_in ${list_index} list_elem)
      if (combo_has_bit)
        if (current_combo)
          set(current_combo "${current_combo}:${list_elem}")
        else ()
          set(current_combo ${list_elem})
        endif ()
        string(SUBSTRING ${list_elem} 0 1 code_char)
      else ()
        list(GET absent_codes_in ${list_index} code_char)
      endif ()
      set(current_code "${current_code}${code_char}")
    endforeach ()

    if (current_combo STREQUAL "")
      list(APPEND LIST_OUT " ") # Empty set is a valid combination, but CMake isn't appending the empty string for some reason, use a space
    else ()
      list(APPEND LIST_OUT ${current_combo})
    endif ()
    list(APPEND CODES_OUT ${current_code})

  endforeach ()

  set(LIST_OUT ${LIST_OUT} PARENT_SCOPE)
  set(CODES_OUT ${CODES_OUT} PARENT_SCOPE)
endfunction ()

# generates object files for each of the sources, using the BLAS naming scheme to pass the funciton name as a preprocessor definition
# @param sources_in the source files to build from
# @param defines_in (optional) preprocessor definitions that will be applied to all objects
# @param name_in (optional) if this is set this name will be used instead of the filename. Use a * to indicate where the float character should go, if no star the character will be prepended.
#                           e.g. with DOUBLE set, "i*max" will generate the name "idmax", and "max" will be "dmax"
# @param replace_last_with replaces the last character in the filename with this string (e.g. symm_k should be symm_TU)
# @param append_with appends the filename with this string (e.g. trmm_R should be trmm_RTUU or some other combination of characters)
# @param no_float_type turns off the float type define for this build (e.g. SINGLE/DOUBLE/etc)
# @param complex_filename_scheme some routines have separate source files for complex and non-complex float types.
#                               0 - compiles for all types
#                               1 - compiles the sources for non-complex types only (SINGLE/DOUBLE)
#                               2 - compiles for complex types only (COMPLEX/DOUBLE COMPLEX)
#                               3 - compiles for all types, but changes source names for complex by prepending z (e.g. axpy.c becomes zaxpy.c)
#                               4 - compiles for complex types only, but changes source names for complex by prepending z (e.g. hemv.c becomes zhemv.c)
#                               STRING - compiles only the given type (e.g. DOUBLE)
function(GenerateNamedObjects sources_in)

  if (DEFINED ARGV1)
    set(defines_in ${ARGV1})
  endif ()

  if (DEFINED ARGV2 AND NOT "${ARGV2}" STREQUAL "")
    set(name_in ${ARGV2})
    # strip off extension for kernel files that pass in the object name.
    get_filename_component(name_in ${name_in} NAME_WE)
  endif ()

  if (DEFINED ARGV3)
    set(use_cblas ${ARGV3})
  else ()
    set(use_cblas false)
  endif ()

  if (DEFINED ARGV4)
    set(replace_last_with ${ARGV4})
  endif ()

  if (DEFINED ARGV5)
    set(append_with ${ARGV5})
  endif ()

  if (DEFINED ARGV6)
    set(no_float_type ${ARGV6})
  else ()
    set(no_float_type false)
  endif ()

  if (no_float_type)
    set(float_list "DUMMY") # still need to loop once
  else ()
    set(float_list "${FLOAT_TYPES}")
  endif ()

  set(real_only false)
  set(complex_only false)
  set(mangle_complex_sources false)
  if (DEFINED ARGV7 AND NOT "${ARGV7}" STREQUAL "")
    if (${ARGV7} EQUAL 1)
      set(real_only true)
    elseif (${ARGV7} EQUAL 2)
      set(complex_only true)
    elseif (${ARGV7} EQUAL 3)
      set(mangle_complex_sources true)
    elseif (${ARGV7} EQUAL 4)
      set(mangle_complex_sources true)
      set(complex_only true)
    elseif (NOT ${ARGV7} EQUAL 0)
      set(float_list ${ARGV7})
    endif ()
  endif ()

  if (complex_only)
    list(REMOVE_ITEM float_list "SINGLE")
    list(REMOVE_ITEM float_list "DOUBLE")
  elseif (real_only)
    list(REMOVE_ITEM float_list "COMPLEX")
    list(REMOVE_ITEM float_list "ZCOMPLEX")
  endif ()

  set(float_char "")
  set(OBJ_LIST_OUT "")
  foreach (float_type ${float_list})
    foreach (source_file ${sources_in})

      if (NOT no_float_type)
        string(SUBSTRING ${float_type} 0 1 float_char)
        string(TOLOWER ${float_char} float_char)
      endif ()

      if (NOT name_in)
        get_filename_component(source_name ${source_file} NAME_WE)
        set(obj_name "${float_char}${source_name}")
      else ()
        # replace * with float_char
        if (${name_in} MATCHES "\\*")
          string(REPLACE "*" ${float_char} obj_name ${name_in})
        else ()
          set(obj_name "${float_char}${name_in}")
        endif ()
      endif ()

      if (replace_last_with)
        string(REGEX REPLACE ".$" ${replace_last_with} obj_name ${obj_name})
      else ()
        set(obj_name "${obj_name}${append_with}")
      endif ()

      # now add the object and set the defines
      set(obj_defines ${defines_in})

      if (use_cblas)
        set(obj_name "cblas_${obj_name}")
        list(APPEND obj_defines "CBLAS")
      elseif (NOT "${obj_name}" MATCHES "${ARCH_SUFFIX}")
        set(obj_name "${obj_name}${ARCH_SUFFIX}")
      endif ()

      list(APPEND obj_defines "ASMNAME=${FU}${obj_name};ASMFNAME=${FU}${obj_name}${BU};NAME=${obj_name}${BU};CNAME=${obj_name};CHAR_NAME=\"${obj_name}${BU}\";CHAR_CNAME=\"${obj_name}\"")
      if (${float_type} STREQUAL "DOUBLE" OR ${float_type} STREQUAL "ZCOMPLEX")
        list(APPEND obj_defines "DOUBLE")
      endif ()
      if (${float_type} STREQUAL "COMPLEX" OR ${float_type} STREQUAL "ZCOMPLEX")
        list(APPEND obj_defines "COMPLEX")
        if (mangle_complex_sources)
          # add a z to the filename
          get_filename_component(source_name ${source_file} NAME)
          get_filename_component(source_dir ${source_file} DIRECTORY)
          string(REPLACE ${source_name} "z${source_name}" source_file ${source_file})
        endif ()
      endif ()

      if (VERBOSE_GEN)
        message(STATUS "${obj_name}:${source_file}")
        message(STATUS "${obj_defines}")
      endif ()

      # create a copy of the source to avoid duplicate obj filename problem with ar.exe
      get_filename_component(source_extension ${source_file} EXT)
      set(new_source_file "${CMAKE_CURRENT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/${obj_name}${source_extension}")
      if (IS_ABSOLUTE ${source_file})
        set(old_source_file ${source_file})
      else ()
        set(old_source_file "${CMAKE_CURRENT_LIST_DIR}/${source_file}")
      endif ()

      string(REPLACE ";" "\n#define " define_source "${obj_defines}")
      string(REPLACE "=" " " define_source "${define_source}")
      file(WRITE ${new_source_file}.tmp "#define ${define_source}\n#include \"${old_source_file}\"")
      configure_file(${new_source_file}.tmp ${new_source_file} COPYONLY)
      file(REMOVE ${new_source_file}.tmp)
      list(APPEND SRC_LIST_OUT ${new_source_file})

    endforeach ()
  endforeach ()

  list(APPEND OPENBLAS_SRC ${SRC_LIST_OUT})
  set(OPENBLAS_SRC ${OPENBLAS_SRC} PARENT_SCOPE)
endfunction ()

# generates object files for each of the sources for each of the combinations of the preprocessor definitions passed in
# @param sources_in the source files to build from
# @param defines_in the preprocessor definitions that will be combined to create the object files
# @param all_defines_in (optional) preprocessor definitions that will be applied to all objects
# @param replace_scheme If 1, replace the "k" in the filename with the define combo letters. E.g. symm_k.c with TRANS and UNIT defined will be symm_TU.
#                  If 0, it will simply append the code, e.g. symm_L.c with TRANS and UNIT will be symm_LTU.
#                  If 2, it will append the code with an underscore, e.g. symm.c with TRANS and UNIT will be symm_TU.
#                  If 3, it will insert the code *around* the last character with an underscore, e.g. symm_L.c with TRANS and UNIT will be symm_TLU (required by BLAS level2 objects).
#                  If 4, it will insert the code before the last underscore. E.g. trtri_U_parallel with TRANS will be trtri_UT_parallel
# @param alternate_name replaces the source name as the object name (define codes are still appended)
# @param no_float_type turns off the float type define for this build (e.g. SINGLE/DOUBLE/etc)
# @param complex_filename_scheme see GenerateNamedObjects
function(GenerateCombinationObjects sources_in defines_in absent_codes_in all_defines_in replace_scheme)

  set(alternate_name_in "")
  if (DEFINED ARGV5)
    set(alternate_name_in ${ARGV5})
  endif ()

  set(no_float_type false)
  if (DEFINED ARGV6)
    set(no_float_type ${ARGV6})
  endif ()

  set(complex_filename_scheme "")
  if (DEFINED ARGV7)
    set(complex_filename_scheme ${ARGV7})
  endif ()

  AllCombinations("${defines_in}" "${absent_codes_in}")
  set(define_combos ${LIST_OUT})
  set(define_codes ${CODES_OUT})

  list(LENGTH define_combos num_combos)
  math(EXPR num_combos "${num_combos} - 1")

  foreach (c RANGE 0 ${num_combos})

    list(GET define_combos ${c} define_combo)
    list(GET define_codes ${c} define_code)

    foreach (source_file ${sources_in})

      set(alternate_name ${alternate_name_in})

      # replace colon separated list with semicolons, this turns it into a CMake list that we can use foreach with
      string(REPLACE ":" ";" define_combo ${define_combo})

      # now add the object and set the defines
      set(cur_defines ${define_combo})
      if ("${cur_defines}" STREQUAL " ")
        set(cur_defines ${all_defines_in})
      else ()
        list(APPEND cur_defines ${all_defines_in})
      endif ()

      set(replace_code "")
      set(append_code "")
      if (replace_scheme EQUAL 1)
        set(replace_code ${define_code})
      else ()
        if (replace_scheme EQUAL 2)
          set(append_code "_${define_code}")
        elseif (replace_scheme EQUAL 3)
          if ("${alternate_name}" STREQUAL "")
            string(REGEX MATCH "[a-zA-Z]\\." last_letter ${source_file})
          else ()
            string(REGEX MATCH "[a-zA-Z]$" last_letter ${alternate_name})
          endif ()
          # first extract the last letter
          string(SUBSTRING ${last_letter} 0 1 last_letter) # remove period from match
          # break the code up into the first letter and the remaining (should only be 2 anyway)
          string(SUBSTRING ${define_code} 0 1 define_code_first)
          string(SUBSTRING ${define_code} 1 -1 define_code_second)
          set(replace_code "${define_code_first}${last_letter}${define_code_second}")
        elseif (replace_scheme EQUAL 4)
          # insert code before the last underscore and pass that in as the alternate_name
          if ("${alternate_name}" STREQUAL "")
            get_filename_component(alternate_name ${source_file} NAME_WE)
          endif ()
          set(extra_underscore "")
          # check if filename has two underscores, insert another if not (e.g. getrs_parallel needs to become getrs_U_parallel not getrsU_parallel)
          string(REGEX MATCH "_[a-zA-Z]+_" underscores ${alternate_name})
          string(LENGTH "${underscores}" underscores)
          if (underscores EQUAL 0)
            set(extra_underscore "_")
          endif ()
          string(REGEX REPLACE "(.+)(_[^_]+)$" "\\1${extra_underscore}${define_code}\\2" alternate_name ${alternate_name})
        else()
          set(append_code ${define_code}) # replace_scheme should be 0
        endif ()
      endif ()

      GenerateNamedObjects("${source_file}" "${cur_defines}" "${alternate_name}" false "${replace_code}" "${append_code}" "${no_float_type}" "${complex_filename_scheme}")
    endforeach ()
  endforeach ()

  set(OPENBLAS_SRC ${OPENBLAS_SRC} PARENT_SCOPE)
endfunction ()

