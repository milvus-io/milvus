get_filename_component(_IMPORT_PREFIX "${PROJECT_SOURCE_DIR}/3rdparty/civetweb/" ABSOLUTE)

macro(set_and_check _var _file)
  set(${_var} "${_file}")
  if(NOT EXISTS "${_file}")
    message(FATAL_ERROR "File or directory ${_file} referenced by variable ${_var} does not exist !")
  endif()
endmacro()

set_and_check(CIVETWEB_INCLUDE_DIR ${_IMPORT_PREFIX}/include)
set(CIVETWEB_INCLUDE_DIRS "${CIVETWEB_INCLUDE_DIR}")

add_library(civetweb OBJECT
  ${_IMPORT_PREFIX}/include/CivetServer.h
  ${_IMPORT_PREFIX}/include/civetweb.h
  ${_IMPORT_PREFIX}/src/CivetServer.cpp
  ${_IMPORT_PREFIX}/src/civetweb.c
  ${_IMPORT_PREFIX}/src/handle_form.inl
  ${_IMPORT_PREFIX}/src/md5.inl
)

target_compile_definitions(civetweb
  PRIVATE
    CIVETWEB_API=
    USE_IPV6
    NDEBUG
    NO_CGI
    NO_CACHING
    NO_SSL
    NO_FILES
)

target_compile_options(civetweb
  PRIVATE
    $<$<CXX_COMPILER_ID:AppleClang>:-w>
    $<$<CXX_COMPILER_ID:GNU>:-w>
)

target_include_directories(civetweb
  PRIVATE
    ${CIVETWEB_INCLUDE_DIRS}
)

if(BUILD_SHARED_LIBS)
  set_target_properties(civetweb PROPERTIES
    POSITION_INDEPENDENT_CODE ON
    C_VISIBILITY_PRESET hidden
    CXX_VISIBILITY_PRESET hidden
    VISIBILITY_INLINES_HIDDEN ON
  )
endif()
