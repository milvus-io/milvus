# This module perdorms several try-compiles to determine the default integer
# size being used by the fortran compiler
#
# After execution, the following variables are set.  If they are un set then
# size detection was not possible
#
# SIZEOF_INTEGER   - Number of bytes used to store the default INTEGER type
# SIZEOF_REAL      - Number of bytes used to store the default REAL type
# SIZEOF_LOGICAL   - Number of bytes used to store the default LOGICAL type
# SIZEOF_CHARACTER - Number of bytes used to store the default CHARACTER type
#
#=============================================================================
# Author: Chuck Atkins
# Copyright 2011
#=============================================================================

# Check the size of a single fortran type
macro( _CHECK_FORTRAN_TYPE_SIZE _TYPE_NAME _TEST_SIZES )

  foreach( __TEST_SIZE ${_TEST_SIZES} )
    set( __TEST_FILE ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/CMakeTmp/testFortran${_TYPE_NAME}Size${__TEST_SIZE}.f90 )
    file( WRITE ${__TEST_FILE}
"
       PROGRAM check_size
         ${_TYPE_NAME}*${__TEST_SIZE}, TARGET :: a
         ${_TYPE_NAME}, POINTER :: pa
         pa => a
       END PROGRAM
")
    try_compile( SIZEOF_${_TYPE_NAME} ${CMAKE_BINARY_DIR} ${__TEST_FILE} )
    if( SIZEOF_${_TYPE_NAME} )
      message( STATUS "Testing default ${_TYPE_NAME}*${__TEST_SIZE} - found" )
      set( SIZEOF_${_TYPE_NAME} ${__TEST_SIZE} CACHE INTERNAL "Size of the default ${_TYPE_NAME} type" FORCE )
      break()
    else()
      message( STATUS "Testing default ${_TYPE_NAME}*${__TEST_SIZE} -" )
    endif()
  endforeach()

endmacro()


macro( CHECK_FORTRAN_TYPE_SIZES )
  if( NOT CMAKE_Fortran_COMPILER_SUPPORTS_F90 )
    message( FATAL_ERROR "Type size tests require Fortran 90 support" )
  endif()

  _CHECK_FORTRAN_TYPE_SIZE( "INTEGER" "2;4;8;16" )
  _CHECK_FORTRAN_TYPE_SIZE( "REAL" "4;8;12;16" )
  _CHECK_FORTRAN_TYPE_SIZE( "LOGICAL" "1;2;4;8;16" )
  _CHECK_FORTRAN_TYPE_SIZE( "CHARACTER" "1;2;4;8;16" )
endmacro()

