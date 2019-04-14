# Macro that defines variables describing the Fortran name mangling
# convention
#
# Sets the following outputs on success:
#
#  INTFACE
#    Add_
#    NoChange
#    f77IsF2C
#    UpCase
#
macro(FORTRAN_MANGLING CDEFS)
message(STATUS "=========")
  get_filename_component(F77_NAME ${CMAKE_Fortran_COMPILER} NAME)
  get_filename_component(F77_PATH ${CMAKE_Fortran_COMPILER} PATH)
  set(F77 ${F77_NAME} CACHE INTERNAL "Name of the fortran compiler.")

  if(${F77} STREQUAL "ifort.exe")
    #settings for Intel Fortran
    set(F77_OPTION_COMPILE "/c" CACHE INTERNAL
      "Fortran compiler option for compiling without linking.")
    set(F77_OUTPUT_OBJ "/Fo" CACHE INTERNAL
      "Fortran compiler option for setting object file name.")
    set(F77_OUTPUT_EXE "/Fe" CACHE INTERNAL
      "Fortran compiler option for setting executable file name.")
  else()
    # in other case, let user specify their fortran configrations.
    set(F77_OPTION_COMPILE "-c" CACHE STRING
      "Fortran compiler option for compiling without linking.")
    set(F77_OUTPUT_OBJ "-o" CACHE STRING
      "Fortran compiler option for setting object file name.")
    set(F77_OUTPUT_EXE "-o" CACHE STRING
      "Fortran compiler option for setting executable file name.")
    set(F77_LIB_PATH "" CACHE PATH
      "Library path for the fortran compiler")
    set(F77_INCLUDE_PATH "" CACHE PATH
      "Include path for the fortran compiler")
  endif()


message(STATUS "Testing FORTRAN_MANGLING")

message(STATUS "Compiling Finface.f...")

    execute_process ( COMMAND  ${CMAKE_Fortran_COMPILER} ${F77_OPTION_COMPILE} ${PROJECT_SOURCE_DIR}/lapacke/mangling/Fintface.f
      WORKING_DIRECTORY  ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/CMakeTmp
      OUTPUT_VARIABLE    OUTPUT
      RESULT_VARIABLE    RESULT
      ERROR_VARIABLE     ERROR)

    if(RESULT EQUAL 0)
    message(STATUS "Compiling Finface.f successful")
    else()
    message(FATAL_ERROR " Compiling Finface.f FAILED")
    message(FATAL_ERROR " Error:\n ${ERROR}")
    endif()

message(STATUS "Compiling Cintface.c...")

    execute_process ( COMMAND  ${CMAKE_C_COMPILER} ${F77_OPTION_COMPILE} ${PROJECT_SOURCE_DIR}/lapacke/mangling/Cintface.c
      WORKING_DIRECTORY  ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/CMakeTmp
      OUTPUT_VARIABLE    OUTPUT
      RESULT_VARIABLE    RESULT
      ERROR_VARIABLE     ERROR)

    if(RESULT EQUAL 0)
    message(STATUS "Compiling Cintface.c successful")
    else()
    message(FATAL_ERROR " Compiling Cintface.c FAILED")
    message(FATAL_ERROR " Error:\n ${ERROR}")
    endif()

message(STATUS "Linking Finface.f and Cintface.c...")

    execute_process ( COMMAND  ${CMAKE_Fortran_COMPILER} ${F77_OUTPUT_OBJ} xintface.exe Fintface.o Cintface.o
      WORKING_DIRECTORY  ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/CMakeTmp
      OUTPUT_VARIABLE    OUTPUT
      RESULT_VARIABLE    RESULT
      ERROR_VARIABLE     ERROR)

    if(RESULT EQUAL 0)
    message(STATUS "Linking Finface.f and Cintface.c successful")
    else()
    message(FATAL_ERROR " Linking Finface.f and Cintface.c FAILED")
    message(FATAL_ERROR " Error:\n ${ERROR}")
    endif()

message(STATUS "Running ./xintface...")

    execute_process ( COMMAND  ./xintface.exe
      WORKING_DIRECTORY  ${CMAKE_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/CMakeTmp
      RESULT_VARIABLE xintface_RES
      OUTPUT_VARIABLE xintface_OUT
      ERROR_VARIABLE xintface_ERR)


       if (xintface_RES EQUAL 0)
          string(REPLACE "\n" "" xintface_OUT "${xintface_OUT}")
          message(STATUS "Fortran MANGLING convention: ${xintface_OUT}")
          set(CDEFS ${xintface_OUT})
      else()
          message(FATAL_ERROR "FORTRAN_MANGLING:ERROR ${xintface_ERR}")
      endif()

endmacro()
