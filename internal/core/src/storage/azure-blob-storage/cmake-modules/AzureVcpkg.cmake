# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

# We need to know an absolute path to our repo root to do things like referencing ./LICENSE.txt file.
set(AZ_ROOT_DIR "${CMAKE_CURRENT_LIST_DIR}/..")

macro(az_vcpkg_integrate)
  message("Vcpkg integrate step.")

  # AUTO CMAKE_TOOLCHAIN_FILE:
  #   User can call `cmake -DCMAKE_TOOLCHAIN_FILE="path_to_the_toolchain"` as the most specific scenario.
  #   As the last alternative (default case), Azure SDK will automatically clone VCPKG folder and set toolchain from there.
  if(NOT DEFINED CMAKE_TOOLCHAIN_FILE)
    message("CMAKE_TOOLCHAIN_FILE is not defined. Define it for the user.")
    # Set AZURE_SDK_DISABLE_AUTO_VCPKG env var to avoid Azure SDK from cloning and setting VCPKG automatically
    # This option delegate package's dependencies installation to user.
    if(NOT DEFINED ENV{AZURE_SDK_DISABLE_AUTO_VCPKG})
      message("AZURE_SDK_DISABLE_AUTO_VCPKG is not defined. Fetch a local copy of vcpkg.")
      # GET VCPKG FROM SOURCE
      #  User can set env var AZURE_SDK_VCPKG_COMMIT to pick the VCPKG commit to fetch
      set(VCPKG_COMMIT_STRING 8150939b69720adc475461978e07c2d2bf5fb76e) # default SDK tested commit
      if(DEFINED ENV{AZURE_SDK_VCPKG_COMMIT})
        message("AZURE_SDK_VCPKG_COMMIT is defined. Using that instead of the default.")
        set(VCPKG_COMMIT_STRING "$ENV{AZURE_SDK_VCPKG_COMMIT}") # default SDK tested commit
      endif()
      message("Vcpkg commit string used: ${VCPKG_COMMIT_STRING}")
      include(FetchContent)
      FetchContent_Declare(
          vcpkg
          GIT_REPOSITORY      https://github.com/microsoft/vcpkg.git
          GIT_TAG             ${VCPKG_COMMIT_STRING}
          )
      FetchContent_GetProperties(vcpkg)
      # make sure to pull vcpkg only once.
      if(NOT vcpkg_POPULATED)
          FetchContent_Populate(vcpkg)
      endif()
      # use the vcpkg source path
      set(CMAKE_TOOLCHAIN_FILE "${vcpkg_SOURCE_DIR}/scripts/buildsystems/vcpkg.cmake" CACHE STRING "")
    endif()
  endif()

  # enable triplet customization
  if(DEFINED ENV{VCPKG_DEFAULT_TRIPLET} AND NOT DEFINED VCPKG_TARGET_TRIPLET)
    set(VCPKG_TARGET_TRIPLET "$ENV{VCPKG_DEFAULT_TRIPLET}" CACHE STRING "")
  endif()
  message("Vcpkg integrate step - DONE.")
endmacro()

macro(az_vcpkg_portfile_prep targetName fileName contentToRemove)
  # with sdk/<lib>/vcpkg/<fileName>
  file(READ "${CMAKE_CURRENT_SOURCE_DIR}/vcpkg/${fileName}" fileContents)

  # Windows -> Unix line endings
  string(FIND fileContents "\r\n" crLfPos)

  if (crLfPos GREATER -1)
    string(REPLACE "\r\n" "\n" fileContents ${fileContents})
  endif()

  # remove comment header
  string(REPLACE "${contentToRemove}" "" fileContents ${fileContents})

  # undo Windows -> Unix line endings (if applicable)
  if (crLfPos GREATER -1)
    string(REPLACE "\n" "\r\n" fileContents ${fileContents})
  endif()
  unset(crLfPos)

  # output to an intermediate location
  file (WRITE "${CMAKE_BINARY_DIR}/vcpkg_prep/${targetName}/${fileName}" ${fileContents})
  unset(fileContents)

  # Produce the files to help with the vcpkg release.
  # Go to the /out/build/<cfg>/vcpkg directory, and copy (merge) "ports" folder to the vcpkg repo.
  # Then, update the portfile.cmake file SHA512 from "1" to the actual hash (a good way to do it is to uninstall a package,
  # clean vcpkg/downloads, vcpkg/buildtrees, run "vcpkg install <pkg>", and get the SHA from the error message).
  configure_file(
    "${CMAKE_BINARY_DIR}/vcpkg_prep/${targetName}/${fileName}"
    "${CMAKE_BINARY_DIR}/vcpkg/ports/${targetName}-cpp/${fileName}"
    @ONLY
  )
endmacro()

macro(az_vcpkg_export targetName macroNamePart dllImportExportHeaderPath)
  foreach(vcpkgFile "vcpkg.json" "portfile.cmake")
    az_vcpkg_portfile_prep(
      "${targetName}"
      "${vcpkgFile}"
      "# Copyright (c) Microsoft Corporation.\n# Licensed under the MIT License.\n\n"
    )
  endforeach()

  # Standard names for folders such as "bin", "lib", "include". We could hardcode, but some other libs use it too (curl).
  include(GNUInstallDirs)

  # When installing, copy our "inc" directory (headers) to "include" directory at the install location.
  install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/inc/azure/" DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/azure")

  # Copy license as "copyright" (vcpkg dictates naming and location).
  install(FILES "${AZ_ROOT_DIR}/LICENSE.txt" DESTINATION "${CMAKE_INSTALL_DATAROOTDIR}/${targetName}-cpp" RENAME "copyright")

  # Indicate where to install targets. Mirrors what other ports do.
  install(
    TARGETS "${targetName}"
      EXPORT "${targetName}-cppTargets"
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR} # DLLs (if produced by build) go to "/bin"
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR} # static .lib files
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR} # .lib files for DLL build
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR} # headers
    )

  # If building a Windows DLL, patch the dll_import_export.hpp
  if(WIN32 AND BUILD_SHARED_LIBS)
    add_compile_definitions(AZ_${macroNamePart}_BEING_BUILT)
    target_compile_definitions(${targetName} PUBLIC AZ_${macroNamePart}_DLL)

    set(AZ_${macroNamePart}_DLL_INSTALLED_AS_PACKAGE "*/ + 1 /*")
    configure_file(
      "${CMAKE_CURRENT_SOURCE_DIR}/inc/${dllImportExportHeaderPath}"
      "${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_INCLUDEDIR}/${dllImportExportHeaderPath}"
      @ONLY
    )
    unset(AZ_${macroNamePart}_DLL_INSTALLED_AS_PACKAGE)

    get_filename_component(dllImportExportHeaderDir ${dllImportExportHeaderPath} DIRECTORY)
    install(
        FILES "${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_INCLUDEDIR}/${dllImportExportHeaderPath}"
        DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/${dllImportExportHeaderDir}"
    )
    unset(dllImportExportHeaderDir)
  endif()

  # Export the targets file itself.
  install(
    EXPORT "${targetName}-cppTargets"
      DESTINATION "${CMAKE_INSTALL_DATAROOTDIR}/${targetName}-cpp"
      NAMESPACE Azure:: # Not the C++ namespace, but a namespace in terms of cmake.
      FILE "${targetName}-cppTargets.cmake"
    )

  # configure_package_config_file(), write_basic_package_version_file()
  include(CMakePackageConfigHelpers)

  # Produce package config file.
  configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/vcpkg/Config.cmake.in"
    "${targetName}-cppConfig.cmake"
    INSTALL_DESTINATION "${CMAKE_INSTALL_DATAROOTDIR}/${targetName}-cpp"
    PATH_VARS
      CMAKE_INSTALL_LIBDIR)

  # Produce version file.
  write_basic_package_version_file(
    "${targetName}-cppConfigVersion.cmake"
      VERSION ${AZ_LIBRARY_VERSION} # the version that we extracted from package_version.hpp
      COMPATIBILITY SameMajorVersion
    )

  # Install package config and version files.
  install(
      FILES
        "${CMAKE_CURRENT_BINARY_DIR}/${targetName}-cppConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/${targetName}-cppConfigVersion.cmake"
      DESTINATION
        "${CMAKE_INSTALL_DATAROOTDIR}/${targetName}-cpp" # to shares/<our_pkg>
    )

  # Export all the installs above as package.
  export(PACKAGE "${targetName}-cpp")
endmacro()