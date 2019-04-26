cmake_minimum_required (VERSION 3.2)
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# PACKAGE_VERSION is used by cpack scripts currently
# Both sqlite_orm_VERSION and PACKAGE_VERSION should be the same for now

set(sqlite_orm_VERSION "1.3.0")
set(PACKAGE_VERSION ${sqlite_orm_VERSION})

project("sqlite_orm" VERSION ${PACKAGE_VERSION})

message(STATUS "Configuring ${CMAKE_PROJECT_NAME} ${sqlite_orm_VERSION}")

set(CMAKE_MODULE_PATH "${CMAKE_MODULE_PATH}" "${CMAKE_CURRENT_SOURCE_DIR}/build/cmake")

# Build time options are defined here
include(DefineInstallationPaths)

# Generate the SqliteOrmConfig.cmake module
include(GenerateConfigModule)

set(ProjectName "SqliteOrm")

option(SqliteOrm_BuildTests "Build sqlite_orm unit tests" ON)

set(SqliteOrm_INCLUDE "${CMAKE_CURRENT_SOURCE_DIR}/include")
add_library(sqlite_orm INTERFACE)

target_sources(sqlite_orm INTERFACE
	$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include/sqlite_orm/sqlite_orm.h>
	$<INSTALL_INTERFACE:include/sqlite_orm/sqlite_orm.h>)

target_include_directories(sqlite_orm INTERFACE
	$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
	$<INSTALL_INTERFACE:include>)

include(ucm)

if (MSVC)
	string(REGEX REPLACE "/RTC(su|[1su])" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")	
	string(REGEX REPLACE "/RTC(su|[1su])" "" CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG}")	
	string(REGEX REPLACE "/RTC(su|[1su])" "" CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")	
	add_compile_options(/EHsc)

	if ("${CMAKE_GENERATOR}" MATCHES "(Win64|x64)")
		message(STATUS "Add /bigobj flag to compiler")
    	add_compile_options(/bigobj)
	endif()
endif()

ucm_print_flags()
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")

# Tests
# include(CTest)
#if(SqliteOrm_BuildTests AND BUILD_TESTING)
#    enable_testing()
#    add_subdirectory(tests)
#endif()

#add_subdirectory(examples)

install(TARGETS sqlite_orm EXPORT "${ProjectName}Targets"
	INCLUDES DESTINATION "${INCLUDE_INSTALL_DIR}" COMPONENT Development
	PUBLIC_HEADER DESTINATION "${INCLUDE_INSTALL_DIR}" COMPONENT Development)

install(FILES "include/sqlite_orm/sqlite_orm.h"
	DESTINATION "${INCLUDE_INSTALL_DIR}" COMPONENT Development)

export(EXPORT "${ProjectName}Targets"
	FILE "${CMAKE_CURRENT_BINARY_DIR}/${ProjectName}/${ProjectName}Targets.cmake"
	NAMESPACE "sqlite_orm::")

install(EXPORT "${ProjectName}Targets"
	FILE "${ProjectName}Targets.cmake"
	NAMESPACE "sqlite_orm::"
	DESTINATION "${CMAKE_INSTALL_DIR}/sqlite_orm")
