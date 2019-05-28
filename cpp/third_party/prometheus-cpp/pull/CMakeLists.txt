
if(USE_THIRDPARTY_LIBRARIES)
  find_package(civetweb-3rdparty CONFIG REQUIRED)
else()
  find_package(civetweb REQUIRED)
endif()

if(ENABLE_COMPRESSION)
  find_package(ZLIB REQUIRED)
endif()

add_library(pull
  src/exposer.cc
  src/handler.cc
  src/handler.h
  $<$<BOOL:${USE_THIRDPARTY_LIBRARIES}>:$<TARGET_OBJECTS:civetweb>>
)

add_library(${PROJECT_NAME}::pull ALIAS pull)

target_link_libraries(pull
  PUBLIC
    ${PROJECT_NAME}::core
  PRIVATE
    Threads::Threads
    ${CIVETWEB_LIBRARIES}
    $<$<AND:$<BOOL:UNIX>,$<NOT:$<BOOL:APPLE>>>:rt>
    $<$<BOOL:${ENABLE_COMPRESSION}>:ZLIB::ZLIB>
)

target_include_directories(pull
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  PRIVATE
    ${CIVETWEB_INCLUDE_DIRS}
)

target_compile_definitions(pull
  PRIVATE
    $<$<BOOL:${ENABLE_COMPRESSION}>:HAVE_ZLIB>
)

set_target_properties(pull PROPERTIES OUTPUT_NAME ${PROJECT_NAME}-pull)

install(
  TARGETS pull
  EXPORT ${PROJECT_NAME}-targets
  RUNTIME DESTINATION  ${CMAKE_INSTALL_BINDIR}
  LIBRARY DESTINATION  ${CMAKE_INSTALL_LIBDIR}
  ARCHIVE DESTINATION  ${CMAKE_INSTALL_LIBDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

install(
  DIRECTORY include/
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

if(ENABLE_TESTING)
  add_subdirectory(tests)
endif()
