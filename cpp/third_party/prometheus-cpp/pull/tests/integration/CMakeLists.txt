
add_executable(sample_server
  sample_server.cc
)

target_link_libraries(sample_server
  PRIVATE
    ${PROJECT_NAME}::pull
)
