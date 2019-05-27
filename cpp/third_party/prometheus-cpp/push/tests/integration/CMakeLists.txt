
add_executable(sample_client
  sample_client.cc
)

target_link_libraries(sample_client
  PRIVATE
    ${PROJECT_NAME}::push
)
