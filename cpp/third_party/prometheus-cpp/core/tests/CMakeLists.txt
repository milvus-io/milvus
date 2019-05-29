
add_executable(prometheus_test
  builder_test.cc
  check_names_test.cc
  counter_test.cc
  family_test.cc
  gauge_test.cc
  histogram_test.cc
  registry_test.cc
  summary_test.cc
  utils_test.cc
)

target_link_libraries(prometheus_test
  PRIVATE
    ${PROJECT_NAME}::core
    GTest::gmock_main
)

add_test(
  NAME prometheus_test
  COMMAND prometheus_test
)
