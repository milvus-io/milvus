
add_executable(benchmarks
  main.cc
  benchmark_helpers.cc
  benchmark_helpers.h
  counter_bench.cc
  gauge_bench.cc
  histogram_bench.cc
  registry_bench.cc
  summary_bench.cc
)

target_link_libraries(benchmarks
  PRIVATE
    ${PROJECT_NAME}::core
    benchmark::benchmark
)

add_test(
  NAME benchmarks
  COMMAND benchmarks
)

set_property(
  TEST benchmarks
  APPEND PROPERTY LABELS Benchmark
)
