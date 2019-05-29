#include <chrono>
#include <random>

#include <benchmark/benchmark.h>
#include <prometheus/registry.h>

using prometheus::Histogram;

static Histogram::BucketBoundaries CreateLinearBuckets(double start, double end,
                                                       double step) {
  auto bucket_boundaries = Histogram::BucketBoundaries{};
  for (auto i = start; i < end; i += step) {
    bucket_boundaries.push_back(i);
  }
  return bucket_boundaries;
}

static void BM_Histogram_Observe(benchmark::State& state) {
  using prometheus::BuildHistogram;
  using prometheus::Histogram;
  using prometheus::Registry;

  const auto number_of_buckets = state.range(0);

  Registry registry;
  auto& histogram_family =
      BuildHistogram().Name("benchmark_histogram").Help("").Register(registry);
  auto bucket_boundaries = CreateLinearBuckets(0, number_of_buckets - 1, 1);
  auto& histogram = histogram_family.Add({}, bucket_boundaries);
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> d(0, number_of_buckets);

  while (state.KeepRunning()) {
    auto observation = d(gen);
    auto start = std::chrono::high_resolution_clock::now();
    histogram.Observe(observation);
    auto end = std::chrono::high_resolution_clock::now();

    auto elapsed_seconds =
        std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
    state.SetIterationTime(elapsed_seconds.count());
  }
}
BENCHMARK(BM_Histogram_Observe)->Range(0, 4096);

static void BM_Histogram_Collect(benchmark::State& state) {
  using prometheus::BuildHistogram;
  using prometheus::Histogram;
  using prometheus::Registry;

  const auto number_of_buckets = state.range(0);

  Registry registry;
  auto& histogram_family =
      BuildHistogram().Name("benchmark_histogram").Help("").Register(registry);
  auto bucket_boundaries = CreateLinearBuckets(0, number_of_buckets - 1, 1);
  auto& histogram = histogram_family.Add({}, bucket_boundaries);

  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(histogram.Collect());
  }
}
BENCHMARK(BM_Histogram_Collect)->Range(0, 4096);
