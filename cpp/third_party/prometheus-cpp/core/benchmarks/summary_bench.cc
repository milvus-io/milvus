#include <chrono>
#include <random>

#include <benchmark/benchmark.h>
#include <prometheus/registry.h>

using prometheus::Summary;

static const auto ITERATIONS = 262144;

static Summary::Quantiles CreateLinearQuantiles(int count) {
  static auto generator = [](double x) {
    static auto exp = [](double x) {
      static const double A = 2;
      return 1 - std::exp(-A * x);
    };

    return exp(x) / exp(1);
  };

  auto quantiles = Summary::Quantiles{};
  for (auto i = 0; i < count; ++i) {
    quantiles.emplace_back(generator(double(i) / count), 0.01);
  }
  return quantiles;
}

static void BM_Summary_Observe(benchmark::State& state) {
  using prometheus::BuildSummary;
  using prometheus::Registry;
  using prometheus::Summary;

  const auto number_of_quantiles = state.range(0);

  Registry registry;
  auto& summary_family =
      BuildSummary().Name("benchmark_summary").Help("").Register(registry);
  auto quantiles = CreateLinearQuantiles(number_of_quantiles);
  auto& summary = summary_family.Add({}, quantiles);
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> d(0, 100);

  while (state.KeepRunning()) {
    auto observation = d(gen);
    auto start = std::chrono::high_resolution_clock::now();
    summary.Observe(observation);
    auto end = std::chrono::high_resolution_clock::now();

    auto elapsed_seconds =
        std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
    state.SetIterationTime(elapsed_seconds.count());
  }
}
BENCHMARK(BM_Summary_Observe)->Range(0, 64)->Iterations(ITERATIONS);

static void BM_Summary_Collect(benchmark::State& state) {
  using prometheus::BuildSummary;
  using prometheus::Registry;
  using prometheus::Summary;

  const auto number_of_quantiles = state.range(0);
  const auto number_of_entries = state.range(1);

  Registry registry;
  auto& summary_family =
      BuildSummary().Name("benchmark_summary").Help("").Register(registry);
  auto quantiles = CreateLinearQuantiles(number_of_quantiles);
  auto& summary = summary_family.Add({}, quantiles);

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> d(0, 100);
  for (auto i = 1; i <= number_of_entries; ++i) summary.Observe(d(gen));

  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(summary.Collect());
  }
}
BENCHMARK(BM_Summary_Collect)->RangePair(0, 64, 0, ITERATIONS);

static void BM_Summary_Observe_Common(benchmark::State& state) {
  using prometheus::BuildSummary;
  using prometheus::Registry;
  using prometheus::Summary;

  Registry registry;
  auto& summary_family =
      BuildSummary().Name("benchmark_summary").Help("").Register(registry);
  auto& summary = summary_family.Add(
      {}, Summary::Quantiles{
              {0.5, 0.05}, {0.9, 0.01}, {0.95, 0.005}, {0.99, 0.001}});
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> d(0, 100);

  while (state.KeepRunning()) {
    auto observation = d(gen);
    auto start = std::chrono::high_resolution_clock::now();
    summary.Observe(observation);
    auto end = std::chrono::high_resolution_clock::now();

    auto elapsed_seconds =
        std::chrono::duration_cast<std::chrono::duration<double>>(end - start);
    state.SetIterationTime(elapsed_seconds.count());
  }
}
BENCHMARK(BM_Summary_Observe_Common)->Iterations(ITERATIONS);

static void BM_Summary_Collect_Common(benchmark::State& state) {
  using prometheus::BuildSummary;
  using prometheus::Registry;
  using prometheus::Summary;

  const auto number_of_entries = state.range(0);

  Registry registry;
  auto& summary_family =
      BuildSummary().Name("benchmark_summary").Help("").Register(registry);
  auto& summary = summary_family.Add(
      {}, Summary::Quantiles{
              {0.5, 0.05}, {0.9, 0.01}, {0.95, 0.005}, {0.99, 0.001}});

  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> d(0, 100);
  for (auto i = 1; i <= number_of_entries; ++i) summary.Observe(d(gen));

  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(summary.Collect());
  }
}
BENCHMARK(BM_Summary_Collect_Common)->Range(0, ITERATIONS);
