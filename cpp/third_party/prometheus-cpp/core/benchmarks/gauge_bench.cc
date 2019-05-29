#include <benchmark/benchmark.h>
#include <prometheus/registry.h>

static void BM_Gauge_Increment(benchmark::State& state) {
  using prometheus::BuildGauge;
  using prometheus::Gauge;
  using prometheus::Registry;
  Registry registry;
  auto& gauge_family =
      BuildGauge().Name("benchmark_gauge").Help("").Register(registry);
  auto& gauge = gauge_family.Add({});

  while (state.KeepRunning()) gauge.Increment(2);
}
BENCHMARK(BM_Gauge_Increment);

static void BM_Gauge_Decrement(benchmark::State& state) {
  using prometheus::BuildGauge;
  using prometheus::Gauge;
  using prometheus::Registry;
  Registry registry;
  auto& gauge_family =
      BuildGauge().Name("benchmark_gauge").Help("").Register(registry);
  auto& gauge = gauge_family.Add({});

  while (state.KeepRunning()) gauge.Decrement(2);
}
BENCHMARK(BM_Gauge_Decrement);

static void BM_Gauge_SetToCurrentTime(benchmark::State& state) {
  using prometheus::BuildGauge;
  using prometheus::Gauge;
  using prometheus::Registry;
  Registry registry;
  auto& gauge_family =
      BuildGauge().Name("benchmark_gauge").Help("").Register(registry);
  auto& gauge = gauge_family.Add({});

  while (state.KeepRunning()) gauge.SetToCurrentTime();
}
BENCHMARK(BM_Gauge_SetToCurrentTime);

static void BM_Gauge_Collect(benchmark::State& state) {
  using prometheus::BuildGauge;
  using prometheus::Gauge;
  using prometheus::Registry;
  Registry registry;
  auto& gauge_family =
      BuildGauge().Name("benchmark_gauge").Help("").Register(registry);
  auto& gauge = gauge_family.Add({});

  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(gauge.Collect());
  };
}
BENCHMARK(BM_Gauge_Collect);
