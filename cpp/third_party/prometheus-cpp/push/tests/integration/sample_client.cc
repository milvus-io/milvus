#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <thread>

#include <prometheus/gateway.h>
#include <prometheus/registry.h>

#ifdef _WIN32
#include <Winsock2.h>
#else
#include <sys/param.h>
#include <unistd.h>
#endif

static std::string GetHostName() {
  char hostname[1024];

  if (::gethostname(hostname, sizeof(hostname))) {
    return {};
  }
  return hostname;
}

int main() {
  using namespace prometheus;

  // create a push gateway
  const auto labels = Gateway::GetInstanceLabel(GetHostName());

  Gateway gateway{"127.0.0.1", "9091", "sample_client", labels};

  // create a metrics registry with component=main labels applied to all its
  // metrics
  auto registry = std::make_shared<Registry>();

  // add a new counter family to the registry (families combine values with the
  // same name, but distinct label dimensions)
  auto& counter_family = BuildCounter()
                             .Name("time_running_seconds_total")
                             .Help("How many seconds is this server running?")
                             .Labels({{"label", "value"}})
                             .Register(*registry);

  // add a counter to the metric family
  auto& second_counter = counter_family.Add(
      {{"another_label", "value"}, {"yet_another_label", "value"}});

  // ask the pusher to push the metrics to the pushgateway
  gateway.RegisterCollectable(registry);

  for (;;) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
    // increment the counter by one (second)
    second_counter.Increment();

    // push metrics
    gateway.Push();
  }
  return 0;
}
