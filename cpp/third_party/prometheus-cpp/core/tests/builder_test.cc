#include "prometheus/counter.h"
#include "prometheus/gauge.h"
#include "prometheus/histogram.h"
#include "prometheus/registry.h"
#include "prometheus/summary.h"

#include <gmock/gmock.h>
#include <algorithm>

namespace prometheus {
namespace {

class BuilderTest : public testing::Test {
 protected:
  std::vector<ClientMetric::Label> getExpectedLabels() {
    std::vector<ClientMetric::Label> labels;

    auto gen = [](std::pair<const std::string, std::string> p) {
      return ClientMetric::Label{p.first, p.second};
    };

    std::transform(std::begin(const_labels), std::end(const_labels),
                   std::back_inserter(labels), gen);
    std::transform(std::begin(more_labels), std::end(more_labels),
                   std::back_inserter(labels), gen);

    return labels;
  }

  void verifyCollectedLabels() {
    const auto collected = registry.Collect();

    ASSERT_EQ(1U, collected.size());
    EXPECT_EQ(name, collected.at(0).name);
    EXPECT_EQ(help, collected.at(0).help);
    ASSERT_EQ(1U, collected.at(0).metric.size());

    EXPECT_THAT(collected.at(0).metric.at(0).label,
                testing::UnorderedElementsAreArray(expected_labels));
  }

  Registry registry;

  const std::string name = "some_name";
  const std::string help = "Additional description.";
  const std::map<std::string, std::string> const_labels = {{"key", "value"}};
  const std::map<std::string, std::string> more_labels = {{"name", "test"}};
  const std::vector<ClientMetric::Label> expected_labels = getExpectedLabels();
};

TEST_F(BuilderTest, build_counter) {
  auto& family = BuildCounter()
                     .Name(name)
                     .Help(help)
                     .Labels(const_labels)
                     .Register(registry);
  family.Add(more_labels);

  verifyCollectedLabels();
}

TEST_F(BuilderTest, build_gauge) {
  auto& family = BuildGauge()
                     .Name(name)
                     .Help(help)
                     .Labels(const_labels)
                     .Register(registry);
  family.Add(more_labels);

  verifyCollectedLabels();
}

TEST_F(BuilderTest, build_histogram) {
  auto& family = BuildHistogram()
                     .Name(name)
                     .Help(help)
                     .Labels(const_labels)
                     .Register(registry);
  family.Add(more_labels, Histogram::BucketBoundaries{1, 2});

  verifyCollectedLabels();
}

TEST_F(BuilderTest, build_summary) {
  auto& family = BuildSummary()
                     .Name(name)
                     .Help(help)
                     .Labels(const_labels)
                     .Register(registry);
  family.Add(more_labels, Summary::Quantiles{});

  verifyCollectedLabels();
}

}  // namespace
}  // namespace prometheus
