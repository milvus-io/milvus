#include "prometheus/histogram.h"

#include <limits>

#include <gmock/gmock.h>

namespace prometheus {
namespace {

TEST(HistogramTest, initialize_with_zero) {
  Histogram histogram{{}};
  auto metric = histogram.Collect();
  auto h = metric.histogram;
  EXPECT_EQ(h.sample_count, 0U);
  EXPECT_EQ(h.sample_sum, 0);
}

TEST(HistogramTest, sample_count) {
  Histogram histogram{{1}};
  histogram.Observe(0);
  histogram.Observe(200);
  auto metric = histogram.Collect();
  auto h = metric.histogram;
  EXPECT_EQ(h.sample_count, 2U);
}

TEST(HistogramTest, sample_sum) {
  Histogram histogram{{1}};
  histogram.Observe(0);
  histogram.Observe(1);
  histogram.Observe(101);
  auto metric = histogram.Collect();
  auto h = metric.histogram;
  EXPECT_EQ(h.sample_sum, 102);
}

TEST(HistogramTest, bucket_size) {
  Histogram histogram{{1, 2}};
  auto metric = histogram.Collect();
  auto h = metric.histogram;
  EXPECT_EQ(h.bucket.size(), 3U);
}

TEST(HistogramTest, bucket_bounds) {
  Histogram histogram{{1, 2}};
  auto metric = histogram.Collect();
  auto h = metric.histogram;
  EXPECT_EQ(h.bucket.at(0).upper_bound, 1);
  EXPECT_EQ(h.bucket.at(1).upper_bound, 2);
  EXPECT_EQ(h.bucket.at(2).upper_bound,
            std::numeric_limits<double>::infinity());
}

TEST(HistogramTest, bucket_counts_not_reset_by_collection) {
  Histogram histogram{{1, 2}};
  histogram.Observe(1.5);
  histogram.Collect();
  histogram.Observe(1.5);
  auto metric = histogram.Collect();
  auto h = metric.histogram;
  ASSERT_EQ(h.bucket.size(), 3U);
  EXPECT_EQ(h.bucket.at(1).cumulative_count, 2U);
}

TEST(HistogramTest, cumulative_bucket_count) {
  Histogram histogram{{1, 2}};
  histogram.Observe(0);
  histogram.Observe(0.5);
  histogram.Observe(1);
  histogram.Observe(1.5);
  histogram.Observe(1.5);
  histogram.Observe(2);
  histogram.Observe(3);
  auto metric = histogram.Collect();
  auto h = metric.histogram;
  ASSERT_EQ(h.bucket.size(), 3U);
  EXPECT_EQ(h.bucket.at(0).cumulative_count, 3U);
  EXPECT_EQ(h.bucket.at(1).cumulative_count, 6U);
  EXPECT_EQ(h.bucket.at(2).cumulative_count, 7U);
}

}  // namespace
}  // namespace prometheus
