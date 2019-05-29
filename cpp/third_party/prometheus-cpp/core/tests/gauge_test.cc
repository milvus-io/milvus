#include "prometheus/gauge.h"

#include <gmock/gmock.h>

namespace prometheus {
namespace {

TEST(GaugeTest, initialize_with_zero) {
  Gauge gauge;
  EXPECT_EQ(gauge.Value(), 0);
}

TEST(GaugeTest, inc) {
  Gauge gauge;
  gauge.Increment();
  EXPECT_EQ(gauge.Value(), 1.0);
}

TEST(GaugeTest, inc_number) {
  Gauge gauge;
  gauge.Increment(4);
  EXPECT_EQ(gauge.Value(), 4.0);
}

TEST(GaugeTest, inc_multiple) {
  Gauge gauge;
  gauge.Increment();
  gauge.Increment();
  gauge.Increment(5);
  EXPECT_EQ(gauge.Value(), 7.0);
}

TEST(GaugeTest, inc_negative_value) {
  Gauge gauge;
  gauge.Increment(5.0);
  gauge.Increment(-5.0);
  EXPECT_EQ(gauge.Value(), 5.0);
}

TEST(GaugeTest, dec) {
  Gauge gauge;
  gauge.Set(5.0);
  gauge.Decrement();
  EXPECT_EQ(gauge.Value(), 4.0);
}

TEST(GaugeTest, dec_negative_value) {
  Gauge gauge;
  gauge.Set(5.0);
  gauge.Decrement(-1.0);
  EXPECT_EQ(gauge.Value(), 5.0);
}

TEST(GaugeTest, dec_number) {
  Gauge gauge;
  gauge.Set(5.0);
  gauge.Decrement(3.0);
  EXPECT_EQ(gauge.Value(), 2.0);
}

TEST(GaugeTest, set) {
  Gauge gauge;
  gauge.Set(3.0);
  EXPECT_EQ(gauge.Value(), 3.0);
}

TEST(GaugeTest, set_multiple) {
  Gauge gauge;
  gauge.Set(3.0);
  gauge.Set(8.0);
  gauge.Set(1.0);
  EXPECT_EQ(gauge.Value(), 1.0);
}

TEST(GaugeTest, set_to_current_time) {
  Gauge gauge;
  gauge.SetToCurrentTime();
  EXPECT_GT(gauge.Value(), 0.0);
}

}  // namespace
}  // namespace prometheus
