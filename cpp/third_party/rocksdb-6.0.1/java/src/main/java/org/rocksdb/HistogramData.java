// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

public class HistogramData {
  private final double median_;
  private final double percentile95_;
  private final double percentile99_;
  private final double average_;
  private final double standardDeviation_;
  private final double max_;
  private final long count_;
  private final long sum_;
  private final double min_;

  public HistogramData(final double median, final double percentile95,
                       final double percentile99, final double average,
                       final double standardDeviation) {
    this(median, percentile95, percentile99, average, standardDeviation, 0.0, 0, 0, 0.0);
  }

  public HistogramData(final double median, final double percentile95,
      final double percentile99, final double average,
      final double standardDeviation, final double max, final long count,
      final long sum, final double min) {
    median_ = median;
    percentile95_ = percentile95;
    percentile99_ = percentile99;
    average_ = average;
    standardDeviation_ = standardDeviation;
    min_ = min;
    max_ = max;
    count_ = count;
    sum_ = sum;
  }

  public double getMedian() {
    return median_;
  }

  public double getPercentile95() {
    return percentile95_;
  }

  public double getPercentile99() {
    return percentile99_;
  }

  public double getAverage() {
    return average_;
  }

  public double getStandardDeviation() {
    return standardDeviation_;
  }

  public double getMax() {
    return max_;
  }

  public long getCount() {
    return count_;
  }

  public long getSum() {
    return sum_;
  }

  public double getMin() {
    return min_;
  }
}
