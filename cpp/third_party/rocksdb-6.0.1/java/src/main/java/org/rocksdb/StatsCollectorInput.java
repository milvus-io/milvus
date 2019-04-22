// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * Contains all information necessary to collect statistics from one instance
 * of DB statistics.
 */
public class StatsCollectorInput {
  private final Statistics _statistics;
  private final StatisticsCollectorCallback _statsCallback;

  /**
   * Constructor for StatsCollectorInput.
   *
   * @param statistics Reference of DB statistics.
   * @param statsCallback Reference of statistics callback interface.
   */
  public StatsCollectorInput(final Statistics statistics,
      final StatisticsCollectorCallback statsCallback) {
    _statistics = statistics;
    _statsCallback = statsCallback;
  }

  public Statistics getStatistics() {
    return _statistics;
  }

  public StatisticsCollectorCallback getCallback() {
    return _statsCallback;
  }
}
