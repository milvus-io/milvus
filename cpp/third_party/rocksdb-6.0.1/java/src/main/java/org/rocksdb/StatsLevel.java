// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * The level of Statistics to report.
 */
public enum StatsLevel {
    /**
     * Collect all stats except time inside mutex lock AND time spent on
     * compression.
     */
    EXCEPT_DETAILED_TIMERS((byte) 0x0),

    /**
     * Collect all stats except the counters requiring to get time inside the
     * mutex lock.
     */
    EXCEPT_TIME_FOR_MUTEX((byte) 0x1),

    /**
     * Collect all stats, including measuring duration of mutex operations.
     *
     * If getting time is expensive on the platform to run, it can
     * reduce scalability to more threads, especially for writes.
     */
    ALL((byte) 0x2);

    private final byte value;

    StatsLevel(final byte value) {
        this.value = value;
    }

    /**
     * <p>Returns the byte value of the enumerations value.</p>
     *
     * @return byte representation
     */
    public byte getValue() {
        return value;
    }

    /**
     * Get StatsLevel by byte value.
     *
     * @param value byte representation of StatsLevel.
     *
     * @return {@link org.rocksdb.StatsLevel} instance.
     * @throws java.lang.IllegalArgumentException if an invalid
     *     value is provided.
     */
    public static StatsLevel getStatsLevel(final byte value) {
        for (final StatsLevel statsLevel : StatsLevel.values()) {
            if (statsLevel.getValue() == value){
                return statsLevel;
            }
        }
        throw new IllegalArgumentException(
                "Illegal value provided for StatsLevel.");
    }
}
