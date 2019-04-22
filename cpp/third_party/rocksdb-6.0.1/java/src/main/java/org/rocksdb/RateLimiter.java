// Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

package org.rocksdb;

/**
 * RateLimiter, which is used to control write rate of flush and
 * compaction.
 *
 * @since 3.10.0
 */
public class RateLimiter extends RocksObject {
  public static final long DEFAULT_REFILL_PERIOD_MICROS = 100 * 1000;
  public static final int DEFAULT_FAIRNESS = 10;
  public static final RateLimiterMode DEFAULT_MODE =
      RateLimiterMode.WRITES_ONLY;
  public static final boolean DEFAULT_AUTOTUNE = false;

  /**
   * RateLimiter constructor
   *
   * @param rateBytesPerSecond this is the only parameter you want to set
   *     most of the time. It controls the total write rate of compaction
   *     and flush in bytes per second. Currently, RocksDB does not enforce
   *     rate limit for anything other than flush and compaction, e.g. write to
   *     WAL.
   */
  public RateLimiter(final long rateBytesPerSecond) {
    this(rateBytesPerSecond, DEFAULT_REFILL_PERIOD_MICROS, DEFAULT_FAIRNESS,
        DEFAULT_MODE, DEFAULT_AUTOTUNE);
  }

  /**
   * RateLimiter constructor
   *
   * @param rateBytesPerSecond this is the only parameter you want to set
   *     most of the time. It controls the total write rate of compaction
   *     and flush in bytes per second. Currently, RocksDB does not enforce
   *     rate limit for anything other than flush and compaction, e.g. write to
   *     WAL.
   * @param refillPeriodMicros this controls how often tokens are refilled. For
   *     example,
   *     when rate_bytes_per_sec is set to 10MB/s and refill_period_us is set to
   *     100ms, then 1MB is refilled every 100ms internally. Larger value can
   *     lead to burstier writes while smaller value introduces more CPU
   *     overhead. The default of 100,000ms should work for most cases.
   */
  public RateLimiter(final long rateBytesPerSecond,
      final long refillPeriodMicros) {
    this(rateBytesPerSecond, refillPeriodMicros, DEFAULT_FAIRNESS, DEFAULT_MODE,
        DEFAULT_AUTOTUNE);
  }

  /**
   * RateLimiter constructor
   *
   * @param rateBytesPerSecond this is the only parameter you want to set
   *     most of the time. It controls the total write rate of compaction
   *     and flush in bytes per second. Currently, RocksDB does not enforce
   *     rate limit for anything other than flush and compaction, e.g. write to
   *     WAL.
   * @param refillPeriodMicros this controls how often tokens are refilled. For
   *     example,
   *     when rate_bytes_per_sec is set to 10MB/s and refill_period_us is set to
   *     100ms, then 1MB is refilled every 100ms internally. Larger value can
   *     lead to burstier writes while smaller value introduces more CPU
   *     overhead. The default of 100,000ms should work for most cases.
   * @param fairness RateLimiter accepts high-pri requests and low-pri requests.
   *     A low-pri request is usually blocked in favor of hi-pri request.
   *     Currently, RocksDB assigns low-pri to request from compaction and
   *     high-pri to request from flush. Low-pri requests can get blocked if
   *     flush requests come in continuously. This fairness parameter grants
   *     low-pri requests permission by fairness chance even though high-pri
   *     requests exist to avoid starvation.
   *     You should be good by leaving it at default 10.
   */
  public RateLimiter(final long rateBytesPerSecond,
      final long refillPeriodMicros, final int fairness) {
    this(rateBytesPerSecond, refillPeriodMicros, fairness, DEFAULT_MODE,
        DEFAULT_AUTOTUNE);
  }

  /**
   * RateLimiter constructor
   *
   * @param rateBytesPerSecond this is the only parameter you want to set
   *     most of the time. It controls the total write rate of compaction
   *     and flush in bytes per second. Currently, RocksDB does not enforce
   *     rate limit for anything other than flush and compaction, e.g. write to
   *     WAL.
   * @param refillPeriodMicros this controls how often tokens are refilled. For
   *     example,
   *     when rate_bytes_per_sec is set to 10MB/s and refill_period_us is set to
   *     100ms, then 1MB is refilled every 100ms internally. Larger value can
   *     lead to burstier writes while smaller value introduces more CPU
   *     overhead. The default of 100,000ms should work for most cases.
   * @param fairness RateLimiter accepts high-pri requests and low-pri requests.
   *     A low-pri request is usually blocked in favor of hi-pri request.
   *     Currently, RocksDB assigns low-pri to request from compaction and
   *     high-pri to request from flush. Low-pri requests can get blocked if
   *     flush requests come in continuously. This fairness parameter grants
   *     low-pri requests permission by fairness chance even though high-pri
   *     requests exist to avoid starvation.
   *     You should be good by leaving it at default 10.
   * @param rateLimiterMode indicates which types of operations count against
   *     the limit.
   */
  public RateLimiter(final long rateBytesPerSecond,
      final long refillPeriodMicros, final int fairness,
      final RateLimiterMode rateLimiterMode) {
    this(rateBytesPerSecond, refillPeriodMicros, fairness, rateLimiterMode,
        DEFAULT_AUTOTUNE);
  }

  /**
   * RateLimiter constructor
   *
   * @param rateBytesPerSecond this is the only parameter you want to set
   *     most of the time. It controls the total write rate of compaction
   *     and flush in bytes per second. Currently, RocksDB does not enforce
   *     rate limit for anything other than flush and compaction, e.g. write to
   *     WAL.
   * @param refillPeriodMicros this controls how often tokens are refilled. For
   *     example,
   *     when rate_bytes_per_sec is set to 10MB/s and refill_period_us is set to
   *     100ms, then 1MB is refilled every 100ms internally. Larger value can
   *     lead to burstier writes while smaller value introduces more CPU
   *     overhead. The default of 100,000ms should work for most cases.
   * @param fairness RateLimiter accepts high-pri requests and low-pri requests.
   *     A low-pri request is usually blocked in favor of hi-pri request.
   *     Currently, RocksDB assigns low-pri to request from compaction and
   *     high-pri to request from flush. Low-pri requests can get blocked if
   *     flush requests come in continuously. This fairness parameter grants
   *     low-pri requests permission by fairness chance even though high-pri
   *     requests exist to avoid starvation.
   *     You should be good by leaving it at default 10.
   * @param rateLimiterMode indicates which types of operations count against
   *     the limit.
   * @param autoTune Enables dynamic adjustment of rate limit within the range
   *     {@code [rate_bytes_per_sec / 20, rate_bytes_per_sec]}, according to
   *     the recent demand for background I/O.
   */
  public RateLimiter(final long rateBytesPerSecond,
      final long refillPeriodMicros, final int fairness,
      final RateLimiterMode rateLimiterMode, final boolean autoTune) {
    super(newRateLimiterHandle(rateBytesPerSecond,
        refillPeriodMicros, fairness, rateLimiterMode.getValue(), autoTune));
  }

  /**
   * <p>This API allows user to dynamically change rate limiter's bytes per second.
   * REQUIRED: bytes_per_second &gt; 0</p>
   *
   * @param bytesPerSecond bytes per second.
   */
  public void setBytesPerSecond(final long bytesPerSecond) {
    assert(isOwningHandle());
    setBytesPerSecond(nativeHandle_, bytesPerSecond);
  }

  /**
   * Returns the bytes per second.
   *
   * @return bytes per second.
   */
  public long getBytesPerSecond() {
    assert(isOwningHandle());
    return getBytesPerSecond(nativeHandle_);
  }

  /**
   * <p>Request for token to write bytes. If this request can not be satisfied,
   * the call is blocked. Caller is responsible to make sure
   * {@code bytes &lt; GetSingleBurstBytes()}.</p>
   *
   * @param bytes requested bytes.
   */
  public void request(final long bytes) {
    assert(isOwningHandle());
    request(nativeHandle_, bytes);
  }

  /**
   * <p>Max bytes can be granted in a single burst.</p>
   *
   * @return max bytes can be granted in a single burst.
   */
  public long getSingleBurstBytes() {
    assert(isOwningHandle());
    return getSingleBurstBytes(nativeHandle_);
  }

  /**
   * <p>Total bytes that go through rate limiter.</p>
   *
   * @return total bytes that go through rate limiter.
   */
  public long getTotalBytesThrough() {
    assert(isOwningHandle());
    return getTotalBytesThrough(nativeHandle_);
  }

  /**
   * <p>Total # of requests that go through rate limiter.</p>
   *
   * @return total # of requests that go through rate limiter.
   */
  public long getTotalRequests() {
    assert(isOwningHandle());
    return getTotalRequests(nativeHandle_);
  }

  private static native long newRateLimiterHandle(final long rateBytesPerSecond,
      final long refillPeriodMicros, final int fairness,
      final byte rateLimiterMode, final boolean autoTune);
  @Override protected final native void disposeInternal(final long handle);

  private native void setBytesPerSecond(final long handle,
      final long bytesPerSecond);
  private native long getBytesPerSecond(final long handle);
  private native void request(final long handle, final long bytes);
  private native long getSingleBurstBytes(final long handle);
  private native long getTotalBytesThrough(final long handle);
  private native long getTotalRequests(final long handle);
}
