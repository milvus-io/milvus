---
title: Auto-tuned Rate Limiter
layout: post
author: ajkr
category: blog
---

### Introduction

Our rate limiter has been hard to configure since users need to pick a value that is low enough to prevent background I/O spikes, which can impact user-visible read/write latencies. Meanwhile, picking too low a value can cause memtables and L0 files to pile up, eventually leading to writes stalling. Tuning the rate limiter has been especially difficult for users whose DB instances have different workloads, or have workloads that vary over time, or commonly both.

To address this, in RocksDB 5.9 we released a dynamic rate limiter that adjusts itself over time according to demand for background I/O. It can be enabled simply by passing `auto_tuned=true` in the `NewGenericRateLimiter()` call. In this case `rate_bytes_per_sec` will indicate the upper-bound of the window within which a rate limit will be picked dynamically. The chosen rate limit will be much lower unless absolutely necessary, so setting this to the device's maximum throughput is a reasonable choice on dedicated hosts.

### Algorithm

We use a simple multiplicative-increase, multiplicative-decrease algorithm. We measure demand for background I/O as the ratio of intervals where the rate limiter is drained. There are low and high watermarks for this ratio, which will trigger a change in rate limit when breached. The rate limit can move within a window bounded by the user-specified upper-bound, and a lower-bound that we derive internally. Users can expect this lower bound to be 1-2 orders of magnitude less than the provided upper-bound (so don't provide INT64_MAX as your upper-bound), although it's subject to change.

### Benchmark Results

Data is ingested at 10MB/s and the rate limiter was created with 1000MB/s as its upper bound. The dynamically chosen rate limit hovers around 125MB/s. The other clustering of points at 50MB/s is due to number of compaction threads being reduced to one when there's no compaction pressure. 

![](/static/images/rate-limiter/write-KBps-series.png)

![](/static/images/rate-limiter/auto-tuned-write-KBps-series.png)

The following graph summarizes the above two time series graphs in CDF form. In particular, notice the p90 - p100 for background write rate are significantly lower with auto-tuned rate limiter enabled.

![](/static/images/rate-limiter/write-KBps-cdf.png)
