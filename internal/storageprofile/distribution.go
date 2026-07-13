// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storageprofile

import (
	"math"
	"time"
)

var latencyBucketUpperBoundsNanos = [...]uint64{
	250_000, 500_000, 1_000_000, 2_000_000, 5_000_000, 10_000_000,
	25_000_000, 50_000_000, 100_000_000, 250_000_000, 500_000_000,
	1_000_000_000, 2_500_000_000, 5_000_000_000, 10_000_000_000,
	30_000_000_000, 60_000_000_000, 120_000_000_000, 300_000_000_000,
	math.MaxUint64,
}

var sizeBucketUpperBounds = [...]uint64{
	1 << 10, 4 << 10, 16 << 10, 64 << 10, 256 << 10,
	1 << 20, 4 << 20, 16 << 20, 64 << 20, 256 << 20,
	1 << 30, 4 << 30, math.MaxUint64,
}

type LatencyDistribution struct {
	Count    uint64
	SumNanos uint64
	MinNanos uint64
	MaxNanos uint64
	Buckets  [LatencyBucketCount]uint64
}

func (d *LatencyDistribution) Observe(duration time.Duration) {
	nanos := uint64(max(duration.Nanoseconds(), 0))
	d.ObserveNanos(nanos)
}

func (d *LatencyDistribution) ObserveNanos(nanos uint64) {
	d.Count++
	d.SumNanos += nanos
	if d.Count == 1 || nanos < d.MinNanos {
		d.MinNanos = nanos
	}
	if nanos > d.MaxNanos {
		d.MaxNanos = nanos
	}
	for bucketIndex, upperBound := range latencyBucketUpperBoundsNanos {
		if nanos <= upperBound {
			d.Buckets[bucketIndex]++
			break
		}
	}
}

func (d *LatencyDistribution) Merge(other LatencyDistribution) {
	if other.Count == 0 {
		return
	}
	if d.Count == 0 || other.MinNanos < d.MinNanos {
		d.MinNanos = other.MinNanos
	}
	if other.MaxNanos > d.MaxNanos {
		d.MaxNanos = other.MaxNanos
	}
	d.Count += other.Count
	d.SumNanos += other.SumNanos
	for bucketIndex := range d.Buckets {
		d.Buckets[bucketIndex] += other.Buckets[bucketIndex]
	}
}

func (d LatencyDistribution) Average() (time.Duration, bool) {
	if d.Count == 0 {
		return 0, false
	}
	return time.Duration(d.SumNanos / d.Count), true
}

func (d LatencyDistribution) Quantile(q float64) (time.Duration, bool) {
	if d.Count == 0 || q < 0 || q > 1 || math.IsNaN(q) {
		return 0, false
	}
	target := uint64(math.Ceil(q * float64(d.Count)))
	if target == 0 {
		target = 1
	}
	var cumulative uint64
	for bucketIndex, count := range d.Buckets {
		cumulative += count
		if cumulative >= target {
			upperBound := latencyBucketUpperBoundsNanos[bucketIndex]
			if upperBound == math.MaxUint64 {
				upperBound = d.MaxNanos
			}
			return time.Duration(upperBound), true
		}
	}
	return time.Duration(d.MaxNanos), true
}

func (d LatencyDistribution) CumulativeBuckets() [LatencyBucketCount]uint64 {
	var result [LatencyBucketCount]uint64
	var cumulative uint64
	for bucketIndex, count := range d.Buckets {
		cumulative += count
		result[bucketIndex] = cumulative
	}
	return result
}

type SizeDistribution struct {
	Count   uint64
	Sum     uint64
	Min     uint64
	Max     uint64
	Buckets [SizeBucketCount]uint64
}

func (d *SizeDistribution) Observe(size uint64) {
	d.Count++
	d.Sum += size
	if d.Count == 1 || size < d.Min {
		d.Min = size
	}
	if size > d.Max {
		d.Max = size
	}
	for bucketIndex, upperBound := range sizeBucketUpperBounds {
		if size <= upperBound {
			d.Buckets[bucketIndex]++
			break
		}
	}
}

func (d *SizeDistribution) Merge(other SizeDistribution) {
	if other.Count == 0 {
		return
	}
	if d.Count == 0 || other.Min < d.Min {
		d.Min = other.Min
	}
	if other.Max > d.Max {
		d.Max = other.Max
	}
	d.Count += other.Count
	d.Sum += other.Sum
	for bucketIndex := range d.Buckets {
		d.Buckets[bucketIndex] += other.Buckets[bucketIndex]
	}
}
