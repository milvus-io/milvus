package balancer

import (
	"math"
	"time"

	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/resourcewatermark"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

const (
	capacityRecoveryMargin = 5.0 // percentage points, beyond measurement jitter
	capacityProbeInterval  = time.Minute
)

func WithResourceWatermarks(reader resourcewatermark.Reader) PolicyOption {
	return func(p *DefaultBalancePolicy) { p.resourceWatermarks = reader }
}

type capacityPlacement struct {
	planning *planningContext
	failures map[int64]map[coordview.FailedResourceKey]coordview.ResourceFailure
	keys     map[int64]coordview.FailedResourceKey
	reasons  map[int64]uint8
}

// Only failure-bearing shards allocate this state. Samples are pinned lazily,
// once per node in the entire planning pass, not once per collection/segment.
func newCapacityPlacement(p *planningContext, id qviews.ShardID, segments map[int64]*SegmentDataView, failures map[int64]map[coordview.FailedResourceKey]coordview.ResourceFailure) *capacityPlacement {
	if len(failures) == 0 {
		return nil
	}
	var data qviews.DataVersion
	var schema int32
	var loadInfo uint64
	versionsLoaded := false
	var result *capacityPlacement
	for _, node := range failures {
		for key, failure := range node {
			if !failure.ObservedAt.IsZero() && p.priorityNow.Sub(failure.ObservedAt) > coordview.CapacityFailureHistoryTTL {
				continue
			}
			segment := segments[key.SegmentID]
			if segment == nil {
				continue
			}
			if !versionsLoaded {
				c := p.collectionForShard(id)
				data, _ = p.DataVersionForCollection(c.ID())
				schema, loadInfo = p.SchemaVersion(c.ID()), p.ConfigVersion(c.ID())
				versionsLoaded = true
			}
			if coordview.FailureKey(segment.PartitionID, segment.SegmentID, segment.ManifestVersion, schema, loadInfo, data) != key {
				continue
			}
			if result == nil {
				result = &capacityPlacement{planning: p, failures: failures, keys: make(map[int64]coordview.FailedResourceKey), reasons: make(map[int64]uint8)}
			}
			result.keys[key.SegmentID] = key
			switch failure.Reason {
			case viewpb.SegmentPerparingFailure_MemoryLimitExceeded:
				result.reasons[key.SegmentID] |= 1
			case viewpb.SegmentPerparingFailure_DiskLimitExceeded:
				result.reasons[key.SegmentID] |= 2
			default:
				result.reasons[key.SegmentID] |= 3
			}
		}
	}
	return result
}

func (p *planningContext) resourceSample(node int64) resourcewatermark.Sample {
	if sample, ok := p.resourceSamples[node]; ok {
		return sample
	}
	var sample resourcewatermark.Sample
	if p.resourceWatermarks != nil {
		if value, ok := p.resourceWatermarks.Read(node, time.Now()); ok {
			sample = value
		}
	}
	if p.resourceSamples == nil {
		p.resourceSamples = make(map[int64]resourcewatermark.Sample)
	}
	p.resourceSamples[node] = sample
	return sample
}

func pressure(sample resourcewatermark.Sample, reason uint8) float64 {
	switch reason {
	case 1:
		return sample.Memory
	case 2:
		return sample.Disk
	default:
		return max(sample.Memory, sample.Disk)
	}
}

// Bounded pressure cost gives little preference between low-water nodes and a
// strong preference for headroom near saturation. Five-point bands suppress
// insignificant ranking changes; existing assignments are retained separately.
func pressureCost(level float64) float64 {
	level = min(100, math.Ceil(level/capacityRecoveryMargin)*capacityRecoveryMargin)
	return level / max(capacityRecoveryMargin, 100-level)
}

func (c *capacityPlacement) state(segment, node int64) (blocked bool, cost float64, version qviews.QueryViewVersion) {
	reason := c.reasons[segment]
	if reason == 0 {
		return false, 0, version
	}
	sample := c.planning.resourceSample(node)
	known := !sample.ReceivedAt.IsZero()
	level := 100.0 // unknown is not free capacity; still eligible for a bounded probe
	if known {
		level = pressure(sample, reason)
	}
	cost = pressureCost(level)
	failure, failed := c.failures[node][c.keys[segment]]
	if !failed {
		return known && level >= 100, cost, version
	}
	version = failure.Version
	// Fresh feedback can release a rejection before the bounded probe interval.
	// The dimension is local to this failure, not another node's failure reason.
	dimension := uint8(3)
	switch failure.Reason {
	case viewpb.SegmentPerparingFailure_MemoryLimitExceeded:
		dimension = 1
	case viewpb.SegmentPerparingFailure_DiskLimitExceeded:
		dimension = 2
	}
	recovered := known && !failure.Watermarks.ReceivedAt.IsZero() && pressure(sample, dimension)+capacityRecoveryMargin <= pressure(failure.Watermarks, dimension)
	probeDue := !failure.ObservedAt.IsZero() && c.planning.priorityNow.Sub(failure.ObservedAt) >= capacityProbeInterval
	return (known && level >= 100) || (!recovered && !probeDue), cost, version
}

func (ctx *allocationContext) capacityCost(segments []*SegmentDataView, destination int64) (float64, bool) {
	if ctx.capacity == nil {
		return 0, true
	}
	var cost float64
	for _, segment := range segments {
		if ctx.assignments[segment.SegmentID] == destination || ctx.reusable[segment.SegmentID][destination] {
			continue
		}
		blocked, risk, _ := ctx.capacity.state(segment.SegmentID, destination)
		if blocked {
			return 0, false
		}
		cost += ctx.config.LoadCostPerUnit * float64(segment.LogicalSize) * risk
	}
	return cost, true
}
