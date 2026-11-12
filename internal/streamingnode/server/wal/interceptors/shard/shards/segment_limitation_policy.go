package shards

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// getSegmentLimitationPolicy returns the segment limitation policy.
func getSegmentLimitationPolicy() SegmentLimitationPolicy {
	// TODO: dynamic policy can be applied here in future.
	return jitterSegmentLimitationPolicy{}
}

// segmentLimitation is the limitation of the segment.
type segmentLimitation struct {
	PolicyName  string
	SegmentRows uint64
	SegmentSize uint64
}

// SegmentLimitationPolicy is the interface to generate the limitation of the segment.
type SegmentLimitationPolicy interface {
	// GenerateLimitation generates the limitation of the segment.
	// An L1 segment is sealed on rows as well as on bytes. maxRowsOverride is
	// the collection.segment.maxRows property of the collection, or a
	// negative value when it is not set and dataCoord.segment.maxRows
	// applies; 0 means no row limit.
	GenerateLimitation(lv datapb.SegmentLevel, maxRowsOverride int64) segmentLimitation
}

// jitterSegmentLimitationPolicyExtraInfo is the extra info of the jitter segment limitation policy.
type jitterSegmentLimitationPolicyExtraInfo struct {
	JitterRatio    float64
	Proportion     float64
	MaxSegmentSize uint64
}

// jiiterSegmentLimitationPolicy is the policy to generate the limitation of the segment.
// Add a jitter to the segment size limitation to scatter the segment sealing time.
type jitterSegmentLimitationPolicy struct{}

// GenerateLimitation generates the limitation of the segment.
func (p jitterSegmentLimitationPolicy) GenerateLimitation(lv datapb.SegmentLevel, maxRowsOverride int64) segmentLimitation {
	switch lv {
	case datapb.SegmentLevel_L0:
		return p.generateL0Limitation()
	case datapb.SegmentLevel_L1:
		return p.generateL1Limitation(maxRowsOverride)
	default:
		panic(fmt.Sprintf("invalid segment level: %s", lv))
	}
}

// generateL0Limitation generates the limitation of the L0 segment.
func (p jitterSegmentLimitationPolicy) generateL0Limitation() segmentLimitation {
	rows := paramtable.Get().StreamingCfg.FlushL0MaxRowNum.GetAsUint64()
	size := paramtable.Get().StreamingCfg.FlushL0MaxSize.GetAsUint64()
	jitterRatio := p.getJitterRatio()

	if rows <= 0 {
		rows = uint64(math.MaxUint64)
	} else {
		rows = uint64(jitterRatio * float64(rows))
	}
	if size <= 0 {
		size = uint64(math.MaxUint64)
	} else {
		size = uint64(jitterRatio * float64(size))
	}
	return segmentLimitation{
		PolicyName:  "jitter_segment_limitation",
		SegmentRows: rows,
		SegmentSize: size,
	}
}

// generateL1Limitation generates the limitation of the L1 segment: the seal
// proportion of the segment size, and the row cap of the collection
// (maxRowsOverride, or dataCoord.segment.maxRows when it is negative), both
// with the same jitter. A row cap of 0 leaves the segment sealed on bytes only.
//
// The seal proportion is not applied to the row limit. It exists to keep a
// growing segment, which is held in memory, well below maxSize bytes; a
// segment of narrow rows is small in bytes at maxRows rows, and sealing it
// earlier would only produce segments that compaction has to merge back.
func (p jitterSegmentLimitationPolicy) generateL1Limitation(maxRowsOverride int64) segmentLimitation {
	// TODO: It's weird to set such a parameter into datacoord configuration.
	// Refactor it in the future
	jitterRatio := p.getJitterRatio()
	maxSegmentSize := uint64(paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024)
	proportion := paramtable.Get().DataCoordCfg.SegmentSealProportion.GetAsFloat()
	segmentSize := uint64(jitterRatio * float64(maxSegmentSize) * proportion)
	segmentRows := uint64(math.MaxUint64)
	maxRows := maxRowsOverride
	if maxRows < 0 {
		maxRows = paramtable.Get().DataCoordCfg.SegmentMaxRows.GetAsInt64()
	}
	if maxRows > 0 {
		segmentRows = max(uint64(jitterRatio*float64(maxRows)), 1)
	}
	return segmentLimitation{
		PolicyName:  "jitter_segment_limitation",
		SegmentRows: segmentRows,
		SegmentSize: segmentSize,
	}
}

func (p jitterSegmentLimitationPolicy) getJitterRatio() float64 {
	jitter := paramtable.Get().DataCoordCfg.SegmentSealProportionJitter.GetAsFloat()
	jitterRatio := 1 - jitter*rand.Float64() // generate a random number in [1-jitter, 1]
	if jitterRatio <= 0 || jitterRatio > 1 {
		jitterRatio = 1
	}
	return jitterRatio
}
