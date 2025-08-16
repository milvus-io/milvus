package shards

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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
	GenerateLimitation(lv datapb.SegmentLevel) segmentLimitation
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
func (p jitterSegmentLimitationPolicy) GenerateLimitation(lv datapb.SegmentLevel) segmentLimitation {
	switch lv {
	case datapb.SegmentLevel_L0:
		return p.generateL0Limitation()
	case datapb.SegmentLevel_L1:
		return p.generateL1Limitation()
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

// generateL1Limitation generates the limitation of the L1 segment.
func (p jitterSegmentLimitationPolicy) generateL1Limitation() segmentLimitation {
	// TODO: It's weird to set such a parameter into datacoord configuration.
	// Refactor it in the future
	jitterRatio := p.getJitterRatio()
	maxSegmentSize := uint64(paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024)
	proportion := paramtable.Get().DataCoordCfg.SegmentSealProportion.GetAsFloat()
	segmentSize := uint64(jitterRatio * float64(maxSegmentSize) * proportion)
	return segmentLimitation{
		PolicyName:  "jitter_segment_limitation",
		SegmentRows: math.MaxUint64,
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
