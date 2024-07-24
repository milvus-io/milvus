package policy

import (
	"math/rand"

	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// GetSegmentLimitationPolicy returns the segment limitation policy.
func GetSegmentLimitationPolicy() SegmentLimitationPolicy {
	// TODO: dynamic policy can be applied here in future.
	return jitterSegmentLimitationPolicy{}
}

// SegmentLimitation is the limitation of the segment.
type SegmentLimitation struct {
	PolicyName  string
	SegmentSize uint64
	ExtraInfo   interface{}
}

// SegmentLimitationPolicy is the interface to generate the limitation of the segment.
type SegmentLimitationPolicy interface {
	// GenerateLimitation generates the limitation of the segment.
	GenerateLimitation() SegmentLimitation
}

// jitterSegmentLimitationPolicyExtraInfo is the extra info of the jitter segment limitation policy.
type jitterSegmentLimitationPolicyExtraInfo struct {
	Jitter         float64
	JitterRatio    float64
	MaxSegmentSize uint64
}

// jiiterSegmentLimitationPolicy is the policy to generate the limitation of the segment.
// Add a jitter to the segment size limitation to scatter the segment sealing time.
type jitterSegmentLimitationPolicy struct{}

// GenerateLimitation generates the limitation of the segment.
func (p jitterSegmentLimitationPolicy) GenerateLimitation() SegmentLimitation {
	// TODO: It's weird to set such a parameter into datacoord configuration.
	// Refactor it in the future
	jitter := paramtable.Get().DataCoordCfg.SegmentSealProportionJitter.GetAsFloat()
	jitterRatio := 1 - jitter*rand.Float64() // generate a random number in [1-jitter, 1]
	if jitterRatio <= 0 || jitterRatio > 1 {
		jitterRatio = 1
	}
	maxSegmentSize := uint64(paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024)
	segmentSize := uint64(jitterRatio * float64(maxSegmentSize))
	return SegmentLimitation{
		PolicyName:  "jitter_segment_limitation",
		SegmentSize: segmentSize,
		ExtraInfo: jitterSegmentLimitationPolicyExtraInfo{
			Jitter:         jitter,
			JitterRatio:    jitterRatio,
			MaxSegmentSize: maxSegmentSize,
		},
	}
}
