package shards

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	GenerateLimitation(lv datapb.SegmentLevel, schema *schemapb.CollectionSchema) segmentLimitation
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
func (p jitterSegmentLimitationPolicy) GenerateLimitation(lv datapb.SegmentLevel, schema *schemapb.CollectionSchema) segmentLimitation {
	switch lv {
	case datapb.SegmentLevel_L0:
		return p.generateL0Limitation()
	case datapb.SegmentLevel_L1:
		return p.generateL1Limitation(schema)
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
func (p jitterSegmentLimitationPolicy) generateL1Limitation(schema *schemapb.CollectionSchema) segmentLimitation {
	// TODO: It's weird to set such a parameter into datacoord configuration.
	// Refactor it in the future
	jitterRatio := p.getJitterRatio()
	maxSegmentSize := uint64(paramtable.Get().DataCoordCfg.SegmentMaxSize.GetAsInt64() * 1024 * 1024)
	proportion := paramtable.Get().DataCoordCfg.SegmentSealProportion.GetAsFloat()
	segmentSize := uint64(jitterRatio * float64(maxSegmentSize) * proportion)

	// Under the mainIndex metric the budget (SegmentSize) is interpreted in
	// main-index-column bytes. Bind MaxRows to the row-cap formula so the
	// persisted row cap survives recovery and the seal budget is enforced even
	// before the seal-specific accumulator rebuilds.
	rows := uint64(math.MaxUint64)
	if typeutil.IsMainIndexSizeMetric(paramtable.Get().DataCoordCfg.SizeMetric.GetValue()) {
		if r, ok := estimateMainIndexMaxRows(schema, jitterRatio, maxSegmentSize, proportion); ok {
			rows = r
		}
	}
	return segmentLimitation{
		PolicyName:  "jitter_segment_limitation",
		SegmentRows: rows,
		SegmentSize: segmentSize,
	}
}

// estimateMainIndexMaxRows computes the row cap
// min(proportion×maxSize / mainIndexPerRecord, ceiling / wholeRowPerRecord)
// from the schema. Returns false when no fixed-dim dense vector exists, in
// which case the caller falls back to whole-row semantics.
func estimateMainIndexMaxRows(schema *schemapb.CollectionSchema, jitterRatio float64, maxSegmentSize uint64, proportion float64) (uint64, bool) {
	if schema == nil {
		return 0, false
	}
	mainIndexPerRecord, err := typeutil.EstimateMainIndexSizePerRecord(schema)
	if err != nil || mainIndexPerRecord <= 0 {
		return 0, false
	}
	budgetRows := uint64(jitterRatio * float64(maxSegmentSize) * proportion / float64(mainIndexPerRecord))
	if ceilingMB := paramtable.Get().DataCoordCfg.MaxFullSegmentSize.GetAsInt64(); ceilingMB > 0 {
		wholeRowPerRecord, err := typeutil.EstimateSizePerRecord(schema)
		if err == nil && wholeRowPerRecord > 0 {
			ceilingRows := uint64(ceilingMB) * 1024 * 1024 / uint64(wholeRowPerRecord)
			if ceilingRows < budgetRows {
				budgetRows = ceilingRows
			}
		}
	}
	return budgetRows, true
}

func (p jitterSegmentLimitationPolicy) getJitterRatio() float64 {
	jitter := paramtable.Get().DataCoordCfg.SegmentSealProportionJitter.GetAsFloat()
	jitterRatio := 1 - jitter*rand.Float64() // generate a random number in [1-jitter, 1]
	if jitterRatio <= 0 || jitterRatio > 1 {
		jitterRatio = 1
	}
	return jitterRatio
}
