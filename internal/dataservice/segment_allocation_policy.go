package dataservice

import (
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type calUpperLimitPolicy interface {
	// apply accept collection schema and return max number of rows per segment
	apply(schema *schemapb.CollectionSchema) (int, error)
}

type calBySchemaPolicy struct {
}

func (p *calBySchemaPolicy) apply(schema *schemapb.CollectionSchema) (int, error) {
	sizePerRecord, err := typeutil.EstimateSizePerRecord(schema)
	if err != nil {
		return -1, err
	}
	threshold := Params.SegmentSize * 1024 * 1024
	return int(threshold / float64(sizePerRecord)), nil
}

func newCalBySchemaPolicy() calUpperLimitPolicy {
	return &calBySchemaPolicy{}
}

type allocatePolicy interface {
	apply(maxCount, writtenCount, allocatedCount, count int64) bool
}

type allocatePolicyV1 struct {
}

func (p *allocatePolicyV1) apply(maxCount, writtenCount, allocatedCount, count int64) bool {
	free := maxCount - writtenCount - allocatedCount
	return free >= count
}

func newAllocatePolicyV1() allocatePolicy {
	return &allocatePolicyV1{}
}

type sealPolicy interface {
	apply(maxCount, writtenCount, allocatedCount int64) bool
}

type sealPolicyV1 struct {
}

func (p *sealPolicyV1) apply(maxCount, writtenCount, allocatedCount int64) bool {
	return float64(writtenCount) >= Params.SegmentSizeFactor*float64(maxCount)
}

func newSealPolicyV1() sealPolicy {
	return &sealPolicyV1{}
}

type flushPolicy interface {
	apply(status *segmentStatus, t Timestamp) bool
}

type flushPolicyV1 struct {
}

func (p *flushPolicyV1) apply(status *segmentStatus, t Timestamp) bool {
	return status.sealed && status.lastExpireTime <= t
}

func newFlushPolicyV1() flushPolicy {
	return &flushPolicyV1{}
}
