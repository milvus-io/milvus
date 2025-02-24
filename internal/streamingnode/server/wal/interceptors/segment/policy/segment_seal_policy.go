package policy

import (
	"time"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/segment/stats"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type PolicyName string

var (
	PolicyNamePartitionNotFound PolicyName = "partition_not_found"
	PolicyNamePartitionRemoved  PolicyName = "partition_removed"
	PolicyNameCollectionRemoved PolicyName = "collection_removed"
	PolicyNameRecover           PolicyName = "recover"
	PolicyNameFenced            PolicyName = "fenced"
	PolicyNameForce             PolicyName = "force"
)

// GetSegmentAsyncSealPolicy returns the segment async seal policy.
func GetSegmentAsyncSealPolicy() []SegmentAsyncSealPolicy {
	// TODO: dynamic policy can be applied here in future.
	return []SegmentAsyncSealPolicy{
		&sealByCapacity{},
		&sealByBinlogNumber{},
		&sealByLifetime{},
		&sealByIdleTime{},
	}
}

// SealPolicyResult is the result of the seal policy.
type SealPolicyResult struct {
	PolicyName     PolicyName
	ShouldBeSealed bool
	ExtraInfo      interface{}
}

// SegmentAsyncSealPolicy is the policy to check if a segment should be sealed or not.
// Those policies are called asynchronously, so the stat is not real time.
// A policy should be stateless, and only check by segment stats.
// quick enough to be called.
type SegmentAsyncSealPolicy interface {
	// ShouldBeSealed checks if the segment should be sealed, and return the reason string.
	ShouldBeSealed(stats *stats.SegmentStats) SealPolicyResult
}

// sealByCapacity is a policy to seal the segment by the capacity.
type sealByCapacity struct{}

// ShouldBeSealed checks if the segment should be sealed, and return the reason string.
func (p *sealByCapacity) ShouldBeSealed(stats *stats.SegmentStats) SealPolicyResult {
	return SealPolicyResult{
		PolicyName:     "by_capacity",
		ShouldBeSealed: stats.ReachLimit,
		ExtraInfo:      nil,
	}
}

// sealByBinlogFileExtraInfo is the extra info of the seal by binlog file number policy.
type sealByBinlogFileExtraInfo struct {
	BinLogNumberLimit int
}

// sealByBinlogNumber is a policy to seal the segment by the binlog file number.
type sealByBinlogNumber struct{}

// ShouldBeSealed checks if the segment should be sealed, and return the reason string.
func (p *sealByBinlogNumber) ShouldBeSealed(stats *stats.SegmentStats) SealPolicyResult {
	limit := paramtable.Get().DataCoordCfg.SegmentMaxBinlogFileNumber.GetAsInt()
	shouldBeSealed := stats.BinLogCounter >= uint64(limit)
	return SealPolicyResult{
		PolicyName:     "binlog_number",
		ShouldBeSealed: shouldBeSealed,
		ExtraInfo: &sealByBinlogFileExtraInfo{
			BinLogNumberLimit: limit,
		},
	}
}

// sealByLifetimeExtraInfo is the extra info of the seal by lifetime policy.
type sealByLifetimeExtraInfo struct {
	MaxLifeTime time.Duration
}

// sealByLifetime is a policy to seal the segment by the lifetime.
type sealByLifetime struct{}

// ShouldBeSealed checks if the segment should be sealed, and return the reason string.
func (p *sealByLifetime) ShouldBeSealed(stats *stats.SegmentStats) SealPolicyResult {
	lifetime := paramtable.Get().DataCoordCfg.SegmentMaxLifetime.GetAsDuration(time.Second)
	shouldBeSealed := time.Since(stats.CreateTime) > lifetime
	return SealPolicyResult{
		PolicyName:     "by_lifetime",
		ShouldBeSealed: shouldBeSealed,
		ExtraInfo: sealByLifetimeExtraInfo{
			MaxLifeTime: lifetime,
		},
	}
}

// sealByIdleTimeExtraInfo is the extra info of the seal by idle time policy.
type sealByIdleTimeExtraInfo struct {
	IdleTime    time.Duration
	MinimalSize uint64
}

// sealByIdleTime is a policy to seal the segment by the idle time.
type sealByIdleTime struct{}

// ShouldBeSealed checks if the segment should be sealed, and return the reason string.
func (p *sealByIdleTime) ShouldBeSealed(stats *stats.SegmentStats) SealPolicyResult {
	idleTime := paramtable.Get().DataCoordCfg.SegmentMaxIdleTime.GetAsDuration(time.Second)
	minSize := uint64(paramtable.Get().DataCoordCfg.SegmentMinSizeFromIdleToSealed.GetAsInt() * 1024 * 1024)

	shouldBeSealed := stats.Insert.BinarySize > minSize && time.Since(stats.LastModifiedTime) > idleTime
	return SealPolicyResult{
		PolicyName:     "by_idle_time",
		ShouldBeSealed: shouldBeSealed,
		ExtraInfo: sealByIdleTimeExtraInfo{
			IdleTime:    idleTime,
			MinimalSize: minSize,
		},
	}
}
