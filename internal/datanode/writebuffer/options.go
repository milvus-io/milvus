package writebuffer

import (
	"time"

	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

const (
	// DeletePolicyBFPKOracle is the const config value for using bf pk oracle as delete policy
	DeletePolicyBFPkOracle = `bloom_filter_pkoracle`

	// DeletePolicyL0Delta is the const config value for using L0 delta as deleta policy.
	DeletePolicyL0Delta = `l0_delta`
)

type WriteBufferOption func(opt *writeBufferOption)

type writeBufferOption struct {
	deletePolicy string
	idAllocator  allocator.Interface
	syncPolicies []SyncPolicy

	pkStatsFactory metacache.PkStatsFactory
	metaWriter     syncmgr.MetaWriter
}

func defaultWBOption(metacache metacache.MetaCache) *writeBufferOption {
	deletePolicy := DeletePolicyBFPkOracle
	if paramtable.Get().DataCoordCfg.EnableLevelZeroSegment.GetAsBool() {
		deletePolicy = DeletePolicyL0Delta
	}

	return &writeBufferOption{
		// TODO use l0 delta as default after implementation.
		deletePolicy: deletePolicy,
		syncPolicies: []SyncPolicy{
			GetFullBufferPolicy(),
			GetSyncStaleBufferPolicy(paramtable.Get().DataNodeCfg.SyncPeriod.GetAsDuration(time.Second)),
			GetCompactedSegmentsPolicy(metacache),
			GetSealedSegmentsPolicy(metacache),
			GetDroppedSegmentPolicy(metacache),
		},
	}
}

func WithDeletePolicy(policy string) WriteBufferOption {
	return func(opt *writeBufferOption) {
		opt.deletePolicy = policy
	}
}

func WithIDAllocator(allocator allocator.Interface) WriteBufferOption {
	return func(opt *writeBufferOption) {
		opt.idAllocator = allocator
	}
}

func WithPKStatsFactory(factory metacache.PkStatsFactory) WriteBufferOption {
	return func(opt *writeBufferOption) {
		opt.pkStatsFactory = factory
	}
}

func WithMetaWriter(writer syncmgr.MetaWriter) WriteBufferOption {
	return func(opt *writeBufferOption) {
		opt.metaWriter = writer
	}
}

func WithSyncPolicy(policy SyncPolicy) WriteBufferOption {
	return func(opt *writeBufferOption) {
		opt.syncPolicies = append(opt.syncPolicies, policy)
	}
}
