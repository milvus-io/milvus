package qvresource

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type qvLoadedSegment interface {
	ID() int64
	Partition() int64
	Delete(ctx context.Context, primaryKeys storage.PrimaryKeys, timestamps []typeutil.Timestamp) error
	Release(ctx context.Context) error
}

type qvPKCandidateSegment interface {
	PkCandidateExist() bool
	BatchPkExist(lc *storage.BatchLocationsCache) []bool
}

type qvSegmentLoader interface {
	NewSegment(ctx context.Context, collection qnview.CollectionRuntime, info *querypb.SegmentLoadInfo) (qvLoadedSegment, error)
	LoadSegment(ctx context.Context, segment qvLoadedSegment, info *querypb.SegmentLoadInfo) error
	ReopenSegment(ctx context.Context, segment qvLoadedSegment, collection qnview.CollectionRuntime, info *querypb.SegmentLoadInfo) error
	LoadDeltaLogs(ctx context.Context, segment qvLoadedSegment, info *querypb.SegmentLoadInfo) error
	LoadPKCandidate(ctx context.Context, segment qvLoadedSegment, info *querypb.SegmentLoadInfo) error
}
