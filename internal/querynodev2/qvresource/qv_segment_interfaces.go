package qvresource

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/qnview"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type qvCollectionManager interface {
	Get(collectionID int64) *segments.Collection
	PutOrRef(collectionID int64, schema *schemapb.CollectionSchema, meta *segcorepb.CollectionIndexMeta, loadMeta *querypb.LoadMetaInfo) error
	Ref(collectionID int64, count uint32) bool
	Unref(collectionID int64, count uint32) bool
}

type qvSegmentManager interface {
	Remove(ctx context.Context, segmentID int64, scope querypb.DataScope) (int, int)
}

type qvLoadedSegment interface {
	ID() int64
	Partition() int64
	Delete(ctx context.Context, primaryKeys storage.PrimaryKeys, timestamps []typeutil.Timestamp) error
	Release(ctx context.Context) error
}

type qvReadableSegment interface {
	QuerySegment() segments.Segment
	Collection() *segments.Collection
}

type qvPKCandidateSegment interface {
	PkCandidateExist() bool
	BatchPkExist(lc *storage.BatchLocationsCache) []bool
}

type qvSegmentLoader interface {
	NewSegment(ctx context.Context, collection qnview.CollectionRuntime, info *querypb.SegmentLoadInfo) (qvLoadedSegment, error)
	LoadSegment(ctx context.Context, segment qvLoadedSegment, info *querypb.SegmentLoadInfo) error
	LoadDeltaLogs(ctx context.Context, segment qvLoadedSegment, info *querypb.SegmentLoadInfo) error
	LoadPKCandidate(ctx context.Context, segment qvLoadedSegment, info *querypb.SegmentLoadInfo) error
}
