package meta

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// CheckRefreshSchema prevents background builders from pairing an old external
// segment with the new source/spec before the refresh results have been applied.
func CheckRefreshSchema(segment *SegmentInfo, schema *schemapb.CollectionSchema) error {
	if !typeutil.IsExternalCollection(schema) || segment.GetSchemaVersion() >= schema.GetVersion() {
		return nil
	}
	target, err := broadcast.GetPendingSchemaTarget(segment.GetCollectionID())
	if err != nil {
		return err
	}
	if target != nil && target.RefreshJobID != 0 {
		return merr.WrapErrServiceNotReadyMsg("refresh segment %d has not reached schema %d", segment.GetID(), schema.GetVersion())
	}
	return nil
}
