package querycoordv2

import (
	"cmp"
	"context"
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// queryViewLoadInfoReader supplies QN requirements without owning coordinator
// publication or GC. The producer retains a version while a View references it.
// Each lookup reads storage so deletion is not hidden by an unbounded local cache.
type queryViewLoadInfoReader struct {
	storage kv.MetaKv
	indexes func(int64, ...int64) []*indexpb.IndexInfo
}

func (s *queryViewLoadInfoReader) GetLoadInfo(ctx context.Context, collectionID int64, version uint64) (*querypb.GetQueryViewLoadInfoResponse, error) {
	cfg, err := queryview.GetCollectionLoadInfo(ctx, s.storage, collectionID, version)
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, merr.WrapErrServiceNotReadyMsg("load requirements %d for collection %d are unavailable", version, collectionID)
	}
	if len(cfg.RequiredFields) == 0 {
		return nil, merr.WrapErrDataIntegrityMsg("load requirements %d for collection %d have no required fields", version, collectionID)
	}
	// Definitions stay owned by index metadata. Resolve only this version's
	// selected IDs; neither new indexes nor current field selections may leak in.
	selected := make(map[int64]int64)
	var ids []int64
	for _, field := range cfg.RequiredFields {
		for _, id := range field.GetIndexIds() {
			selected[id] = field.GetFieldId()
			ids = append(ids, id)
		}
	}
	var indexes []*indexpb.IndexInfo
	if len(ids) != 0 && s.indexes != nil {
		for _, info := range s.indexes(collectionID, ids...) {
			field, ok := selected[info.GetIndexID()]
			if ok && field == info.GetFieldID() {
				indexes = append(indexes, proto.Clone(info).(*indexpb.IndexInfo))
				delete(selected, info.GetIndexID())
			}
		}
	}
	if len(selected) != 0 {
		return nil, merr.WrapErrServiceNotReadyMsg("selected index metadata for load requirements %d of collection %d is unavailable", version, collectionID)
	}
	slices.SortFunc(indexes, func(a, b *indexpb.IndexInfo) int { return cmp.Compare(a.GetIndexID(), b.GetIndexID()) })
	return &querypb.GetQueryViewLoadInfoResponse{
		Status: merr.Success(), CollectionID: collectionID, Version: version,
		PartitionIDs: cfg.PartitionIds, RequiredFields: cfg.RequiredFields,
		LoadFields: cfg.PreloadFields, IndexInfoList: indexes,
	}, nil
}
