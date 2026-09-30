package storage

import (
	"slices"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// CopyInsertRequestMetadata gives a consumer private execution metadata and
// field wrappers for validity normalization. Column values, RowIDs, RowData
// and bitmap backing arrays remain borrowed and must not be modified.
// Timestamps may be replaced, but their original backing array is read-only.
func CopyInsertRequestMetadata(src *msgpb.InsertRequest) *msgpb.InsertRequest {
	dst := &msgpb.InsertRequest{
		ShardName: src.ShardName, DbName: src.DbName, CollectionName: src.CollectionName,
		PartitionName: src.PartitionName, DbID: src.DbID, CollectionID: src.CollectionID,
		PartitionID: src.PartitionID, SegmentID: src.SegmentID,
		Timestamps: src.Timestamps, RowIDs: src.RowIDs, RowData: src.RowData,
		FieldsData: typeutil.CopyFieldDataMetadata(src.FieldsData), NumRows: src.NumRows, Version: src.Version,
	}
	if src.Base != nil {
		dst.Base = proto.Clone(src.Base).(*commonpb.MsgBase)
	}
	if src.Namespace != nil {
		namespace := *src.Namespace
		dst.Namespace = &namespace
	}
	dst.ProtoReflect().SetUnknown(slices.Clone(src.ProtoReflect().GetUnknown()))
	return dst
}
