package segments

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Physical metadata outlives the Collection used for initial preparation.
// It is installed before publishing a QueryView segment and never mutated.
type sealedRuntimeMetadata struct {
	database      string
	resourceGroup string
	delta         deltaStorageMetadata
}

type deltaStorageMetadata struct {
	primaryKeyType    schemapb.DataType
	external          bool
	milvusTable       bool
	milvusTableRealPK bool
	source            string
	spec              string
}

func newDeltaStorageMetadata(schema *schemapb.CollectionSchema) deltaStorageMetadata {
	resolver := typeutil.NewStorageColumnResolver(schema)
	return deltaStorageMetadata{
		primaryKeyType:    GetPkField(schema).GetDataType(),
		external:          typeutil.IsExternalCollection(schema),
		milvusTable:       resolver.IsMilvusTable(),
		milvusTableRealPK: resolver.IsMilvusTable() && HasExternalPrimaryKey(schema),
		source:            schema.GetExternalSource(),
		spec:              schema.GetExternalSpec(),
	}
}

// DetachPreparingCollection ends the initial load operation. Subsequent
// QueryView operations supply their own pinned Collection; physical resources
// retain only their storage metadata. Call before publishing the segment.
func (s *LocalSegment) DetachPreparingCollection() error {
	if s.resourceUsageCache.Load() == nil {
		return merr.WrapErrServiceInternalMsg("segment %d has no finalized resource estimate", s.ID())
	}
	s.sealedMetadata = &sealedRuntimeMetadata{
		database:      s.collection.GetDBName(),
		resourceGroup: s.collection.GetResourceGroup(),
		delta:         newDeltaStorageMetadata(s.collection.Schema()),
	}
	s.collection = nil
	return nil
}
