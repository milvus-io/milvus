package segments

import (
	"cmp"
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
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
	properties        []*commonpb.KeyValuePair
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
		properties:        schema.GetProperties(),
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

// UpdateField identifies a mutable logical column tracked by a physical store.
type UpdateField struct {
	ID       int64
	Type     schemapb.DataType
	Element  schemapb.DataType
	Nullable bool
}

// UpdateCoverage freezes the writable fields in the native segment's load scope.
// Index choices and schema revisions can change without changing version coverage.
func (c *Collection) UpdateCoverage() []UpdateField {
	if c == nil {
		return nil
	}
	schema := c.Schema()
	restricted := typeutil.PKUpdateFieldRestriction(schema)
	var fields []UpdateField
	for _, f := range schema.GetFields() {
		if restricted(f) || !typeutil.SupportsPKUpdateType(f) || c.loadFields != nil && !c.loadFields.Contain(f.GetFieldID()) {
			continue
		}
		fields = append(fields, UpdateField{f.GetFieldID(), f.GetDataType(), f.GetElementType(), f.GetNullable()})
	}
	slices.SortFunc(fields, func(a, b UpdateField) int { return cmp.Compare(a.ID, b.ID) })
	return fields
}
