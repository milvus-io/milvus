package shards

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// An L1 segment is sealed at the seal proportion of the segment size. A row
// capped collection is additionally sealed at dataCoord.segment.maxRows rows,
// without the seal proportion; the byte limit does not change.
func TestL1LimitationRowCap(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	require.NoError(t, params.Save(params.DataCoordCfg.SegmentMaxSize.Key, "1024"))
	require.NoError(t, params.Save(params.DataCoordCfg.SegmentMaxRows.Key, "4000000"))
	require.NoError(t, params.Save(params.DataCoordCfg.SegmentSealProportion.Key, "0.5"))
	require.NoError(t, params.Save(params.DataCoordCfg.SegmentSealProportionJitter.Key, "0"))
	t.Cleanup(func() {
		params.Reset(params.DataCoordCfg.SegmentMaxSize.Key)
		params.Reset(params.DataCoordCfg.SegmentMaxRows.Key)
		params.Reset(params.DataCoordCfg.SegmentSealProportion.Key)
		params.Reset(params.DataCoordCfg.SegmentSealProportionJitter.Key)
	})
	policy := getSegmentLimitationPolicy()

	// no collection property: dataCoord.segment.maxRows
	global := policy.GenerateLimitation(datapb.SegmentLevel_L1, -1)
	require.Equal(t, uint64(512*1024*1024), global.SegmentSize)
	require.Equal(t, uint64(4000000), global.SegmentRows)

	// the collection property overrides it, in both directions
	require.Equal(t, uint64(1000000), policy.GenerateLimitation(datapb.SegmentLevel_L1, 1000000).SegmentRows)
	require.Equal(t, uint64(9000000), policy.GenerateLimitation(datapb.SegmentLevel_L1, 9000000).SegmentRows)
	// and 0 turns the row limit off for the collection
	off := policy.GenerateLimitation(datapb.SegmentLevel_L1, 0)
	require.Equal(t, uint64(512*1024*1024), off.SegmentSize)
	require.Equal(t, uint64(math.MaxUint64), off.SegmentRows)

	// maxRows 0 disables the cap, a collection property still applies
	require.NoError(t, params.Save(params.DataCoordCfg.SegmentMaxRows.Key, "0"))
	require.Equal(t, uint64(math.MaxUint64), policy.GenerateLimitation(datapb.SegmentLevel_L1, -1).SegmentRows)
	require.Equal(t, uint64(1000000), policy.GenerateLimitation(datapb.SegmentLevel_L1, 1000000).SegmentRows)
}

func TestCollectionInfoMaxRowsOverride(t *testing.T) {
	info := &CollectionInfo{PartitionIDs: map[int64]struct{}{}}
	// unknown schema, nil info: not overridden
	require.Equal(t, int64(-1), info.MaxRowsOverride())
	require.Equal(t, int64(-1), (*CollectionInfo)(nil).MaxRowsOverride())

	pk := &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}
	vec := &schemapb.FieldSchema{
		FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "8"}},
	}
	schemaOf := func(value string, fields ...*schemapb.FieldSchema) *streamingpb.CollectionSchemaOfVChannel {
		schema := &schemapb.CollectionSchema{Fields: fields}
		if value != "" {
			schema.Properties = []*commonpb.KeyValuePair{{Key: common.CollectionSegmentMaxRowsKey, Value: value}}
		}
		return &streamingpb.CollectionSchemaOfVChannel{Schema: schema}
	}

	// The override follows the property, not the fields of the schema.
	info.setSchema(schemaOf("", pk))
	require.Equal(t, int64(-1), info.MaxRowsOverride())
	info.setSchema(schemaOf("", pk, vec))
	require.Equal(t, int64(-1), info.MaxRowsOverride())
	info.setSchema(schemaOf("1000000", pk, vec))
	require.Equal(t, int64(1000000), info.MaxRowsOverride())
	info.setSchema(schemaOf("0", pk))
	require.Equal(t, int64(0), info.MaxRowsOverride())
	// a value that does not parse is ignored
	info.setSchema(schemaOf("-5", pk))
	require.Equal(t, int64(-1), info.MaxRowsOverride())
	info.setSchema(schemaOf("many", pk))
	require.Equal(t, int64(-1), info.MaxRowsOverride())
	// the property is removed, the schema is dropped
	info.setSchema(schemaOf("1000000", pk))
	info.setSchema(schemaOf("", pk))
	require.Equal(t, int64(-1), info.MaxRowsOverride())
	info.setSchema(schemaOf("1000000", pk))
	info.setSchema(nil)
	require.Equal(t, int64(-1), info.MaxRowsOverride())
}
