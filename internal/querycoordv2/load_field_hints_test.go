package querycoordv2

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestLoadFieldHintsAfterProtocolSimplification(t *testing.T) {
	paramtable.Init()
	coll := &metacache.CollectionInfo{ID: 1, Schema: &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, DataType: schemapb.DataType_FloatVector},
			{FieldID: 103, TypeParams: []*commonpb.KeyValuePair{{Key: common.FieldSkipLoadKey, Value: "true"}}},
			{FieldID: 102, DataType: schemapb.DataType_JSON, IsDynamic: true},
		},
	}}
	for _, tc := range []struct {
		name       string
		fields     []int64
		wantFields []int64
	}{
		{"defaults materialized with skip_load", nil, []int64{100, 101, 102}},
		{"skip dynamic retains explicit hints", []int64{100, 101}, []int64{100, 101}},
		{"explicit full hints", []int64{100, 101, 102}, []int64{100, 101, 102}},
		{"explicit hints", []int64{101, 100}, []int64{100, 101}},
		{"placement update preserves hints", []int64{100, 101}, []int64{100, 101}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			collectionMsg, err := (&Server{}).generateAlterLoadConfigMessageForLoadCollection(t.Context(), coll, nil, qviewsExpectedLoadConfig{
				LoadFields: tc.fields,
			})
			require.NoError(t, err)
			partitionMsg, err := job.GenerateAlterLoadConfigMessage(t.Context(), &job.AlterLoadConfigRequest{
				CollectionInfo: coll,
				Expected:       job.ExpectedLoadConfig{ExpectedLoadFields: loadmgr.PreloadFieldIDsForSchema(coll.Schema, tc.fields), ExpectedRequiredFields: loadmgr.FieldsToMessage(loadmgr.RequiredFieldsForSchema(coll.Schema))},
			})
			require.NoError(t, err)
			for _, msg := range []message.BroadcastMutableMessage{collectionMsg, partitionMsg} {
				header := message.MustAsBroadcastAlterLoadConfigMessageV2(msg).Header()
				var hints, required []int64
				for _, field := range header.GetLoadFields() {
					hints = append(hints, field.GetFieldId())
				}
				for _, field := range header.GetRequiredFields() {
					required = append(required, field.GetFieldId())
				}
				require.Equal(t, tc.wantFields, hints)
				require.Equal(t, []int64{100, 101, 103, 102}, required)
			}
		})
	}
}

func TestPlacementMessagePreservesEmptyLoadScopes(t *testing.T) {
	msg, err := job.GenerateAlterLoadConfigMessage(t.Context(), &job.AlterLoadConfigRequest{
		CollectionInfo: &metacache.CollectionInfo{ID: 1, Schema: &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{{FieldID: 100}, {FieldID: 101}},
		}},
		// Placement updates carry existing explicit sets, even when both are empty.
		Expected: job.ExpectedLoadConfig{},
	})
	require.NoError(t, err)
	header := message.MustAsBroadcastAlterLoadConfigMessageV2(msg).Header()
	require.Empty(t, header.GetLoadFields())
	require.Empty(t, header.GetRequiredFields())
}
