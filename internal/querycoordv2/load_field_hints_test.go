package querycoordv2

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestLoadFieldHintsAfterProtocolSimplification(t *testing.T) {
	paramtable.Init()
	coll := &milvuspb.DescribeCollectionResponse{CollectionID: 1, Schema: &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, DataType: schemapb.DataType_FloatVector},
			{FieldID: 102, DataType: schemapb.DataType_JSON, IsDynamic: true},
		},
	}}
	for _, tc := range []struct {
		name       string
		all        *bool
		skip       bool
		fields     []int64
		wantFields []int64
	}{
		{"all follows schema", proto.Bool(true), false, []int64{100, 101, 102}, nil},
		{"skip dynamic retains explicit hints", proto.Bool(true), true, []int64{100, 101}, []int64{100, 101}},
		{"explicit hints", proto.Bool(false), false, []int64{101, 100}, []int64{100, 101}},
		{"placement update preserves hints", nil, false, []int64{100, 101}, []int64{100, 101}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			collectionMsg, err := (&Server{}).generateAlterLoadConfigMessageForLoadCollection(t.Context(), coll, nil, qviewsExpectedLoadConfig{
				LoadFields: tc.fields, LoadAllFields: tc.all, SkipLoadDynamicField: tc.skip,
			})
			require.NoError(t, err)
			partitionMsg, err := job.GenerateAlterLoadConfigMessage(t.Context(), &job.AlterLoadConfigRequest{
				CollectionInfo: coll,
				Expected:       job.ExpectedLoadConfig{ExpectedLoadFields: tc.fields, ExpectedLoadAllFields: tc.all, ExpectedSkipLoadDynamicField: tc.skip},
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
				require.Equal(t, []int64{100, 101, 102}, required)
			}
		})
	}
}
