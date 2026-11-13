package model

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
)

func TestStreamingCollectionSchemaMatchesCoordinatorUpdates(t *testing.T) {
	for _, paths := range [][]string{
		{message.FieldMaskDB},
		{message.FieldMaskCollectionName},
		{message.FieldMaskCollectionDescription},
		{message.FieldMaskCollectionProperties},
		{message.FieldMaskCollectionExternalSpec},
		{message.FieldMaskCollectionSchema},
		{message.FieldMaskCollectionDescription, message.FieldMaskCollectionProperties, message.FieldMaskCollectionSchema},
		{message.FieldMaskCollectionSchema, message.FieldMaskCollectionProperties, message.FieldMaskCollectionDescription},
		{message.FieldMaskCollectionConsistencyLevel},
		{"future_field"},
	} {
		t.Run(paths[0], func(t *testing.T) {
			coll := &Collection{
				Name: "old", DBName: "old_db", Description: "old description", SchemaVersion: 1,
				Properties:     []*commonpb.KeyValuePair{{Key: "timezone", Value: "UTC"}},
				Fields:         []*Field{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
				ExternalSource: "old_source", ExternalSpec: "old_spec",
			}
			before := coll.ToCollectionSchemaPB()
			original := proto.Clone(before)
			header := &message.AlterCollectionMessageHeader{UpdateMask: &fieldmaskpb.FieldMask{Paths: paths}}
			body := &message.AlterCollectionMessageBody{Updates: &message.AlterCollectionMessageUpdates{
				DbName: "new_db", CollectionName: "new", Description: "new description",
				// Nil replaces the entire properties list, including deletions.
				Schema: &schemapb.CollectionSchema{
					Version: 2, Name: "stale", Description: "stale",
					Fields:       []*schemapb.FieldSchema{{FieldID: 101, Name: "new_field", DataType: schemapb.DataType_Int64}},
					ExternalSpec: "new_spec",
				},
			}}
			bodyBefore := proto.Clone(body)
			got := messageutil.ApplyCollectionSchemaUpdates(before, header, body)
			coll.ApplyUpdates(header, body)
			if got == nil {
				require.True(t, proto.Equal(before, coll.ToCollectionSchemaPB()))
			} else {
				require.True(t, proto.Equal(coll.ToCollectionSchemaPB(), got), "Coord: %v; SN: %v", coll.ToCollectionSchemaPB(), got)
				if len(got.Fields) > 0 {
					got.Fields[0].Name = "caller mutation"
				}
			}
			require.True(t, proto.Equal(original, before))
			require.True(t, proto.Equal(bodyBefore, body))
		})
	}
}

func TestStreamingCollectionSchemaPropertyReplacement(t *testing.T) {
	before := &schemapb.CollectionSchema{Version: 2, Properties: []*commonpb.KeyValuePair{{Key: "deleted", Value: "old"}}}
	body := &message.AlterCollectionMessageBody{Updates: &message.AlterCollectionMessageUpdates{
		Properties: []*commonpb.KeyValuePair{{Key: common.TimezoneKey, Value: "Asia/Shanghai"}, {Key: common.CollectionOnTruncatingKey, Value: "1"}},
	}}
	header := &message.AlterCollectionMessageHeader{UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionProperties}}}
	got := messageutil.ApplyCollectionSchemaUpdates(before, header, body)
	require.EqualValues(t, 2, got.Version)
	require.Len(t, got.Properties, 1)
	require.Equal(t, common.TimezoneKey, got.Properties[0].Key)
	require.Equal(t, "Asia/Shanghai", got.Properties[0].Value)
	got.Properties[0].Value = "caller mutation"
	require.Equal(t, "Asia/Shanghai", body.Updates.Properties[0].Value)
	require.Equal(t, "deleted", before.Properties[0].Key)
}

func TestStreamingCollectionSchemaLegacyExternalSourceUpdate(t *testing.T) {
	before := &schemapb.CollectionSchema{ExternalSource: "old source", ExternalSpec: "retained spec"}
	header := &message.AlterCollectionMessageHeader{UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionExternalSpec}}}
	body := &message.AlterCollectionMessageBody{Updates: &message.AlterCollectionMessageUpdates{
		Schema: &schemapb.CollectionSchema{ExternalSource: "new source"},
	}}
	got := messageutil.ApplyCollectionSchemaUpdates(before, header, body)
	require.Equal(t, "new source", got.ExternalSource)
	require.Equal(t, "retained spec", got.ExternalSpec)
	require.Equal(t, "old source", before.ExternalSource)
}
