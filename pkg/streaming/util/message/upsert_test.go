// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package message_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestUpsertEnvelope(t *testing.T) {
	for _, marker := range []string{"", "_ur", "_np", "_pcl", "_bh"} {
		builder := message.NewUpsertMessageBuilderV2().WithVChannel("p_1v0").
			WithHeader(&message.UpsertMessageHeader{}).WithBody(&message.UpsertMessageBody{})
		if marker != "" {
			builder.WithProperty(marker, "")
		}
		msg := builder.MustBuildMutable()
		err := message.ValidateUpsertEnvelope(msg)
		if marker == "" {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
}

func TestUpsertContract(t *testing.T) {
	header := &message.UpsertMessageHeader{CollectionId: 10, SchemaVersion: new(int32), Partition: &message.PartitionSegmentAssignment{PartitionId: 20, Rows: 2}, DeleteRows: 2}
	body := &message.UpsertMessageBody{
		Insert: &msgpb.InsertRequest{Version: msgpb.InsertDataVersion_ColumnBased, CollectionID: 10, PartitionID: 20, NumRows: 2, RowIDs: []int64{1, 2}, FieldsData: []*schemapb.FieldData{{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{4, 5}}}}}}}},
		Delete: &msgpb.DeleteRequest{CollectionID: 10, PartitionID: -1, NumRows: 2, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{5, 4}}}}},
	}
	pk := &schemapb.FieldSchema{FieldID: 100, IsPrimaryKey: true, DataType: schemapb.DataType_Int64}
	require.NoError(t, message.ValidateUpsert(header, body, pk))
	body.Delete.PrimaryKeys.GetIntId().Data = []int64{4, 6}
	require.Error(t, message.ValidateUpsert(header, body, pk))
	body.Delete.PrimaryKeys.GetIntId().Data = []int64{4, 4}
	require.Error(t, message.ValidateUpsert(header, body, pk))
	require.True(t, message.MessageTypeUpsert.IsDMLMessageType())
	require.True(t, message.MessageTypeUpsert.CanEnableCipher())
	require.False(t, message.MessageTypeUpsert.IsExclusiveRequired())
}

func TestUpsertRequiresSchemaVersionAndAcceptsDuplicatePrimaryKeys(t *testing.T) {
	header := &message.UpsertMessageHeader{CollectionId: 10, Partition: &message.PartitionSegmentAssignment{PartitionId: 20, Rows: 2}, DeleteRows: 1}
	body := &message.UpsertMessageBody{
		Insert: &msgpb.InsertRequest{Version: msgpb.InsertDataVersion_ColumnBased, CollectionID: 10, PartitionID: 20, NumRows: 2, RowIDs: []int64{1, 2}, FieldsData: []*schemapb.FieldData{{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{4, 4}}}}}}}},
		Delete: &msgpb.DeleteRequest{CollectionID: 10, PartitionID: -1, NumRows: 1, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{4}}}}},
	}
	pk := &schemapb.FieldSchema{FieldID: 100, IsPrimaryKey: true, DataType: schemapb.DataType_Int64}
	require.Error(t, message.ValidateUpsert(header, body, pk))
	zero := int32(0)
	header.SchemaVersion = &zero
	require.NoError(t, message.ValidateUpsert(header, body, pk))
}

func TestUpsertMalformedContracts(t *testing.T) {
	tests := []struct {
		name   string
		change func(*message.UpsertMessageHeader, *message.UpsertMessageBody, *schemapb.FieldSchema)
	}{
		{"missing schema version", func(h *message.UpsertMessageHeader, _ *message.UpsertMessageBody, _ *schemapb.FieldSchema) {
			h.SchemaVersion = nil
		}},
		{"missing assignment", func(h *message.UpsertMessageHeader, _ *message.UpsertMessageBody, _ *schemapb.FieldSchema) {
			h.Partition = nil
		}},
		{"missing insert", func(_ *message.UpsertMessageHeader, b *message.UpsertMessageBody, _ *schemapb.FieldSchema) {
			b.Insert = nil
		}},
		{"missing delete", func(_ *message.UpsertMessageHeader, b *message.UpsertMessageBody, _ *schemapb.FieldSchema) {
			b.Delete = nil
		}},
		{"foreign insert collection", func(_ *message.UpsertMessageHeader, b *message.UpsertMessageBody, _ *schemapb.FieldSchema) {
			b.Insert.CollectionID = 11
		}},
		{"foreign delete collection", func(_ *message.UpsertMessageHeader, b *message.UpsertMessageBody, _ *schemapb.FieldSchema) {
			b.Delete.CollectionID = 11
		}},
		{"foreign insert partition", func(_ *message.UpsertMessageHeader, b *message.UpsertMessageBody, _ *schemapb.FieldSchema) {
			b.Insert.PartitionID = 21
		}},
		{"partition scoped delete", func(_ *message.UpsertMessageHeader, b *message.UpsertMessageBody, _ *schemapb.FieldSchema) {
			b.Delete.PartitionID = 20
		}},
		{"missing RowIDs", func(_ *message.UpsertMessageHeader, b *message.UpsertMessageBody, _ *schemapb.FieldSchema) {
			b.Insert.RowIDs = nil
		}},
		{"assignment rows", func(h *message.UpsertMessageHeader, _ *message.UpsertMessageBody, _ *schemapb.FieldSchema) {
			h.Partition.Rows = 3
		}},
		{"delete header rows", func(h *message.UpsertMessageHeader, _ *message.UpsertMessageBody, _ *schemapb.FieldSchema) {
			h.DeleteRows = 3
		}},
		{"delete body rows", func(_ *message.UpsertMessageHeader, b *message.UpsertMessageBody, _ *schemapb.FieldSchema) {
			b.Delete.NumRows = -1
		}},
		{"namespace payload", func(_ *message.UpsertMessageHeader, b *message.UpsertMessageBody, _ *schemapb.FieldSchema) {
			b.Insert.Namespace = new(string)
		}},
		{"predicate delete", func(_ *message.UpsertMessageHeader, b *message.UpsertMessageBody, _ *schemapb.FieldSchema) {
			b.Delete.SerializedExprPlan = []byte{1}
		}},
		{"legacy delete keys", func(_ *message.UpsertMessageHeader, b *message.UpsertMessageBody, _ *schemapb.FieldSchema) {
			b.Delete.Int64PrimaryKeys = []int64{4, 5}
		}},
		{"missing primary field", func(_ *message.UpsertMessageHeader, b *message.UpsertMessageBody, _ *schemapb.FieldSchema) {
			b.Insert.FieldsData = nil
		}},
		{"duplicate primary field", func(_ *message.UpsertMessageHeader, b *message.UpsertMessageBody, _ *schemapb.FieldSchema) {
			b.Insert.FieldsData = append(b.Insert.FieldsData, b.Insert.FieldsData[0])
		}},
		{"primary field count", func(_ *message.UpsertMessageHeader, b *message.UpsertMessageBody, _ *schemapb.FieldSchema) {
			b.Insert.FieldsData[0].GetScalars().GetLongData().Data = []int64{4}
		}},
		{"primary type", func(_ *message.UpsertMessageHeader, _ *message.UpsertMessageBody, p *schemapb.FieldSchema) {
			p.DataType = schemapb.DataType_Float
		}},
		{"delete type", func(_ *message.UpsertMessageHeader, b *message.UpsertMessageBody, _ *schemapb.FieldSchema) {
			b.Delete.PrimaryKeys = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"4", "5"}}}}
		}},
		{"append result count", func(h *message.UpsertMessageHeader, b *message.UpsertMessageBody, _ *schemapb.FieldSchema) {
			h.IdempotentResult = &messagespb.IdempotentInsertResult{}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h, b, p := validUpsertContract()
			test.change(h, b, p)
			require.Error(t, message.ValidateUpsert(h, b, p))
		})
	}
	require.Error(t, message.ValidateUpsert(nil, nil, nil))
}

func validUpsertContract() (*message.UpsertMessageHeader, *message.UpsertMessageBody, *schemapb.FieldSchema) {
	h := &message.UpsertMessageHeader{CollectionId: 10, SchemaVersion: new(int32), Partition: &message.PartitionSegmentAssignment{PartitionId: 20, Rows: 2}, DeleteRows: 2}
	b := &message.UpsertMessageBody{
		Insert: &msgpb.InsertRequest{Version: msgpb.InsertDataVersion_ColumnBased, CollectionID: 10, PartitionID: 20, NumRows: 2, RowIDs: []int64{1, 2}, FieldsData: []*schemapb.FieldData{{FieldId: 100, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{4, 5}}}}}}}},
		Delete: &msgpb.DeleteRequest{CollectionID: 10, PartitionID: -1, NumRows: 2, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{5, 4}}}}},
	}
	return h, b, &schemapb.FieldSchema{FieldID: 100, IsPrimaryKey: true, DataType: schemapb.DataType_Int64}
}

func TestUpsertVarCharAndAppendResult(t *testing.T) {
	h, b, p := validUpsertContract()
	p.DataType = schemapb.DataType_VarChar
	b.Insert.FieldsData[0] = &schemapb.FieldData{FieldId: 100, Type: schemapb.DataType_VarChar, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{"alice", "bob"}}}}}}
	b.Delete.PrimaryKeys = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"bob", "alice"}}}}
	h.IdempotentResult = &messagespb.IdempotentInsertResult{RowOffsets: []uint32{10, 20}, Ids: &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"alice", "bob"}}}}}
	require.NoError(t, message.ValidateUpsert(h, b, p))
	h.IdempotentResult.Ids.GetStrId().Data = []string{"bob", "alice"}
	require.Error(t, message.ValidateUpsert(h, b, p))
	h.IdempotentResult = nil
	b.Delete.PrimaryKeys.GetStrId().Data = []string{"alice", "alice"}
	require.Error(t, message.ValidateUpsert(h, b, p))
}

func TestUpsertProtocolRoundTrip(t *testing.T) {
	h, b, p := validUpsertContract()
	h.IdempotentResult = &messagespb.IdempotentInsertResult{RowOffsets: []uint32{2, 4}, Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{4, 5}}}}}
	mutable := message.NewUpsertMessageBuilderV2().WithVChannel("v1").WithHeader(h).WithBody(b).MustBuildMutable()
	require.Equal(t, message.VersionV2, mutable.Version())
	require.Equal(t, message.MessageType(50), mutable.MessageType())
	typed := message.MustAsMutableUpsertMessageV2(mutable)
	require.NotNil(t, typed.Header().SchemaVersion)
	require.Zero(t, typed.Header().GetSchemaVersion())
	require.NoError(t, message.ValidateUpsert(typed.Header(), typed.MustBody(), p))
	// Function materialization replaces only the nested insert in a mutable body.
	replacement := typed.MustBody()
	replacement.Insert.FieldsData = append(replacement.Insert.FieldsData, &schemapb.FieldData{FieldId: 101})
	typed.OverwriteBody(replacement)
	require.Len(t, typed.MustBody().GetInsert().GetFieldsData(), 2)
	require.Equal(t, []int64{5, 4}, typed.MustBody().GetDelete().GetPrimaryKeys().GetIntId().GetData())
	require.Equal(t, []uint32{2, 4}, typed.Header().GetIdempotentResult().GetRowOffsets())
	require.False(t, message.MessageTypeUpsert.IsSystem())
	require.False(t, message.MessageTypeUpsert.IsSelfControlled())
}

func TestUpsertRejectsRowBasedInsertWithPopulatedColumns(t *testing.T) {
	header, body, primary := validUpsertContract()
	body.Insert.Version = msgpb.InsertDataVersion_RowBased
	require.NotEmpty(t, body.Insert.GetFieldsData())
	require.Empty(t, body.Insert.GetRowData())
	require.Error(t, message.ValidateUpsert(header, body, primary))
}

func TestUpsertExactPrimaryKeySetsWithDuplicates(t *testing.T) {
	for _, varchar := range []bool{false, true} {
		for _, tc := range []struct {
			name              string
			inserted, deleted []int64
			valid             bool
		}{
			{"repeated insert", []int64{4, 4}, []int64{4}, true},
			{"repeated delete", []int64{4, 5}, []int64{4, 4, 5}, true},
			{"both repeated", []int64{4, 4, 5}, []int64{5, 4, 5}, true},
			{"missing", []int64{4, 5}, []int64{4, 4}, false},
			{"extra", []int64{4, 4}, []int64{4, 5}, false},
		} {
			t.Run(fmt.Sprintf("%s_varchar_%t", tc.name, varchar), func(t *testing.T) {
				h, b, p := validUpsertContract()
				h.Partition.Rows = uint64(len(tc.inserted))
				h.DeleteRows = uint64(len(tc.deleted))
				b.Insert.NumRows = uint64(len(tc.inserted))
				b.Insert.RowIDs = make([]int64, len(tc.inserted))
				b.Delete.NumRows = int64(len(tc.deleted))
				b.Insert.FieldsData[0].GetScalars().GetLongData().Data = tc.inserted
				b.Delete.PrimaryKeys.GetIntId().Data = tc.deleted
				if varchar {
					values := func(keys []int64) []string {
						out := make([]string, len(keys))
						for i, key := range keys {
							out[i] = fmt.Sprint(key)
						}
						return out
					}
					p.DataType = schemapb.DataType_VarChar
					b.Insert.FieldsData[0].Type = p.DataType
					b.Insert.FieldsData[0].GetScalars().Data = &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values(tc.inserted)}}
					b.Delete.PrimaryKeys.IdField = &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: values(tc.deleted)}}
				}
				err := message.ValidateUpsert(h, b, p)
				if tc.valid {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
			})
		}
	}
}

func TestUpsertResultHasOneOptionalHeaderSource(t *testing.T) {
	var header message.UpsertMessageHeader
	headerField := header.ProtoReflect().Descriptor().Fields().ByName("idempotent_result")
	require.NotNil(t, headerField)
	require.EqualValues(t, 5, headerField.Number())
	require.True(t, headerField.HasOptionalKeyword())
	require.False(t, header.ProtoReflect().Has(headerField))
	result := &messagespb.IdempotentInsertResult{RowOffsets: []uint32{0}, Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7}}}}}
	header.IdempotentResult = result
	require.True(t, header.ProtoReflect().Has(headerField))
	payload, err := proto.Marshal(&header)
	require.NoError(t, err)
	var restored message.UpsertMessageHeader
	require.NoError(t, proto.Unmarshal(payload, &restored))
	require.True(t, proto.Equal(result, restored.GetIdempotentResult()))

	var body message.UpsertMessageBody
	descriptor := body.ProtoReflect().Descriptor()
	require.Equal(t, 2, descriptor.Fields().Len())
	require.Nil(t, descriptor.Fields().ByName("append_result"))
	require.True(t, descriptor.ReservedRanges().Has(3))
	require.True(t, descriptor.ReservedNames().Has("append_result"))
	oldResult, err := proto.Marshal(result)
	require.NoError(t, err)
	oldBody := protowire.AppendTag(nil, 3, protowire.BytesType)
	oldBody = protowire.AppendBytes(oldBody, oldResult)
	require.NoError(t, proto.Unmarshal(oldBody, &body))
	require.NotEmpty(t, body.ProtoReflect().GetUnknown(), "the removed draft field stays unknown; it is not a second result source")
	require.Nil(t, (&message.UpsertMessageHeader{}).GetIdempotentResult())
}
