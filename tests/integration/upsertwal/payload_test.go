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

package upsertwal

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
	"github.com/milvus-io/milvus/tests/integration"
)

func (s *UpsertWALSuite) TestProducerCanonicalColumnsSurvivePersistence() {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	name := "native_upsert_canonical_" + funcutil.GenRandomStr()
	schema := &schemapb.CollectionSchema{Name: name, Fields: []*schemapb.FieldSchema{
		{Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{Name: "marker", DataType: schemapb.DataType_Int64},
		{Name: "vector", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}},
		{Name: "value", DataType: schemapb.DataType_Int64, Nullable: true},
		{Name: "ts", DataType: schemapb.DataType_Timestamptz},
		{Name: "geo", DataType: schemapb.DataType_Geometry},
	}}
	payload, err := proto.Marshal(schema)
	s.Require().NoError(err)
	result, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{CollectionName: name, Schema: payload, ShardsNum: 1})
	s.Require().NoError(merr.CheckRPCCall(result, err))
	desc, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(desc, err))
	partitions, err := s.Cluster.MilvusClient.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(partitions, err))
	result, err = s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{CollectionName: name, FieldName: "vector", ExtraParams: integration.ConstructIndexParam(2, "FLAT", "L2")})
	s.Require().NoError(merr.CheckRPCCall(result, err))
	s.WaitForIndexBuilt(ctx, name, "vector")
	result, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(result, err))
	s.WaitForLoad(ctx, name)
	c := collection{name: name, id: desc.GetCollectionID(), vchannel: desc.GetVirtualChannelNames()[0], partition: partitions.GetPartitionIDs()[0], schema: desc.GetSchema()}
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	wkb, err := common.ConvertWKTToWKB("POINT (1 1)")
	s.Require().NoError(err)
	stamp := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	build := func(marker int64, valid []bool) message.MutableMessage {
		raw := c.upsert(11, marker)
		typed := message.MustAsMutableUpsertMessageV2(raw)
		body := typed.MustBody()
		second := message.MustAsMutableUpsertMessageV2(c.upsert(12, marker)).MustBody()
		body.Insert.NumRows = 2
		body.Insert.RowIDs = append(body.Insert.RowIDs, second.Insert.RowIDs...)
		body.Delete.NumRows = 2
		body.Delete.PrimaryKeys.GetIntId().Data = []int64{11, 12}
		for _, field := range body.Insert.FieldsData {
			switch field.GetFieldName() {
			case "pk":
				field.GetScalars().GetLongData().Data = []int64{11, 12}
			case "marker":
				field.GetScalars().GetLongData().Data = []int64{marker, marker}
			case "vector":
				field.GetVectors().GetFloatVector().Data = []float32{1, 2, 1, 2}
			}
		}
		for _, field := range c.schema.GetFields() {
			column := &schemapb.FieldData{FieldId: field.GetFieldID(), FieldName: field.GetName(), Type: field.GetDataType()}
			switch field.GetName() {
			case "value":
				column.ValidData = valid
				column.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{42, 42}}}}}
			case "ts":
				column.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_TimestamptzData{TimestamptzData: &schemapb.TimestamptzArray{Data: []int64{stamp, stamp}}}}}
			case "geo":
				column.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_GeometryData{GeometryData: &schemapb.GeometryArray{Data: [][]byte{wkb, wkb}}}}}
			default:
				continue
			}
			body.Insert.FieldsData = append(body.Insert.FieldsData, column)
		}
		typed.OverwriteBody(body)
		header := typed.Header()
		header.Partition.Rows = 2
		header.DeleteRows = 2
		typed.OverwriteHeader(header)
		return raw
	}
	check := func(marker int64, valid []bool) {
		for i, pk := range []int64{11, 12} {
			expr := fmt.Sprintf("pk == %d && marker == %d", pk, marker)
			if valid[i] {
				expr += " && value == 42"
			} else {
				expr += " && value is null"
			}
			rows, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{CollectionName: name, Expr: expr, OutputFields: []string{"pk", "marker", "ts", "geo"}, ConsistencyLevel: commonpb.ConsistencyLevel_Strong})
			s.Require().NoError(merr.CheckRPCCall(rows, err))
			s.Require().Len(rows.GetFieldsData(), 4)
			for _, field := range rows.GetFieldsData() {
				switch field.GetFieldName() {
				case "pk":
					s.Require().Equal([]int64{pk}, field.GetScalars().GetLongData().GetData())
				case "marker":
					s.Require().Equal([]int64{marker}, field.GetScalars().GetLongData().GetData())
				case "ts":
					s.Require().Len(field.GetScalars().GetStringData().GetData(), 1)
					actual, err := time.Parse(time.RFC3339Nano, field.GetScalars().GetStringData().GetData()[0])
					s.Require().NoError(err)
					s.Require().Equal(stamp, actual.UnixMicro())
				case "geo":
					s.Require().Len(field.GetScalars().GetGeometryWktData().GetData(), 1)
					actual, err := common.ConvertWKTToWKB(field.GetScalars().GetGeometryWktData().GetData()[0])
					s.Require().NoError(err)
					s.Require().Equal(wkb, actual)
				}
			}
		}
	}
	valid := []bool{true, false}
	_, err = producer.Append(ctx, build(1, valid))
	s.Require().NoError(err)
	check(1, valid)
	marker := int64(1)
	inserts := int64(2)
	for _, bits := range [][]bool{{false, false}, {true, true}, {true, false}} {
		_, err = producer.Append(ctx, build(marker+1, bits))
		s.Require().NoError(err)
		for _, pk := range []int64{11, 12} {
			s.assertRows(ctx, c, pk, marker, 0)
		}
		marker++
		inserts += 2
		check(marker, bits)
	}
	s.assertPhysicalRows(ctx, c, inserts)
	result, err = s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(result, err))
	result, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(result, err))
	s.WaitForLoad(ctx, name)
	check(marker, valid)
	for _, pk := range []int64{11, 12} {
		s.assertRows(ctx, c, pk, marker-1, 0)
	}
}

func (s *UpsertWALSuite) TestFunctionOutputSurvivesReplacementAndReload() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	name := "native_upsert_function_" + funcutil.GenRandomStr()
	schema := &schemapb.CollectionSchema{Name: name, Fields: []*schemapb.FieldSchema{
		{Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{Name: "marker", DataType: schemapb.DataType_Int64},
		{Name: "vector", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}},
		{Name: "text", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "256"}, {Key: "enable_analyzer", Value: "true"}}},
		{Name: "bm25", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
	}, Functions: []*schemapb.FunctionSchema{{Name: "bm25", Type: schemapb.FunctionType_BM25, InputFieldNames: []string{"text"}, OutputFieldNames: []string{"bm25"}}}}
	payload, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{CollectionName: name, Schema: payload, ShardsNum: 1})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	desc, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(desc, err))
	partitions, err := s.Cluster.MilvusClient.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(partitions, err))
	for field, params := range map[string][]*commonpb.KeyValuePair{
		"vector": integration.ConstructIndexParam(2, "FLAT", "L2"),
		"bm25":   {{Key: "index_type", Value: "SPARSE_INVERTED_INDEX"}, {Key: "metric_type", Value: "BM25"}},
	} {
		status, err = s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{CollectionName: name, FieldName: field, ExtraParams: params})
		s.Require().NoError(merr.CheckRPCCall(status, err))
		s.WaitForIndexBuilt(ctx, name, field)
	}
	status, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, name)
	c := collection{name: name, id: desc.GetCollectionID(), vchannel: desc.GetVirtualChannelNames()[0], partition: partitions.GetPartitionIDs()[0], schema: desc.GetSchema()}
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	for marker, text := range []string{"old document", "replacement document"} {
		raw := c.upsert(11, int64(marker+1))
		msg := message.MustAsMutableUpsertMessageV2(raw)
		body := msg.MustBody()
		for _, field := range c.schema.GetFields() {
			if field.GetName() == "text" {
				body.Insert.FieldsData = append(body.Insert.FieldsData, &schemapb.FieldData{FieldId: field.GetFieldID(), FieldName: field.GetName(), Type: field.GetDataType(), Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{text}}}}}})
			}
		}
		msg.OverwriteBody(body)
		_, err = producer.Append(ctx, raw)
		s.Require().NoError(err)
	}
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	s.flush(ctx, c)
	status, err = s.Cluster.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	status, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, name)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	response, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{CollectionName: name, Expr: "pk == 11", OutputFields: []string{"text"}, ConsistencyLevel: commonpb.ConsistencyLevel_Strong})
	s.Require().NoError(merr.CheckRPCCall(response, err))
	var texts []string
	for _, field := range response.GetFieldsData() {
		if field.GetFieldName() == "text" {
			texts = field.GetScalars().GetStringData().GetData()
		}
	}
	s.Require().Equal([]string{"replacement document"}, texts)
}

func (s *UpsertWALSuite) TestRowBasedInputIsRejectedBeforeWAL() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	_, err := producer.Append(ctx, c.upsert(11, 1))
	s.Require().NoError(err)
	msg := c.upsert(11, 2)
	typed := message.MustAsMutableUpsertMessageV2(msg)
	body := typed.MustBody()
	body.Insert.Version = msgpb.InsertDataVersion_RowBased
	typed.OverwriteBody(body)
	_, err = producer.Append(ctx, msg)
	s.Require().Error(err)
	s.assertRows(ctx, c, 11, 1, 1)
	s.assertRows(ctx, c, 11, 2, 0)
	s.assertPhysicalRows(ctx, c, 1)
}

func (s *UpsertWALSuite) TestVarCharAndResolvedAutoID() {
	for _, pkType := range []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_VarChar} {
		for _, autoID := range []bool{false, true} {
			s.Run(fmt.Sprintf("%s_autoID_%t", pkType, autoID), func() {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
				defer cancel()
				c := s.createWithPK(ctx, pkType, autoID)
				producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
				_, err := producer.Append(ctx, c.upsert(11, 1))
				s.Require().NoError(err)
				s.assertRows(ctx, c, 11, 1, 1)
				_, err = producer.Append(ctx, c.upsert(11, 2))
				s.Require().NoError(err)
				s.assertRows(ctx, c, 11, 1, 0)
				s.assertRows(ctx, c, 11, 2, 1)
			})
		}
	}
}

func (s *UpsertWALSuite) TestCrossPartitionReplacementInOneVChannel() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	_, err := producer.Append(ctx, c.upsert(11, 1))
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreatePartition(ctx, &milvuspb.CreatePartitionRequest{CollectionName: c.name, PartitionName: "other"})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	partitions, err := s.Cluster.MilvusClient.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{CollectionName: c.name})
	s.Require().NoError(merr.CheckRPCCall(partitions, err))
	other := c
	for index, name := range partitions.GetPartitionNames() {
		if name == "other" {
			other.partition = partitions.GetPartitionIDs()[index]
		}
	}
	s.Require().NotEqual(c.partition, other.partition)
	_, err = producer.Append(ctx, other.upsert(11, 2))
	s.Require().NoError(err)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
}

func (s *UpsertWALSuite) TestDuplicateKeysInOneMessageAreAccepted() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	_, err := producer.Append(ctx, c.upsert(11, 1))
	s.Require().NoError(err)
	msg := c.upsert(11, 2)
	typed := message.MustAsMutableUpsertMessageV2(msg)
	body := typed.MustBody()
	body.Insert.NumRows = 2
	body.Insert.RowIDs = append(body.Insert.RowIDs, body.Insert.RowIDs[0]+1)
	for _, field := range body.Insert.FieldsData {
		if field.GetScalars() != nil {
			field.GetScalars().GetLongData().Data = append(field.GetScalars().GetLongData().Data, field.GetScalars().GetLongData().Data...)
		} else {
			field.GetVectors().GetFloatVector().Data = append(field.GetVectors().GetFloatVector().Data, field.GetVectors().GetFloatVector().Data...)
		}
	}
	body.Delete.NumRows = 2
	body.Delete.PrimaryKeys.GetIntId().Data = []int64{11, 11}
	typed.OverwriteBody(body)
	header := typed.Header()
	header.Partition.Rows = 2
	header.DeleteRows = 2
	typed.OverwriteHeader(header)
	_, err = producer.Append(ctx, msg)
	s.Require().NoError(err)
	s.assertRows(ctx, c, 11, 1, 0)
	// Query may reduce equal-PK rows. Physical counts prove both inserts.
	rows, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{CollectionName: c.name, Expr: c.rowExpression(11, 2), OutputFields: []string{"pk", "marker"}, ConsistencyLevel: commonpb.ConsistencyLevel_Strong})
	s.Require().NoError(merr.CheckRPCCall(rows, err))
	s.Require().Len(rows.GetFieldsData(), 2)
	for _, field := range rows.GetFieldsData() {
		s.Require().NotEmpty(field.GetScalars().GetLongData().GetData())
	}
	s.assertPhysicalRows(ctx, c, 3)
}

func (s *UpsertWALSuite) TestRawProduceStaleSchemaRefreshAndRetry() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	old := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, old.vchannel)
	_, err := producer.Append(ctx, old.upsert(11, 1))
	s.Require().NoError(err)
	payload, err := proto.Marshal(&schemapb.FieldSchema{Name: "added", DataType: schemapb.DataType_Int64, Nullable: true})
	s.Require().NoError(err)
	result, err := s.Cluster.MilvusClient.AddCollectionField(ctx, &milvuspb.AddCollectionFieldRequest{CollectionName: old.name, Schema: payload})
	s.Require().NoError(merr.CheckRPCCall(result, err))
	refreshed, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: old.name})
	s.Require().NoError(merr.CheckRPCCall(refreshed, err))
	s.Require().Greater(refreshed.GetSchema().GetVersion(), old.schema.GetVersion())
	_, err = producer.Append(ctx, old.upsert(11, 2))
	s.Require().Error(err)
	s.Require().True(status.AsStreamingError(err).IsSchemaVersionMismatch(), "%v", err)
	c := old
	c.schema = refreshed.GetSchema()

	raw := c.upsert(11, 2)
	typed := message.MustAsMutableUpsertMessageV2(raw)
	body := typed.MustBody()
	for _, field := range c.schema.GetFields() {
		if field.GetName() == "added" {
			body.Insert.FieldsData = append(body.Insert.FieldsData, &schemapb.FieldData{FieldId: field.GetFieldID(), FieldName: field.GetName(), Type: field.GetDataType(), ValidData: []bool{true}, Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{42}}}}}})
		}
	}
	typed.OverwriteBody(body)
	_, err = producer.Append(ctx, raw)
	s.Require().NoError(err)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
}

func (s *UpsertWALSuite) TestProducerValidSparseRows() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	name := "native_upsert_sparse_" + funcutil.GenRandomStr()
	schema := &schemapb.CollectionSchema{Name: name, Fields: []*schemapb.FieldSchema{
		{Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{Name: "marker", DataType: schemapb.DataType_Int64},
		{Name: "vector", DataType: schemapb.DataType_SparseFloatVector},
	}}
	payload, err := proto.Marshal(schema)
	s.Require().NoError(err)
	result, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{CollectionName: name, Schema: payload, ShardsNum: 1})
	s.Require().NoError(merr.CheckRPCCall(result, err))
	desc, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(desc, err))
	partitions, err := s.Cluster.MilvusClient.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(partitions, err))
	result, err = s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{CollectionName: name, FieldName: "vector", ExtraParams: []*commonpb.KeyValuePair{{Key: "index_type", Value: "SPARSE_INVERTED_INDEX"}, {Key: "metric_type", Value: "IP"}}})
	s.Require().NoError(merr.CheckRPCCall(result, err))
	s.WaitForIndexBuilt(ctx, name, "vector")
	result, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(result, err))
	s.WaitForLoad(ctx, name)
	c := collection{name: name, id: desc.GetCollectionID(), vchannel: desc.GetVirtualChannelNames()[0], partition: partitions.GetPartitionIDs()[0], schema: desc.GetSchema()}
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	encode := func(indices []uint32, values []float32) []byte {
		row := make([]byte, len(indices)*8)
		for i, index := range indices {
			typeutil.SparseFloatRowSetAt(row, i, index, values[i])
		}
		return row
	}
	build := func(marker int64, row []byte) message.MutableMessage {
		raw := c.upsert(11, marker)
		typed := message.MustAsMutableUpsertMessageV2(raw)
		body := typed.MustBody()
		for _, field := range body.Insert.FieldsData {
			if field.GetFieldName() == "vector" {
				field.Field = &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 10, Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Dim: 10, Contents: [][]byte{row}}}}}
			}
		}
		typed.OverwriteBody(body)
		return raw
	}
	good := encode([]uint32{1, 3}, []float32{1, 2})
	_, err = producer.Append(ctx, build(1, good))
	s.Require().NoError(err)
	s.assertRows(ctx, c, 11, 1, 1)
	_, err = producer.Append(ctx, build(2, good))
	s.Require().NoError(err)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	s.assertPhysicalRows(ctx, c, 2)
}
