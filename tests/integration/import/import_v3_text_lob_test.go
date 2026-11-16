// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package importv2

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

// This checks correctness through the actual Import V3 final writer and query
// path. The deterministic ownership assertion belongs in the packed FFI test;
// process RSS in this MiniCluster also includes the client and input fixture.
type ImportV3TextLOBSuite struct {
	integration.MiniClusterSuite
}

func (s *ImportV3TextLOBSuite) SetupSuite() {
	configureImportV3(&s.MiniClusterSuite)
	s.WithMilvusConfig(paramtable.Get().DataNodeCfg.ImportBaseBufferSize.Key, "0.5")
	s.WithMilvusConfig(paramtable.Get().DataNodeCfg.TextInlineThreshold.Key, "256")
	s.MiniClusterSuite.SetupSuite()
}

func importV3TextPayload(id int64, column int) string {
	prefix := fmt.Sprintf("text-%d-%d-", column, id)
	switch (id + int64(column)) % 6 {
	case 0:
		return ""
	case 1:
		return prefix + "inline-中文"
	case 2:
		return prefix + strings.Repeat("a", (64<<10)-1-len(prefix))
	case 3:
		return prefix + strings.Repeat("b", (64<<10)-len(prefix))
	case 4:
		return prefix + strings.Repeat("c", (64<<10)+1-len(prefix))
	default:
		return prefix + strings.Repeat("文本", 44<<10)
	}
}

func importV3TextVector(id int64) []float32 {
	return []float32{float32(id), float32(id) + 0.25, float32(id) + 0.5, float32(id) + 0.75}
}

func (s *ImportV3TextLOBSuite) TestLargeTextImportAndReload() {
	const rowCount = 512
	c := s.Cluster
	ctx, cancel := context.WithTimeout(c.GetContext(), 10*time.Minute)
	defer cancel()
	name := "ImportV3TextLOB_" + funcutil.RandomString(8)
	schema := integration.ConstructSchema(name, 4, false,
		&schemapb.FieldSchema{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		&schemapb.FieldSchema{FieldID: 101, Name: "content", DataType: schemapb.DataType_Text},
		&schemapb.FieldSchema{FieldID: 102, Name: "extra", DataType: schemapb.DataType_Text},
		&schemapb.FieldSchema{FieldID: 103, Name: "embedding", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}}},
		&schemapb.FieldSchema{FieldID: 104, Name: "tag", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "128"}}},
	)
	encoded, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: name, Schema: encoded, ShardsNum: 1,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))

	// Multiple small Parquet row groups exercise repeated input batches. The
	// raw text exceeds the usual writer/LOB buffer sizes while scalar tails stay small.
	inputSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "content", Type: arrow.BinaryTypes.String},
		{Name: "extra", Type: arrow.BinaryTypes.String},
		{Name: "embedding", Type: arrow.ListOf(arrow.PrimitiveTypes.Float32)},
		{Name: "tag", Type: arrow.BinaryTypes.String},
	}, nil)
	var data bytes.Buffer
	fileWriter, err := pqarrow.NewFileWriter(inputSchema, &data,
		parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(8)), pqarrow.DefaultWriterProps())
	s.Require().NoError(err)
	writeOK := false
	defer func() {
		if !writeOK {
			_ = fileWriter.Close()
		}
	}()
	builder := array.NewRecordBuilder(memory.DefaultAllocator, inputSchema)
	defer builder.Release()
	for start := 0; start < rowCount; start += 8 {
		for i := start; i < start+8; i++ {
			id := int64(i)
			builder.Field(0).(*array.Int64Builder).Append(id)
			builder.Field(1).(*array.StringBuilder).Append(importV3TextPayload(id, 1))
			builder.Field(2).(*array.StringBuilder).Append(importV3TextPayload(id, 2))
			vector := builder.Field(3).(*array.ListBuilder)
			vector.Append(true)
			vector.ValueBuilder().(*array.Float32Builder).AppendValues(importV3TextVector(id), nil)
			builder.Field(4).(*array.StringBuilder).Append(fmt.Sprintf("tag-%d", id))
		}
		record := builder.NewRecord()
		err = fileWriter.Write(record)
		record.Release()
		s.Require().NoError(err)
	}
	s.Require().NoError(fileWriter.Close())
	writeOK = true
	inputPath := path.Join(c.RootPath(), "parquet", name+".parquet")
	s.Require().NoError(c.ChunkManager.Write(ctx, inputPath, data.Bytes()))
	response, err := c.ProxyClient.ImportV2(ctx, &internalpb.ImportRequest{
		CollectionName: name, Files: []*internalpb.ImportFile{{Paths: []string{inputPath}}},
		Options: []*commonpb.KeyValuePair{{Key: "auto_commit", Value: "true"}},
	})
	s.Require().NoError(err)
	s.Require().NoError(merr.CheckRPCCall(response.GetStatus(), nil))
	s.Require().NoError(WaitForImportDone(ctx, c, response.GetJobID()))

	segments, err := c.ShowSegments(name)
	s.Require().NoError(err)
	s.Require().NotEmpty(segments)
	var segmentRows int64
	for _, segment := range segments {
		s.EqualValues(storage.StorageV3, segment.GetStorageVersion())
		s.NotEmpty(segment.GetManifestPath())
		s.Equal(commonpb.SegmentState_Flushed, segment.GetState())
		segmentRows += segment.GetNumOfRows()
	}
	s.EqualValues(rowCount, segmentRows)
	status, err = c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: name, FieldName: "embedding", IndexName: "vector",
		ExtraParams: integration.ConstructIndexParam(4, "HNSW", "L2"),
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForIndexBuilt(ctx, name, "embedding")
	for pass := 0; pass < 2; pass++ {
		status, err = c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
		s.Require().NoError(merr.CheckRPCCall(status, err))
		s.WaitForLoad(ctx, name)
		seen := make(map[int64]struct{}, rowCount)
		for start := 0; start < rowCount; start += 8 {
			result, err := c.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
				CollectionName: name, Expr: fmt.Sprintf("id >= %d && id < %d", start, start+8),
				OutputFields:     []string{"id", "content", "extra", "embedding", "tag"},
				ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
			})
			s.Require().NoError(merr.CheckRPCCall(result, err))
			fields := make(map[string]*schemapb.FieldData)
			for _, field := range result.GetFieldsData() {
				fields[field.GetFieldName()] = field
			}
			for _, field := range []string{"id", "content", "extra", "embedding", "tag"} {
				s.Require().Contains(fields, field)
			}
			ids := fields["id"].GetScalars().GetLongData().GetData()
			s.Require().Len(ids, 8)
			texts := fields["content"].GetScalars().GetStringData().GetData()
			extras := fields["extra"].GetScalars().GetStringData().GetData()
			vectors := fields["embedding"].GetVectors().GetFloatVector().GetData()
			tags := fields["tag"].GetScalars().GetStringData().GetData()
			s.Require().Len(texts, len(ids))
			s.Require().Len(extras, len(ids))
			s.Require().Len(vectors, len(ids)*4)
			s.Require().Len(tags, len(ids))
			for row, id := range ids {
				s.Require().GreaterOrEqual(id, int64(start))
				s.Require().Less(id, int64(start+8))
				_, duplicate := seen[id]
				s.False(duplicate, "duplicate row %d", id)
				seen[id] = struct{}{}
				s.Equal(sha256.Sum256([]byte(importV3TextPayload(id, 1))), sha256.Sum256([]byte(texts[row])), "content row %d pass %d", id, pass)
				s.Equal(sha256.Sum256([]byte(importV3TextPayload(id, 2))), sha256.Sum256([]byte(extras[row])), "extra row %d pass %d", id, pass)
				s.Equal(importV3TextVector(id), vectors[row*4:(row+1)*4])
				s.Equal(fmt.Sprintf("tag-%d", id), tags[row])
			}
		}
		s.Len(seen, rowCount)
		status, err = c.MilvusClient.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: name})
		s.Require().NoError(merr.CheckRPCCall(status, err))
	}
}

func TestImportV3TextLOBSuite(t *testing.T) {
	suite.Run(t, new(ImportV3TextLOBSuite))
}
