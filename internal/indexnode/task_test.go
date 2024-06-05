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

package indexnode

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	milvus_storage "github.com/milvus-io/milvus-storage/go/storage"
	"github.com/milvus-io/milvus-storage/go/storage/options"
	"github.com/milvus-io/milvus-storage/go/storage/schema"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type IndexBuildTaskV2Suite struct {
	suite.Suite
	schema      *schemapb.CollectionSchema
	arrowSchema *arrow.Schema
	space       *milvus_storage.Space
}

func (suite *IndexBuildTaskV2Suite) SetupSuite() {
	paramtable.Init()
}

func (suite *IndexBuildTaskV2Suite) SetupTest() {
	suite.schema = &schemapb.CollectionSchema{
		Name:        "test",
		Description: "test",
		AutoID:      false,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 1, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 2, Name: "ts", DataType: schemapb.DataType_Int64},
			{FieldID: 3, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "1"}}},
		},
	}

	var err error
	suite.arrowSchema, err = typeutil.ConvertToArrowSchema(suite.schema.Fields)
	suite.NoError(err)

	tmpDir := suite.T().TempDir()
	opt := options.NewSpaceOptionBuilder().
		SetSchema(schema.NewSchema(
			suite.arrowSchema,
			&schema.SchemaOptions{
				PrimaryColumn: "pk",
				VectorColumn:  "vec",
				VersionColumn: "ts",
			})).
		Build()
	suite.space, err = milvus_storage.Open("file://"+tmpDir, opt)
	suite.NoError(err)

	b := array.NewRecordBuilder(memory.DefaultAllocator, suite.arrowSchema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1}, nil)
	b.Field(1).(*array.Int64Builder).AppendValues([]int64{1}, nil)
	fb := b.Field(2).(*array.FixedSizeBinaryBuilder)
	fb.Reserve(1)
	fb.Append([]byte{1, 2, 3, 4})

	rec := b.NewRecord()
	defer rec.Release()
	reader, err := array.NewRecordReader(suite.arrowSchema, []arrow.Record{rec})
	suite.NoError(err)
	err = suite.space.Write(reader, &options.DefaultWriteOptions)
	suite.NoError(err)
}

func (suite *IndexBuildTaskV2Suite) TestBuildIndex() {
	req := &indexpb.CreateJobRequest{
		BuildID:      1,
		IndexVersion: 1,
		IndexID:      0,
		IndexName:    "",
		IndexParams:  []*commonpb.KeyValuePair{{Key: common.IndexTypeKey, Value: "FLAT"}, {Key: common.MetricTypeKey, Value: metric.L2}, {Key: common.DimKey, Value: "1"}},
		TypeParams:   []*commonpb.KeyValuePair{{Key: "dim", Value: "1"}},
		NumRows:      10,
		StorageConfig: &indexpb.StorageConfig{
			RootPath:    "/tmp/milvus/data",
			StorageType: "local",
		},
		CollectionID:   1,
		PartitionID:    1,
		SegmentID:      1,
		FieldID:        3,
		FieldName:      "vec",
		FieldType:      schemapb.DataType_FloatVector,
		StorePath:      "file://" + suite.space.Path(),
		StoreVersion:   suite.space.GetCurrentVersion(),
		IndexStorePath: "file://" + suite.space.Path(),
		Dim:            4,
		OptionalScalarFields: []*indexpb.OptionalFieldInfo{
			{FieldID: 1, FieldName: "pk", FieldType: 5, DataIds: []int64{0}},
		},
	}

	task := newIndexBuildTaskV2(context.Background(), nil, req, NewIndexNode(context.Background(), dependency.NewDefaultFactory(true)))

	var err error
	err = task.Prepare(context.Background())
	suite.NoError(err)
	err = task.BuildIndex(context.Background())
	suite.NoError(err)
	err = task.SaveIndexFiles(context.Background())
	suite.NoError(err)
}

func TestIndexBuildTaskV2Suite(t *testing.T) {
	suite.Run(t, new(IndexBuildTaskV2Suite))
}
