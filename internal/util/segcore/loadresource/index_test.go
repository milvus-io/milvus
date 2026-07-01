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

package loadresource

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestEstimateIndexLoadResourceDoesNotReadIndexFiles(t *testing.T) {
	field := &schemapb.FieldSchema{
		FieldID:  100,
		Name:     "vec",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "128"},
		},
	}
	segment := &querypb.SegmentLoadInfo{
		CollectionID: 1,
		PartitionID:  2,
		SegmentID:    3,
		NumOfRows:    1000,
	}
	index := &querypb.FieldIndexInfo{
		FieldID:   100,
		IndexID:   10,
		BuildID:   11,
		IndexSize: 1024 * 1024,
		NumRows:   1000,
		IndexParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "HNSW"},
			{Key: common.MetricTypeKey, Value: "L2"},
		},
		IndexFilePaths: []string{"/path/that/must/not/exist/index_file"},
	}

	usage, err := EstimateIndexLoadResource(context.Background(), field, segment, index)

	require.NoError(t, err)
	require.NotZero(t, usage.FinalMemoryBytes)
	require.True(t, usage.HasRawData)
}

func TestEstimateIndexLoadResourceWithRunner(t *testing.T) {
	field := &schemapb.FieldSchema{
		FieldID:  100,
		Name:     "vec",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "128"},
		},
	}
	segment := &querypb.SegmentLoadInfo{
		CollectionID: 1,
		PartitionID:  2,
		SegmentID:    3,
		NumOfRows:    1000,
	}
	index := &querypb.FieldIndexInfo{
		FieldID:   100,
		IndexID:   10,
		BuildID:   11,
		IndexSize: 1024 * 1024,
		NumRows:   1000,
		IndexParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "HNSW"},
			{Key: common.MetricTypeKey, Value: "L2"},
		},
	}
	calls := 0
	runner := func(fn func() error) error {
		calls++
		return fn()
	}

	usage, err := EstimateIndexLoadResourceWithRunner(context.Background(), field, segment, index, runner)

	require.NoError(t, err)
	require.NotZero(t, usage.FinalMemoryBytes)
	require.Equal(t, 1, calls)
}

func TestBuildCLoadIndexInfoPreservesIndexNumRows(t *testing.T) {
	field := &schemapb.FieldSchema{
		FieldID:  100,
		Name:     "vec",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "128"},
		},
	}
	segment := &querypb.SegmentLoadInfo{
		CollectionID: 1,
		PartitionID:  2,
		SegmentID:    3,
		NumOfRows:    1000,
	}
	index := &querypb.FieldIndexInfo{
		FieldID:   100,
		IndexID:   10,
		BuildID:   11,
		IndexSize: 1024 * 1024,
		IndexParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "HNSW"},
			{Key: common.MetricTypeKey, Value: "L2"},
		},
	}

	info, err := buildCLoadIndexInfo(field, segment, index)

	require.NoError(t, err)
	require.Zero(t, info.GetNumRows())
}

func TestBuildCLoadIndexInfoPreservesIndexStorePathVersion(t *testing.T) {
	field := &schemapb.FieldSchema{
		FieldID:  100,
		Name:     "vec",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "128"},
		},
	}
	segment := &querypb.SegmentLoadInfo{
		CollectionID: 1,
		PartitionID:  2,
		SegmentID:    3,
		NumOfRows:    1000,
	}
	index := &querypb.FieldIndexInfo{
		FieldID:               100,
		IndexID:               10,
		BuildID:               11,
		IndexSize:             1024 * 1024,
		IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED,
		IndexParams: []*commonpb.KeyValuePair{
			{Key: common.IndexTypeKey, Value: "HNSW"},
			{Key: common.MetricTypeKey, Value: "L2"},
		},
	}

	info, err := buildCLoadIndexInfo(field, segment, index)

	require.NoError(t, err)
	require.Equal(t, indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_COLLECTION_ROOTED, info.GetIndexStorePathVersion())
}

func TestEstimateIndexLoadResourceReturnsErrorWithoutIndexType(t *testing.T) {
	field := &schemapb.FieldSchema{
		FieldID:  100,
		Name:     "vec",
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "128"},
		},
	}
	segment := &querypb.SegmentLoadInfo{
		CollectionID: 1,
		PartitionID:  2,
		SegmentID:    3,
		NumOfRows:    1000,
	}
	index := &querypb.FieldIndexInfo{
		FieldID:   100,
		IndexID:   10,
		BuildID:   11,
		IndexSize: 1024 * 1024,
		NumRows:   1000,
	}

	usage, err := EstimateIndexLoadResource(context.Background(), field, segment, index)

	require.Error(t, err)
	require.Zero(t, usage)
}
