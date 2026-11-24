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

package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

func TestWithoutJSONFlatIndexFiles(t *testing.T) {
	snapshot := &SnapshotData{
		Collection: &datapb.CollectionDescription{Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 101, DataType: schemapb.DataType_JSON}}}},
		Indexes:    []*indexpb.IndexInfo{{IndexID: 1, FieldID: 101, IndexParams: []*commonpb.KeyValuePair{{Key: common.JSONCastTypeKey, Value: "JSON"}}}},
		Segments: []*datapb.SegmentDescription{{SegmentId: 100, IndexFiles: []*indexpb.IndexFilePathInfo{
			{IndexID: 1, FieldID: 101, BuildID: 10, IndexFilePaths: []string{"retired"}},
			{IndexID: 2, FieldID: 101, BuildID: 11, IndexFilePaths: []string{"typed"}, IndexParams: []*commonpb.KeyValuePair{{Key: common.JSONCastTypeKey, Value: "VARCHAR"}}},
			{IndexID: 3, FieldID: 101, BuildID: 12, IndexFilePaths: []string{"retired-by-params"}, IndexParams: []*commonpb.KeyValuePair{{Key: common.JSONCastTypeKey, Value: "JSON"}}},
		}}},
		BuildIDs: []int64{10, 11, 12},
	}
	physical := WithoutJSONFlatIndexFiles(snapshot)
	require.Len(t, physical.Segments[0].GetIndexFiles(), 1)
	assert.EqualValues(t, 2, physical.Segments[0].GetIndexFiles()[0].GetIndexID())
	assert.Equal(t, []int64{11}, physical.BuildIDs)
	assert.Len(t, physical.Indexes, 1)
	assert.Len(t, snapshot.Segments[0].GetIndexFiles(), 3)
	assert.Len(t, snapshot.BuildIDs, 3)
}
