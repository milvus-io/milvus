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

package function

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

func TestTextTermCollectorDrainsPerSegmentGeneration(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.EnableAnalyzerKey, Value: "true"},
				},
			},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Name:           "bm25",
				Type:           schemapb.FunctionType_BM25,
				InputFieldIds:  []int64{101},
				OutputFieldIds: []int64{102},
				Params:         []*commonpb.KeyValuePair{{Key: common.EnableFuzzyKey, Value: "true"}},
			},
		},
	}

	collector, err := NewTextTermCollector(schema)
	require.NoError(t, err)
	t.Cleanup(collector.Close)
	require.True(t, collector.Enabled())

	require.NoError(t, collector.Collect(map[int64][]string{101: {"hello world", "hello fuzzy"}}))
	require.NoError(t, collector.Collect(map[int64][]string{101: {"world again"}}))

	first := collector.Drain()
	require.Len(t, first, 1)
	require.ElementsMatch(t, [][]byte{[]byte("again"), []byte("fuzzy"), []byte("hello"), []byte("world")}, first[101])
	require.Nil(t, collector.Drain())

	require.NoError(t, collector.Collect(map[int64][]string{101: {"next segment"}}))
	second := collector.Drain()
	require.Equal(t, [][]byte{[]byte("next"), []byte("segment")}, second[101])
}

func TestTextTermCollectorRequiresEveryRunnerInput(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.EnableAnalyzerKey, Value: "true"},
				},
			},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
		Functions: []*schemapb.FunctionSchema{{
			Name:           "bm25",
			Type:           schemapb.FunctionType_BM25,
			InputFieldIds:  []int64{101},
			OutputFieldIds: []int64{102},
			Params:         []*commonpb.KeyValuePair{{Key: common.EnableFuzzyKey, Value: "true"}},
		}},
	}
	collector, err := NewTextTermCollector(schema)
	require.NoError(t, err)
	t.Cleanup(collector.Close)
	require.ErrorContains(t, collector.Collect(nil), "input field 101 is missing")
}

func TestTextTermCollectorCollectInsertDataSkipsNullRows(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  101,
				Name:     "text",
				DataType: schemapb.DataType_VarChar,
				Nullable: true,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.EnableAnalyzerKey, Value: "true"},
				},
			},
			{FieldID: 102, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector},
		},
		Functions: []*schemapb.FunctionSchema{{
			Name:           "bm25",
			Type:           schemapb.FunctionType_BM25,
			InputFieldIds:  []int64{101},
			OutputFieldIds: []int64{102},
			Params:         []*commonpb.KeyValuePair{{Key: common.EnableFuzzyKey, Value: "true"}},
		}},
	}
	collector, err := NewTextTermCollector(schema)
	require.NoError(t, err)
	t.Cleanup(collector.Close)

	err = collector.CollectInsertData(&storage.InsertData{Data: map[int64]storage.FieldData{
		101: &storage.StringFieldData{
			Data:      []string{"kept value", "must not appear"},
			ValidData: []bool{true, false},
			Nullable:  true,
		},
	}})
	require.NoError(t, err)
	require.ElementsMatch(t, [][]byte{[]byte("kept"), []byte("value")}, collector.Drain()[101])
}
