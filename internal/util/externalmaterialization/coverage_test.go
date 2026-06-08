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

package externalmaterialization

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestSegmentCoversFieldByChildFields(t *testing.T) {
	segment := &datapb.SegmentInfo{
		ManifestPath: "manifest",
		Binlogs: []*datapb.FieldBinlog{
			{ChildFields: []int64{100, 101}},
		},
	}

	assert.True(t, SegmentCoversField(segment, 100))
	assert.True(t, SegmentCoversField(segment, 101))
	assert.False(t, SegmentCoversField(segment, 102))
}

func TestSegmentCoversBM25Stats(t *testing.T) {
	segment := &datapb.SegmentInfo{
		ManifestPath: "manifest",
		Bm25Statslogs: []*datapb.FieldBinlog{
			{FieldID: 101, Binlogs: []*datapb.Binlog{{LogID: 1}}},
			{FieldID: 102},
		},
	}

	assert.True(t, SegmentCoversBM25Stats(segment, 101))
	assert.False(t, SegmentCoversBM25Stats(segment, 102))
}

func TestMissingFieldsForSegment(t *testing.T) {
	fields := []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id"},
		{FieldID: 101, Name: "text"},
		{FieldID: 102, Name: "score"},
	}
	segment := &datapb.SegmentInfo{
		ManifestPath: "manifest",
		Binlogs: []*datapb.FieldBinlog{
			{ChildFields: []int64{100, 101}},
		},
	}

	assert.Equal(t, []FieldRef{{FieldID: 102, FieldName: "score"}}, MissingFieldsForSegment(fields, segment, []int64{100, 102}))
}

func TestMissingFieldsForSegmentRequiresBM25StatsForSparseFunctionOutput(t *testing.T) {
	fields := []*schemapb.FieldSchema{
		{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
		{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		{FieldID: 102, Name: "sparse_without_flag", DataType: schemapb.DataType_SparseFloatVector},
	}
	segment := &datapb.SegmentInfo{
		ManifestPath: "manifest",
		Binlogs: []*datapb.FieldBinlog{
			{ChildFields: []int64{100, 101, 102}},
		},
	}

	assert.Equal(t,
		[]FieldRef{
			{FieldID: 101, FieldName: "sparse"},
			{FieldID: 102, FieldName: "sparse_without_flag"},
		},
		MissingFieldsForSegment(fields, segment, []int64{101, 102}),
	)

	segment.Bm25Statslogs = []*datapb.FieldBinlog{
		{FieldID: 101, Binlogs: []*datapb.Binlog{{LogID: 1}}},
		{FieldID: 102, Binlogs: []*datapb.Binlog{{LogID: 2}}},
	}
	assert.Empty(t, MissingFieldsForSegment(fields, segment, []int64{101, 102}))
}

func TestMissingFieldsForSegmentDeduplicatesAndSorts(t *testing.T) {
	fields := []*schemapb.FieldSchema{
		nil,
		{FieldID: 100, Name: "id"},
		{FieldID: 102, Name: "score"},
	}
	segment := &datapb.SegmentInfo{
		ManifestPath: "manifest",
		Binlogs: []*datapb.FieldBinlog{
			nil,
			{FieldID: 100},
		},
	}

	assert.Equal(t,
		[]FieldRef{
			{FieldID: 101},
			{FieldID: 102, FieldName: "score"},
		},
		MissingFieldsForSegment(fields, segment, []int64{102, 101, 102, 100}),
	)
}

func TestSegmentCoverageRequiresManifest(t *testing.T) {
	segment := &datapb.SegmentInfo{
		Binlogs: []*datapb.FieldBinlog{
			{FieldID: 100, ChildFields: []int64{101}},
		},
		Bm25Statslogs: []*datapb.FieldBinlog{
			{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 1}}},
		},
	}

	assert.False(t, SegmentCoversField(nil, 100))
	assert.False(t, SegmentCoversField(segment, 100))
	assert.False(t, SegmentCoversField(segment, 101))
	assert.False(t, SegmentCoversBM25Stats(nil, 100))
	assert.False(t, SegmentCoversBM25Stats(segment, 100))
}
