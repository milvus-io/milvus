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

package cmek

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/tests/integration/cmek/inspector"
)

func TestSelectFlushSegmentsDoesNotSubstituteCompactionChildren(t *testing.T) {
	original := &datapb.SegmentInfo{
		ID: 11, State: commonpb.SegmentState_Dropped, NumOfRows: 512, Compacted: true,
	}
	child := &datapb.SegmentInfo{
		ID: 22, State: commonpb.SegmentState_Flushed, NumOfRows: 512, CompactionFrom: []int64{11},
	}
	second := &datapb.SegmentInfo{
		ID: 12, State: commonpb.SegmentState_Flushed, NumOfRows: 256,
	}

	require.Equal(t, []*datapb.SegmentInfo{original}, selectFlushSegments([]int64{11}, []*datapb.SegmentInfo{original, child}))
	require.Empty(t, selectFlushSegments([]int64{11}, []*datapb.SegmentInfo{child}))
	require.Empty(t, selectFlushSegments([]int64{11, 12}, []*datapb.SegmentInfo{original, child}))
	require.Equal(t, []*datapb.SegmentInfo{original, second}, selectFlushSegments([]int64{11, 12}, []*datapb.SegmentInfo{original, second, child}))
}

func TestParquetReadSnapshotRequiresPhysicalAndServingIdentity(t *testing.T) {
	manifest := inspector.ManifestLocatorV3{BasePath: "collection/segment", Version: 7}
	expected := map[int64]inspector.ManifestLocatorV3{31: manifest}
	valid := func() v3ReadSnapshot {
		return v3ReadSnapshot{
			Loaded:  map[int64][]v3LoadedIdentity{31: {{NodeID: 5}}},
			Serving: map[int64][]v3ServingIdentity{31: {{NodeID: 5, MinimumManifestVersion: 7}}},
		}
	}
	require.True(t, valid().matches(expected, expected))
	for _, test := range []struct {
		name   string
		change func(*v3ReadSnapshot)
	}{
		{"growing", func(s *v3ReadSnapshot) { s.Growing = true }},
		{"not-serving", func(s *v3ReadSnapshot) { s.Unserviceable = true }},
		{"missing-physical", func(s *v3ReadSnapshot) { delete(s.Loaded, 31) }},
		{"missing-route", func(s *v3ReadSnapshot) { delete(s.Serving, 31) }},
		{"wrong-node", func(s *v3ReadSnapshot) { s.Serving[31][0].NodeID = 6 }},
		{"wrong-manifest", func(s *v3ReadSnapshot) { s.Serving[31][0].MinimumManifestVersion++ }},
		{"duplicate-base", func(s *v3ReadSnapshot) { s.Loaded[31] = append(s.Loaded[31], s.Loaded[31][0]) }},
		{"duplicate-route", func(s *v3ReadSnapshot) { s.Serving[31] = append(s.Serving[31], s.Serving[31][0]) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			snapshot := valid()
			test.change(&snapshot)
			require.False(t, snapshot.matches(expected, expected))
		})
	}
	require.False(t, valid().matches(expected, map[int64]inspector.ManifestLocatorV3{31: {BasePath: manifest.BasePath, Version: 8}}))
	require.False(t, (v3ReadSnapshot{}).matches(nil, nil))
}

func TestCompleteVectorIndexMetadataRequiresEveryField(t *testing.T) {
	segments := []*datapb.SegmentInfo{{ID: 31}}
	vectorFieldIDs := map[int64]struct{}{101: {}, 102: {}}
	response := &indexpb.GetIndexInfoResponse{SegmentInfo: map[int64]*indexpb.SegmentInfo{
		31: {SegmentID: 31},
	}}

	require.False(t, completeVectorIndexMetadata(response, segments, vectorFieldIDs))
	response.SegmentInfo[31].IndexInfos = []*indexpb.IndexFilePathInfo{{SegmentID: 31, FieldID: 101}}
	require.False(t, completeVectorIndexMetadata(response, segments, vectorFieldIDs))
	response.SegmentInfo[31].IndexInfos = append(response.SegmentInfo[31].IndexInfos,
		&indexpb.IndexFilePathInfo{SegmentID: 31, FieldID: 102})
	require.True(t, completeVectorIndexMetadata(response, segments, vectorFieldIDs))
	response.SegmentInfo[31].IndexInfos = append(response.SegmentInfo[31].IndexInfos,
		&indexpb.IndexFilePathInfo{SegmentID: 31, FieldID: 102})
	require.False(t, completeVectorIndexMetadata(response, segments, vectorFieldIDs))
}
