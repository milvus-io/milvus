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

package metacache

import (
	"testing"

	prometheustestutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func numSegments(seg *datapb.SegmentInfo) float64 {
	labels := segMetricLabels(seg)
	return prometheustestutil.ToFloat64(metrics.DataCoordNumSegments.WithLabelValues(labels[:]...))
}

func TestSegmentMetricEmission(t *testing.T) {
	growing := &datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Growing, Level: datapb.SegmentLevel_L1}
	flushed := &datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1}
	flushedSorted := &datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1, IsSorted: true}
	flushedRows := &datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1, NumOfRows: 10}

	tests := []struct {
		name string
		ops  func(s MetaStore)
		// want maps a representative segment (for its labels) to its expected count.
		want map[*datapb.SegmentInfo]float64
	}{
		{
			name: "put new segment",
			ops:  func(s MetaStore) { s.PutSegment(growing) },
			want: map[*datapb.SegmentInfo]float64{growing: 1},
		},
		{
			name: "state change moves the count",
			ops: func(s MetaStore) {
				s.PutSegment(growing)
				s.PutSegment(flushed)
			},
			want: map[*datapb.SegmentInfo]float64{growing: 0, flushed: 1},
		},
		{
			name: "label-only change moves the count",
			ops: func(s MetaStore) {
				s.PutSegment(flushed)
				s.PutSegment(flushedSorted)
			},
			want: map[*datapb.SegmentInfo]float64{flushed: 0, flushedSorted: 1},
		},
		{
			name: "update without label change keeps the count",
			ops: func(s MetaStore) {
				s.PutSegment(flushed)
				s.PutSegment(flushedRows)
			},
			want: map[*datapb.SegmentInfo]float64{flushed: 1},
		},
		{
			name: "remove segment",
			ops: func(s MetaStore) {
				s.PutSegment(flushed)
				s.RemoveSegment(1)
			},
			want: map[*datapb.SegmentInfo]float64{flushed: 0},
		},
		{
			name: "remove missing segment is a no-op",
			ops:  func(s MetaStore) { s.RemoveSegment(1) },
			want: map[*datapb.SegmentInfo]float64{flushed: 0},
		},
		{
			name: "bulk load does not emit",
			ops:  func(s MetaStore) { s.LoadSegments([]*datapb.SegmentInfo{flushed}) },
			want: map[*datapb.SegmentInfo]float64{flushed: 0},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			metrics.DataCoordNumSegments.Reset()
			defer metrics.DataCoordNumSegments.Reset()
			test.ops(NewMetaStore(nil))
			for seg, want := range test.want {
				assert.Equal(t, want, numSegments(seg))
			}
		})
	}
}

func TestSegFormatLabel(t *testing.T) {
	tests := []struct {
		name    string
		segment *datapb.SegmentInfo
		want    string
	}{
		{
			name: "legacy storage without format",
			segment: &datapb.SegmentInfo{
				StorageVersion: storage.StorageV1,
			},
			want: "legacy",
		},
		{
			name: "storage v2 without format",
			segment: &datapb.SegmentInfo{
				StorageVersion: storage.StorageV2,
			},
			want: "unknown",
		},
		{
			name: "storage v3 parquet",
			segment: &datapb.SegmentInfo{
				StorageVersion: storage.StorageV3,
				Binlogs: []*datapb.FieldBinlog{
					{Format: "parquet"},
					{Format: "parquet"},
				},
			},
			want: "parquet",
		},
		{
			name: "storage v3 external iceberg table",
			segment: &datapb.SegmentInfo{
				StorageVersion: storage.StorageV3,
				Binlogs: []*datapb.FieldBinlog{
					{Format: "iceberg-table"},
				},
			},
			want: "iceberg-table",
		},
		{
			name: "storage v3 external lance table",
			segment: &datapb.SegmentInfo{
				StorageVersion: storage.StorageV3,
				Binlogs: []*datapb.FieldBinlog{
					{Format: "lance-table"},
				},
			},
			want: "lance-table",
		},
		{
			name: "mixed column group formats",
			segment: &datapb.SegmentInfo{
				StorageVersion: storage.StorageV3,
				Binlogs: []*datapb.FieldBinlog{
					{Format: "parquet"},
					{Format: "vortex"},
				},
			},
			want: "mixed",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, segFormatLabel(test.segment))
		})
	}
}
