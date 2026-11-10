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

//go:build test

package taskresource

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/taskcommon"
)

// Fixtures shared by the per-family GetTaskResource tests of the datacoord
// task packages (index, stats, analyze, ...).

const (
	MiB = int64(1) << 20

	// FixtureCollectionID, FixturePartitionID and FixtureSegmentID identify the
	// one collection and segment NewFixtureMeta builds.
	FixtureCollectionID = int64(1)
	FixturePartitionID  = int64(2)
	FixtureSegmentID    = int64(3)

	// SystemFieldsBytesPerRow is RowID plus Timestamp, both int64: what every
	// segment stores on top of the schema's own fields.
	SystemFieldsBytesPerRow = 16

	// FixtureRows and FixtureSegmentSize are the fixture segment: 1000 rows of
	// (system fields + int64 pk + 128-dim float vector + 64-byte varchar).
	FixtureRows        = int64(1000)
	FixtureSegmentSize = FixtureRows * (SystemFieldsBytesPerRow + 8 + 512 + 64)

	// BigFixtureRows and BigFixtureSegmentSize are the fixture scaled up so
	// every price in play clears the minTaskMemory floor. Below the floor a
	// wrong price and the floor collapse to the same Resource.
	BigFixtureRows        = int64(1000000)
	BigFixtureSegmentSize = BigFixtureRows * (SystemFieldsBytesPerRow + 8 + 512 + 64)
)

// FixtureSchema is pk int64 (100), vec float32 dim 128 (101), str varchar(64) (102).
func FixtureSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}}},
			{FieldID: 102, Name: "str", DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "64"}}},
		},
	}
}

// NewFixtureMeta builds a meta with one cached collection (FixtureSchema) and
// one flushed segment in the V3 shape: no binlogs, Stats present. big scales
// the segment to BigFixtureRows.
func NewFixtureMeta(big bool) *meta.Meta {
	rows, size := FixtureRows, FixtureSegmentSize
	if big {
		rows, size = BigFixtureRows, BigFixtureSegmentSize
	}
	mt := &meta.Meta{
		MetaStore: metacache.NewMetaStore(nil),
		Segments: meta.NewSegmentsInfoWithSegments(map[int64]*meta.SegmentInfo{
			FixtureSegmentID: {SegmentInfo: &datapb.SegmentInfo{
				ID: FixtureSegmentID, CollectionID: FixtureCollectionID, PartitionID: FixturePartitionID,
				NumOfRows: rows, State: commonpb.SegmentState_Flushed, StorageVersion: 3, ManifestPath: "m",
				Stats: &datapb.Statistics{InsertBinlogSize: size},
			}},
		}),
	}
	PutFixtureSchema(mt, FixtureSchema())
	return mt
}

// PutFixtureSchema caches schema as the fixture collection's.
func PutFixtureSchema(mt *meta.Meta, schema *schemapb.CollectionSchema) {
	mt.MetaStore.PutCollection(&meta.CollectionInfo{ID: FixtureCollectionID, Schema: schema})
}

// DropFixtureSchema empties the collection cache, the state before it is warmed.
func DropFixtureSchema(mt *meta.Meta) {
	mt.MetaStore = metacache.NewMetaStore(nil)
}

// UpdateFixtureSegment rewrites the fixture segment through the SegmentsInfo
// setter, the only sanctioned way to mutate a stored segment.
func UpdateFixtureSegment(mt *meta.Meta, mutate func(seg *datapb.SegmentInfo)) {
	seg := mt.Segments.GetSegment(FixtureSegmentID).Clone()
	mutate(seg.SegmentInfo)
	mt.Segments.SetSegment(FixtureSegmentID, seg)
}

// Price and Resolved unpack the two-value GetTaskResource in assertions.
func Price(res taskcommon.Resource, _ bool) taskcommon.Resource { return res }

func Resolved(_ taskcommon.Resource, ok bool) bool { return ok }
