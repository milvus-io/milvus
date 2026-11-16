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
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// segMemCase is one shape of the segment memory benchmark.
type segMemCase struct {
	name string
	// fromCatalog installs through LoadFromCatalog (DataCoord startup);
	// otherwise through LoadSegments.
	fromCatalog bool
	nullable    int  // nullable fields, on top of 20 not-nullable ones
	binlogs     bool // storage V2 with per-field binlogs instead of a manifest
	msgGroup    string
}

var segMemCases = []segMemCase{
	{name: "catalog/v3/2-nullable", fromCatalog: true, nullable: 2},
	{name: "catalog/v3/0-nullable", fromCatalog: true},
	{name: "catalog/v3/2-nullable/legacy-msggroup", fromCatalog: true, nullable: 2, msgGroup: "by-dev-dataNode-12-by-dev-rootcoord-dml_%d"},
	{name: "catalog/v2-binlogs/2-nullable", fromCatalog: true, nullable: 2, binlogs: true},
	{name: "load-segments/v3/2-nullable", nullable: 2},
}

const (
	segMemCollectionID = 454000000000000001
	segMemPartitionID  = 454000000000000002
	segMemChannels     = 4
	segMemSegments     = 50000
)

// segMemSchema returns 20 fields that are not nullable (RowID and Timestamp,
// which RootCoord adds, the primary key, 15 scalars and 2 vectors), followed
// by nullable scalar fields.
func segMemSchema(nullable int) *schemapb.CollectionSchema {
	fields := []*schemapb.FieldSchema{
		{FieldID: 0, Name: "RowID", DataType: schemapb.DataType_Int64},
		{FieldID: 1, Name: "Timestamp", DataType: schemapb.DataType_Int64},
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}
	scalarTypes := []schemapb.DataType{
		schemapb.DataType_Int64, schemapb.DataType_VarChar, schemapb.DataType_Float,
		schemapb.DataType_Bool, schemapb.DataType_Int32, schemapb.DataType_JSON,
	}
	id := int64(101)
	for i := 0; i < 15; i++ {
		fields = append(fields, &schemapb.FieldSchema{FieldID: id, Name: fmt.Sprintf("s%d", i), DataType: scalarTypes[i%len(scalarTypes)]})
		id++
	}
	for i := 0; i < 2; i++ {
		fields = append(fields, &schemapb.FieldSchema{FieldID: id, Name: fmt.Sprintf("v%d", i), DataType: schemapb.DataType_FloatVector})
		id++
	}
	for i := 0; i < nullable; i++ {
		fields = append(fields, &schemapb.FieldSchema{FieldID: id, Name: fmt.Sprintf("n%d", i), DataType: schemapb.DataType_VarChar, Nullable: true})
		id++
	}
	return &schemapb.CollectionSchema{Name: "bench", Fields: fields}
}

func segMemChannel(i int) string {
	return fmt.Sprintf("by-dev-rootcoord-dml_%d_%dv0", i, segMemCollectionID)
}

// segMemTemplates returns one marshaled flushed segment per channel, shaped
// like the ones the catalog stores: decoding them yields fresh strings, as a
// catalog load does.
func segMemTemplates(c segMemCase, schema *schemapb.CollectionSchema) [][]byte {
	templates := make([][]byte, segMemChannels)
	for ch := range templates {
		channel := segMemChannel(ch)
		msgGroup := c.msgGroup
		if msgGroup != "" {
			msgGroup = fmt.Sprintf(msgGroup, ch)
		}
		pos := func(ts uint64) *msgpb.MsgPosition {
			return &msgpb.MsgPosition{
				ChannelName: channel,
				MsgID:       []byte{8, 0xd2, 0x09, 0x10, 0x86, 0x03, 0x18, 0x00, 0x20, 0x01, 0x28, 0x00},
				MsgGroup:    msgGroup,
				Timestamp:   ts,
			}
		}
		nullCounts := make(map[int64]int64, len(schema.GetFields()))
		for _, f := range schema.GetFields() {
			nullCounts[f.GetFieldID()] = 0
		}
		seg := &datapb.SegmentInfo{
			CollectionID:  segMemCollectionID,
			PartitionID:   segMemPartitionID,
			InsertChannel: channel,
			NumOfRows:     1_000_000,
			State:         commonpb.SegmentState_Flushed,
			StartPosition: pos(454000000000000000),
			DmlPosition:   pos(454000000100000000),
			Level:         datapb.SegmentLevel_L1,
			IsSorted:      true,
			Stats: &datapb.Statistics{
				InsertBinlogSize:   512 << 20,
				StatsBinlogSize:    4 << 20,
				InsertBinlogCount:  44,
				TimestampQuantiles: []int64{454000000020000000, 454000000040000000, 454000000060000000, 454000000080000000, 454000000100000000},
				TimestampFrom:      454000000000000000,
				TimestampTo:        454000000100000000,
				NullCounts:         nullCounts,
				Formats:            []string{"parquet"},
				LoadResource: &datapb.LoadResourceStatistics{ColumnGroups: []*datapb.ColumnGroupStatistics{
					{GroupId: 0, FieldIds: []int64{0, 1, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115}, MemorySize: 96 << 20},
					{GroupId: 116, FieldIds: []int64{116}, MemorySize: 200 << 20},
					{GroupId: 117, FieldIds: []int64{117}, MemorySize: 200 << 20},
				}},
			},
		}
		if c.binlogs {
			seg.StorageVersion = 2
			for _, f := range schema.GetFields() {
				fb := &datapb.FieldBinlog{FieldID: f.GetFieldID()}
				for j := 0; j < 2; j++ {
					fb.Binlogs = append(fb.Binlogs, &datapb.Binlog{
						EntriesNum: 500_000, TimestampFrom: 454000000000000000, TimestampTo: 454000000100000000,
						LogSize: 8 << 20, MemorySize: 12 << 20, LogID: 454000000000100000 + int64(j),
					})
				}
				seg.Binlogs = append(seg.Binlogs, fb)
			}
			seg.Statslogs = []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{EntriesNum: 1_000_000, LogSize: 2 << 20, MemorySize: 2 << 20, LogID: 454000000000200000}}}}
		} else {
			seg.StorageVersion = 3
		}
		data, err := proto.Marshal(seg)
		if err != nil {
			panic(err)
		}
		templates[ch] = data
	}
	return templates
}

// decodeSegMemSegments decodes n fresh segments round-robin over the channel
// templates, giving each its own ID and, for storage V3, manifest path.
func decodeSegMemSegments(c segMemCase, templates [][]byte, n int) []*datapb.SegmentInfo {
	segments := make([]*datapb.SegmentInfo, n)
	for i := range segments {
		seg := &datapb.SegmentInfo{}
		if err := proto.Unmarshal(templates[i%len(templates)], seg); err != nil {
			panic(err)
		}
		seg.ID = 454000001000000000 + int64(i)
		if !c.binlogs {
			seg.ManifestPath = fmt.Sprintf(`{"ver":7,"base_path":"files/insert_log/%d/%d/%d"}`, segMemCollectionID, segMemPartitionID, seg.ID)
		}
		segments[i] = seg
	}
	return segments
}

// segMemCatalog serves decoded segments to LoadFromCatalog.
type segMemCatalog struct {
	metastore.DataCoordCatalog
	list func() []*datapb.SegmentInfo
}

func (c *segMemCatalog) ListSegments(context.Context, int64) ([]*datapb.SegmentInfo, error) {
	return c.list(), nil
}

func (c *segMemCatalog) ListChannelCheckpoint(context.Context) (map[string]*msgpb.MsgPosition, error) {
	return map[string]*msgpb.MsgPosition{}, nil
}

func heapAllocAfterGC() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// benchmarkSegmentMemory installs segMemSegments segments into a fresh store
// per iteration and reports the heap they keep reachable, per segment.
func benchmarkSegmentMemory(b *testing.B, c segMemCase) {
	paramtable.Init()
	schema := segMemSchema(c.nullable)
	templates := segMemTemplates(c, schema)
	channels := make([]string, segMemChannels)
	for i := range channels {
		channels[i] = segMemChannel(i)
	}
	catalog := &segMemCatalog{list: func() []*datapb.SegmentInfo {
		return decodeSegMemSegments(c, templates, segMemSegments)
	}}

	var retained, loadNs float64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s := NewMetaStore(catalog)
		collectionsOf(s).Put(&CollectionInfo{
			ID:            segMemCollectionID,
			Schema:        schema,
			Partitions:    []int64{segMemPartitionID},
			VChannelNames: channels,
		})
		before := heapAllocAfterGC()
		b.StartTimer()

		// Both entry points include decoding the segments.
		start := time.Now()
		if c.fromCatalog {
			if err := s.LoadFromCatalog(context.Background(), []int64{segMemCollectionID}, nil); err != nil {
				b.Fatal(err)
			}
		} else {
			s.LoadSegments(catalog.list())
		}
		loadNs += float64(time.Since(start).Nanoseconds())

		b.StopTimer()
		after := heapAllocAfterGC()
		if got := len(s.GetAllSegments()); got != segMemSegments {
			b.Fatalf("store holds %d segments, want %d", got, segMemSegments)
		}
		runtime.KeepAlive(s)
		retained += float64(int64(after)-int64(before)) / segMemSegments
		b.StartTimer()
	}
	b.StopTimer()
	b.ReportMetric(retained/float64(b.N), "retained-B/seg")
	b.ReportMetric(loadNs/float64(b.N)/segMemSegments, "load-ns/seg")
}

// BenchmarkMetaStoreSegmentMemory measures the heap DataCoord's metaStore
// keeps per flushed segment, for a collection of 20 not-nullable fields plus
// nullable ones, whose segments carry a null count for every field, as the
// DataNode reports them.
func BenchmarkMetaStoreSegmentMemory(b *testing.B) {
	for _, c := range segMemCases {
		b.Run(c.name, func(b *testing.B) {
			benchmarkSegmentMemory(b, c)
		})
	}
}

const (
	gcPutSegments = 20000
	gcPutRounds   = 5
)

// BenchmarkMetaStoreSegmentPutsAcrossGC measures what runtime updates cost
// once the segments are loaded. Each round runs a GC, then re-puts every
// segment the way the coord applies an update: a proto.Clone of the stored
// segment with one scalar changed. It reports the bytes and allocations per
// put of the first round and the mean of the later ones, and the heap kept
// per segment after the last round.
func BenchmarkMetaStoreSegmentPutsAcrossGC(b *testing.B) {
	paramtable.Init()
	c := segMemCase{name: "catalog/v3/2-nullable", fromCatalog: true, nullable: 2}
	schema := segMemSchema(c.nullable)
	templates := segMemTemplates(c, schema)
	channels := make([]string, segMemChannels)
	for i := range channels {
		channels[i] = segMemChannel(i)
	}
	catalog := &segMemCatalog{list: func() []*datapb.SegmentInfo {
		return decodeSegMemSegments(c, templates, gcPutSegments)
	}}

	var firstBytes, firstAllocs, laterBytes, laterAllocs, retained float64
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		s := NewMetaStore(catalog)
		collectionsOf(s).Put(&CollectionInfo{
			ID:            segMemCollectionID,
			Schema:        schema,
			Partitions:    []int64{segMemPartitionID},
			VChannelNames: channels,
		})
		before := heapAllocAfterGC()
		if err := s.LoadFromCatalog(context.Background(), []int64{segMemCollectionID}, nil); err != nil {
			b.Fatal(err)
		}
		for r := 0; r < gcPutRounds; r++ {
			segments := s.GetAllSegments()
			runtime.GC()
			runtime.GC()
			var start, end runtime.MemStats
			runtime.ReadMemStats(&start)
			b.StartTimer()
			for _, seg := range segments {
				updated := proto.Clone(seg).(*datapb.SegmentInfo)
				updated.NumOfRows++
				s.PutSegment(updated)
			}
			b.StopTimer()
			runtime.ReadMemStats(&end)
			bytes := float64(end.TotalAlloc-start.TotalAlloc) / float64(len(segments))
			allocs := float64(end.Mallocs-start.Mallocs) / float64(len(segments))
			if r == 0 {
				firstBytes += bytes
				firstAllocs += allocs
			} else {
				laterBytes += bytes / (gcPutRounds - 1)
				laterAllocs += allocs / (gcPutRounds - 1)
			}
		}
		after := heapAllocAfterGC()
		if got := len(s.GetAllSegments()); got != gcPutSegments {
			b.Fatalf("store holds %d segments, want %d", got, gcPutSegments)
		}
		runtime.KeepAlive(s)
		retained += float64(int64(after)-int64(before)) / gcPutSegments
	}
	n := float64(b.N)
	b.ReportMetric(firstBytes/n, "round1-B/put")
	b.ReportMetric(firstAllocs/n, "round1-allocs/put")
	b.ReportMetric(laterBytes/n, "later-B/put")
	b.ReportMetric(laterAllocs/n, "later-allocs/put")
	b.ReportMetric(retained/n, "retained-B/seg")
}
