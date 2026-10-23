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

package segment

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestUpperLimitCalBySchema(t *testing.T) {
	type testCase struct {
		schema    *schemapb.CollectionSchema
		expected  int
		expectErr bool
	}
	testCases := []testCase{
		{
			schema:    nil,
			expected:  -1,
			expectErr: true,
		},
		{
			schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{},
			},
			expected:  -1,
			expectErr: true,
		},
		{
			schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						DataType: schemapb.DataType_FloatVector,
						TypeParams: []*commonpb.KeyValuePair{
							{Key: common.DimKey, Value: "bad_dim"},
						},
					},
				},
			},
			expected:  -1,
			expectErr: true,
		},
		{
			schema: &schemapb.CollectionSchema{
				Fields: []*schemapb.FieldSchema{
					{
						DataType: schemapb.DataType_Int64,
					},
					{
						DataType: schemapb.DataType_Int32,
					},
					{
						DataType: schemapb.DataType_FloatVector,
						TypeParams: []*commonpb.KeyValuePair{
							{Key: common.DimKey, Value: "128"},
						},
					},
				},
			},
			expected:  int(Params.DataCoordCfg.SegmentMaxSize.GetAsFloat() * 1024 * 1024 / float64(524)),
			expectErr: false,
		},
	}
	for _, c := range testCases {
		result, err := calBySchemaPolicy(c.schema)
		if c.expectErr {
			assert.Error(t, err)
		} else {
			assert.Equal(t, c.expected, result)
		}
	}
}

func TestGetChannelOpenSegCapacityPolicy(t *testing.T) {
	p := getChannelOpenSegCapacityPolicy(3)
	type testCase struct {
		channel   string
		segments  []*meta.SegmentInfo
		ts        Timestamp
		validator func([]*meta.SegmentInfo) bool
	}
	testCases := []testCase{
		{
			segments: []*meta.SegmentInfo{},
			ts:       tsoutil.ComposeTS(time.Now().Unix()/int64(time.Millisecond), rand.Int63n(1000)),
			validator: func(result []*meta.SegmentInfo) bool {
				return len(result) == 0
			},
		},
		{
			segments: []*meta.SegmentInfo{
				{
					SegmentInfo: &datapb.SegmentInfo{},
				},
				{
					SegmentInfo: &datapb.SegmentInfo{},
				},
				{
					SegmentInfo: &datapb.SegmentInfo{},
				},
				{
					SegmentInfo: &datapb.SegmentInfo{},
				},
			},
			ts: tsoutil.ComposeTS(time.Now().Unix()/int64(time.Millisecond), rand.Int63n(1000)),
			validator: func(result []*meta.SegmentInfo) bool {
				return len(result) == 1
			},
		},
	}
	for _, c := range testCases {
		result, _ := p(c.channel, c.segments, c.ts)
		if c.validator != nil {
			assert.True(t, c.validator(result))
		}
	}
}

func TestSortSegmentsByLastExpires(t *testing.T) {
	segs := make([]*meta.SegmentInfo, 0, 10)
	for i := 0; i < 10; i++ {
		segs = append(segs,
			&meta.SegmentInfo{
				SegmentInfo: &datapb.SegmentInfo{
					LastExpireTime: uint64(rand.Int31n(100000)),
				},
			})
	}
	sortSegmentsByLastExpires(segs)
	for i := 1; i < 10; i++ {
		assert.True(t, segs[i-1].LastExpireTime <= segs[i].LastExpireTime)
	}
}

func TestSealSegmentPolicy(t *testing.T) {
	paramtable.Init()
	t.Run("test seal segment by lifetime", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().DataCoordCfg.SegmentMaxLifetime.Key, "2")
		defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.SegmentMaxLifetime.Key)

		lifetime := 2 * time.Second
		now := time.Now()
		curTS := now.UnixNano() / int64(time.Millisecond)
		nosealTs := (now.Add(lifetime / 2)).UnixNano() / int64(time.Millisecond)
		sealTs := (now.Add(lifetime)).UnixNano() / int64(time.Millisecond)

		p := sealL1SegmentByLifetime()

		segment := &meta.SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:            1,
				StartPosition: &msgpb.MsgPosition{Timestamp: tsoutil.ComposeTS(curTS, 0)},
			},
		}

		shouldSeal, _ := p.ShouldSeal(segment, tsoutil.ComposeTS(nosealTs, 0))
		assert.False(t, shouldSeal)

		shouldSeal, _ = p.ShouldSeal(segment, tsoutil.ComposeTS(sealTs, 0))
		assert.True(t, shouldSeal)
	})
}

func Test_sealLongTimeIdlePolicy(t *testing.T) {
	idleTimeTolerance := 2 * time.Second
	minSizeToSealIdleSegment := 16.0
	maxSizeOfSegment := 512.0
	policy := sealL1SegmentByIdleTime(idleTimeTolerance, minSizeToSealIdleSegment, maxSizeOfSegment)
	seg1 := &meta.SegmentInfo{LastWrittenTime: time.Now().Add(idleTimeTolerance * 5)}
	shouldSeal, _ := policy.ShouldSeal(seg1, 100)
	assert.False(t, shouldSeal)
	seg2 := &meta.SegmentInfo{LastWrittenTime: meta.GetZeroTime(), SegmentInfo: &datapb.SegmentInfo{MaxRowNum: 10000, NumOfRows: 1}}
	shouldSeal, _ = policy.ShouldSeal(seg2, 100)
	assert.False(t, shouldSeal)
	seg3 := &meta.SegmentInfo{LastWrittenTime: meta.GetZeroTime(), SegmentInfo: &datapb.SegmentInfo{MaxRowNum: 10000, NumOfRows: 1000}}
	shouldSeal, _ = policy.ShouldSeal(seg3, 100)
	assert.True(t, shouldSeal)
}

func Test_sealByTotalGrowingSegmentsSize(t *testing.T) {
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.GrowingSegmentsMemSizeInMB.Key, "100")
	defer paramtable.Get().Reset(paramtable.Get().DataCoordCfg.GrowingSegmentsMemSizeInMB.Key)

	seg0 := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:      0,
		State:   commonpb.SegmentState_Growing,
		Binlogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{MemorySize: 30 * meta.MB}}}},
	}}
	seg1 := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:      1,
		State:   commonpb.SegmentState_Growing,
		Binlogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{MemorySize: 40 * meta.MB}}}},
	}}
	seg2 := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:      2,
		State:   commonpb.SegmentState_Growing,
		Binlogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{MemorySize: 50 * meta.MB}}}},
	}}
	seg3 := &meta.SegmentInfo{SegmentInfo: &datapb.SegmentInfo{
		ID:    3,
		State: commonpb.SegmentState_Sealed,
	}}

	fn := sealByTotalGrowingSegmentsSize()
	// size not reach threshold
	res, _ := fn("ch-0", []*meta.SegmentInfo{seg0}, 0)
	assert.Equal(t, 0, len(res))
	// size reached the threshold
	res, _ = fn("ch-0", []*meta.SegmentInfo{seg0, seg1, seg2, seg3}, 0)
	assert.Equal(t, 1, len(res))
	assert.Equal(t, seg2.GetID(), res[0].GetID())
}

func Test_sealByBlockingL0(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	type testCase struct {
		tag             string
		channel         string
		sizeLimit       int64
		entryNumLimit   int64
		l0Segments      []*meta.SegmentInfo
		growingSegments []*meta.SegmentInfo
		expected        []int64
	}

	l0_1 := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1001,
			InsertChannel: "channel_1",
			Deltalogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{EntriesNum: 50, MemorySize: 1 * 1024 * 1024},
					},
				},
			},
			Level:         datapb.SegmentLevel_L0,
			StartPosition: &msgpb.MsgPosition{Timestamp: 10},
			DmlPosition:   &msgpb.MsgPosition{Timestamp: 20},
		},
	}
	l0_2 := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            1002,
			InsertChannel: "channel_1",
			Deltalogs: []*datapb.FieldBinlog{
				{
					Binlogs: []*datapb.Binlog{
						{EntriesNum: 60, MemorySize: 2 * 1024 * 1024},
					},
				},
			},
			Level:         datapb.SegmentLevel_L0,
			StartPosition: &msgpb.MsgPosition{Timestamp: 30},
			DmlPosition:   &msgpb.MsgPosition{Timestamp: 40},
		},
	}
	growing1 := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            2001,
			InsertChannel: "channel_1",
			StartPosition: &msgpb.MsgPosition{Timestamp: 10},
		},
	}
	growing2 := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            2002,
			InsertChannel: "channel_1",
			StartPosition: &msgpb.MsgPosition{Timestamp: 35},
		},
	}
	growing3 := &meta.SegmentInfo{
		SegmentInfo: &datapb.SegmentInfo{
			ID:            2003,
			InsertChannel: "channel_1",
		},
	}

	testCases := []*testCase{
		{
			tag:             "seal_by_entrynum",
			channel:         "channel_1",
			sizeLimit:       -1,
			entryNumLimit:   100,
			l0Segments:      []*meta.SegmentInfo{l0_1, l0_2},         // ts: [10,20] [30, 40], entryNum: 50, 60
			growingSegments: []*meta.SegmentInfo{growing1, growing2}, // ts: [10, 35]
			expected:        []int64{2001},
		},
		{
			tag:             "seal_by_size",
			channel:         "channel_1",
			sizeLimit:       1, // 1MB
			entryNumLimit:   -1,
			l0Segments:      []*meta.SegmentInfo{l0_1, l0_2},         // ts: [10,20] [30, 40], entryNum: 1MB, 2MB
			growingSegments: []*meta.SegmentInfo{growing1, growing2}, // ts: [10, 35]
			expected:        []int64{2001, 2002},
		},
		{
			tag:             "empty_input",
			channel:         "channel_1",
			growingSegments: []*meta.SegmentInfo{growing1, growing2},
			sizeLimit:       1,
			entryNumLimit:   50,
			expected:        []int64{},
		},
		{
			tag:             "all_disabled",
			channel:         "channel_1",
			l0Segments:      []*meta.SegmentInfo{l0_1, l0_2},
			growingSegments: []*meta.SegmentInfo{growing1, growing2},
			sizeLimit:       -1,
			entryNumLimit:   -1,
			expected:        []int64{},
		},
		{
			tag:             "growing_segment_with_nil_start_position",
			channel:         "channel_1",
			l0Segments:      []*meta.SegmentInfo{l0_1, l0_2},
			growingSegments: []*meta.SegmentInfo{growing3},
			expected:        []int64{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.tag, func(t *testing.T) {
			pt.Save(pt.DataCoordCfg.BlockingL0SizeInMB.Key, strconv.FormatInt(tc.sizeLimit, 10))
			defer pt.Reset(pt.DataCoordCfg.BlockingL0SizeInMB.Key)
			pt.Save(pt.DataCoordCfg.BlockingL0EntryNum.Key, strconv.FormatInt(tc.entryNumLimit, 10))
			defer pt.Reset(pt.DataCoordCfg.BlockingL0EntryNum.Key)

			segments := meta.NewSegmentsInfo(metacache.NewMetaStore(nil))
			for _, l0segment := range tc.l0Segments {
				segments.SetSegment(l0segment.GetID(), l0segment)
			}

			mt := &meta.Meta{
				Segments: segments,
			}

			result, _ := sealByBlockingL0(mt)(tc.channel, tc.growingSegments, 0)
			sealedIDs := lo.Map(result, func(segment *meta.SegmentInfo, _ int) int64 {
				return segment.GetID()
			})
			assert.ElementsMatch(t, tc.expected, sealedIDs)
		})
	}
}
