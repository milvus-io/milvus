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

package datacoord

import (
	"math/rand"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/stretchr/testify/assert"
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
							{Key: "dim", Value: "bad_dim"},
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
							{Key: "dim", Value: "128"},
						},
					},
				},
			},
			expected:  int(Params.DataCoordCfg.SegmentMaxSize * 1024 * 1024 / float64(524)),
			expectErr: false,
		},
	}
	for _, c := range testCases {
		result, err := calBySchemaPolicy(Params.DataCoordCfg.SegmentMaxSize, c.schema)
		if c.expectErr {
			assert.NotNil(t, err)
		} else {
			assert.Equal(t, c.expected, result)
		}
	}
}

func TestGetChannelOpenSegCapacityPolicy(t *testing.T) {
	p := getChannelOpenSegCapacityPolicy(3)
	type testCase struct {
		channel   string
		segments  []*SegmentInfo
		ts        Timestamp
		validator func([]*SegmentInfo) bool
	}
	testCases := []testCase{
		{
			segments: []*SegmentInfo{},
			ts:       tsoutil.ComposeTS(time.Now().Unix()/int64(time.Millisecond), rand.Int63n(1000)),
			validator: func(result []*SegmentInfo) bool {
				return len(result) == 0
			},
		},
		{
			segments: []*SegmentInfo{
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
			validator: func(result []*SegmentInfo) bool {
				return len(result) == 1
			},
		},
	}
	for _, c := range testCases {
		result := p(c.channel, c.segments, c.ts)
		if c.validator != nil {
			assert.True(t, c.validator(result))
		}
	}
}

func TestSortSegmentsByLastExpires(t *testing.T) {
	segs := make([]*SegmentInfo, 0, 10)
	for i := 0; i < 10; i++ {
		segs = append(segs,
			&SegmentInfo{
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
	t.Run("test seal segment by lifetime", func(t *testing.T) {
		lifetime := 2 * time.Second
		now := time.Now()
		curTS := now.UnixNano() / int64(time.Millisecond)
		nosealTs := (now.Add(lifetime / 2)).UnixNano() / int64(time.Millisecond)
		sealTs := (now.Add(lifetime)).UnixNano() / int64(time.Millisecond)

		p := sealByLifetimePolicy(lifetime)

		segment := &SegmentInfo{
			SegmentInfo: &datapb.SegmentInfo{
				ID:             1,
				LastExpireTime: tsoutil.ComposeTS(curTS, 0),
			},
		}

		shouldSeal := p(segment, tsoutil.ComposeTS(nosealTs, 0))
		assert.False(t, shouldSeal)

		shouldSeal = p(segment, tsoutil.ComposeTS(sealTs, 0))
		assert.True(t, shouldSeal)
	})
}
