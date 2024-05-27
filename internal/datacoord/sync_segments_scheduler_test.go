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
	"errors"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/lock"
	"github.com/stretchr/testify/suite"
)

type SyncSegmentsSchedulerSuite struct {
	suite.Suite

	m   *meta
	new int
	old int
}

func Test_SyncSegmentsSchedulerSuite(t *testing.T) {
	suite.Run(t, new(SyncSegmentsSchedulerSuite))
}

func (s *SyncSegmentsSchedulerSuite) initParams() {
	s.m = &meta{
		RWMutex: lock.RWMutex{},
		collections: map[UniqueID]*collectionInfo{
			1: {
				ID: 1,
				Schema: &schemapb.CollectionSchema{
					Name: "coll1",
					Fields: []*schemapb.FieldSchema{
						{
							FieldID:      100,
							Name:         "pk",
							IsPrimaryKey: true,
							Description:  "",
							DataType:     schemapb.DataType_Int64,
						},
						{
							FieldID:      101,
							Name:         "vec",
							IsPrimaryKey: false,
							Description:  "",
							DataType:     schemapb.DataType_FloatVector,
						},
					},
				},
				Partitions:    []int64{2, 3},
				VChannelNames: []string{"channel1", "channel2"},
			},
		},
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{
				5: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            5,
						CollectionID:  1,
						PartitionID:   2,
						InsertChannel: "channel1",
						NumOfRows:     3000,
						State:         commonpb.SegmentState_Dropped,
						Statslogs: []*datapb.FieldBinlog{
							{
								FieldID: 100,
								Binlogs: []*datapb.Binlog{
									{
										LogID: 1,
									},
								},
							},
							{
								FieldID: 101,
								Binlogs: []*datapb.Binlog{
									{
										LogID: 2,
									},
								},
							},
						},
					},
				},
				6: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            6,
						CollectionID:  1,
						PartitionID:   3,
						InsertChannel: "channel1",
						NumOfRows:     3000,
						State:         commonpb.SegmentState_Dropped,
						Statslogs: []*datapb.FieldBinlog{
							{
								FieldID: 100,
								Binlogs: []*datapb.Binlog{
									{
										LogID: 3,
									},
								},
							},
							{
								FieldID: 101,
								Binlogs: []*datapb.Binlog{
									{
										LogID: 4,
									},
								},
							},
						},
					},
				},
				7: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            7,
						CollectionID:  1,
						PartitionID:   2,
						InsertChannel: "channel2",
						NumOfRows:     3000,
						State:         commonpb.SegmentState_Dropped,
						Statslogs: []*datapb.FieldBinlog{
							{
								FieldID: 100,
								Binlogs: []*datapb.Binlog{
									{
										LogID: 5,
									},
								},
							},
							{
								FieldID: 101,
								Binlogs: []*datapb.Binlog{
									{
										LogID: 6,
									},
								},
							},
						},
					},
				},
				8: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            8,
						CollectionID:  1,
						PartitionID:   3,
						InsertChannel: "channel2",
						NumOfRows:     3000,
						State:         commonpb.SegmentState_Dropped,
						Statslogs: []*datapb.FieldBinlog{
							{
								FieldID: 100,
								Binlogs: []*datapb.Binlog{
									{
										LogID: 7,
									},
								},
							},
							{
								FieldID: 101,
								Binlogs: []*datapb.Binlog{
									{
										LogID: 8,
									},
								},
							},
						},
					},
				},
				9: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            9,
						CollectionID:  1,
						PartitionID:   2,
						InsertChannel: "channel1",
						NumOfRows:     3000,
						State:         commonpb.SegmentState_Flushed,
						Statslogs: []*datapb.FieldBinlog{
							{
								FieldID: 100,
								Binlogs: []*datapb.Binlog{
									{
										LogID: 9,
									},
								},
							},
							{
								FieldID: 101,
								Binlogs: []*datapb.Binlog{
									{
										LogID: 10,
									},
								},
							},
						},
						CompactionFrom: []int64{5},
					},
				},
				10: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            10,
						CollectionID:  1,
						PartitionID:   3,
						InsertChannel: "channel1",
						NumOfRows:     3000,
						State:         commonpb.SegmentState_Flushed,
						Statslogs: []*datapb.FieldBinlog{
							{
								FieldID: 100,
								Binlogs: []*datapb.Binlog{
									{
										LogID: 7,
									},
								},
							},
							{
								FieldID: 101,
								Binlogs: []*datapb.Binlog{
									{
										LogID: 8,
									},
								},
							},
						},
						CompactionFrom: []int64{6},
					},
				},
				11: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            11,
						CollectionID:  1,
						PartitionID:   2,
						InsertChannel: "channel2",
						NumOfRows:     3000,
						State:         commonpb.SegmentState_Flushed,
						Statslogs: []*datapb.FieldBinlog{
							{
								FieldID: 100,
								Binlogs: []*datapb.Binlog{
									{
										LogID: 5,
									},
								},
							},
							{
								FieldID: 101,
								Binlogs: []*datapb.Binlog{
									{
										LogID: 6,
									},
								},
							},
						},
						CompactionFrom: []int64{7},
					},
				},
				12: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:            12,
						CollectionID:  1,
						PartitionID:   3,
						InsertChannel: "channel2",
						NumOfRows:     3000,
						State:         commonpb.SegmentState_Flushed,
						Statslogs: []*datapb.FieldBinlog{
							{
								FieldID: 100,
								Binlogs: []*datapb.Binlog{
									{
										LogID: 7,
									},
								},
							},
							{
								FieldID: 101,
								Binlogs: []*datapb.Binlog{
									{
										LogID: 8,
									},
								},
							},
						},
						CompactionFrom: []int64{8},
					},
				},
			},
			compactionTo: map[UniqueID]UniqueID{
				5: 9,
				6: 10,
				7: 11,
				8: 12,
			},
		},
	}
}

func (s *SyncSegmentsSchedulerSuite) SetupTest() {
	s.initParams()
}

func (s *SyncSegmentsSchedulerSuite) Test_newSyncSegmentsScheduler() {
	cm := NewMockChannelManager(s.T())
	cm.EXPECT().FindWatcher(mock.Anything).Return(100, nil)

	sm := NewMockSessionManager(s.T())
	sm.EXPECT().SyncSegments(mock.Anything, mock.Anything).RunAndReturn(func(i int64, request *datapb.SyncSegmentsRequest) error {
		for _, seg := range request.GetSegmentInfos() {
			if seg.GetState() == commonpb.SegmentState_Flushed {
				s.new++
			}
			if seg.GetState() == commonpb.SegmentState_Dropped {
				s.old++
			}
		}
		return nil
	})

	Params.DataCoordCfg.SyncSegmentsInterval.SwapTempValue("1")
	defer Params.DataCoordCfg.SyncSegmentsInterval.SwapTempValue("600")
	sss := newSyncSegmentsScheduler(s.m, cm, sm)
	sss.Start()

	// 2 channels, 2 partitions, 2 segments
	for s.new != 4 || s.old != 4 {
	}
	sss.Stop()
}

func (s *SyncSegmentsSchedulerSuite) Test_SyncSegmentsFail() {
	cm := NewMockChannelManager(s.T())
	sm := NewMockSessionManager(s.T())

	sss := newSyncSegmentsScheduler(s.m, cm, sm)

	s.Run("pk not found", func() {
		sss.meta.collections[1].Schema.Fields[0].IsPrimaryKey = false
		sss.SyncSegmentsForCollections()
		sss.meta.collections[1].Schema.Fields[0].IsPrimaryKey = true
	})

	s.Run("find watcher failed", func() {
		cm.EXPECT().FindWatcher(mock.Anything).Return(0, errors.New("mock error")).Twice()
		sss.SyncSegmentsForCollections()
	})

	s.Run("sync segment failed", func() {
		cm.EXPECT().FindWatcher(mock.Anything).Return(100, nil)
		sm.EXPECT().SyncSegments(mock.Anything, mock.Anything).Return(errors.New("mock error"))
		sss.SyncSegmentsForCollections()
	})
}
