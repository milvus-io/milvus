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

package writebuffer

import (
	"context"
	"sync"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/broker"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// Exercise real buffering, SyncTask.Run, BrokerMetaWriter and completion
// callbacks. Only object storage and the coordinator RPC are fault-injected.
func (s *L0WriteBufferSuite) TestTextCheckpointSyncLifecycle() {
	params := paramtable.Get()
	s.Require().NoError(params.Save(params.CommonCfg.UseLoonFFI.Key, "true"))
	s.Require().NoError(params.Save(params.CommonCfg.EnableGrowingSourceFlush.Key, "false"))
	defer params.Reset(params.CommonCfg.UseLoonFFI.Key)
	defer params.Reset(params.CommonCfg.EnableGrowingSourceFlush.Key)

	for _, failure := range []string{"none", "storage", "metadata"} {
		s.Run(failure, func() {
			ctx := context.Background()
			const segmentID = int64(2001)
			schema := s.textSchema()
			mc := s.newTextRealMetaCache(schema)
			coord := broker.NewMockBroker(s.T())
			manager := syncmgr.NewMockSyncManager(s.T())
			wb, err := NewL0WriteBuffer(s.channelName, mc, manager, &writeBufferOption{
				idAllocator: s.allocator,
				metaWriter:  syncmgr.BrokerMetaWriter(coord, 1, retry.Attempts(1)),
			})
			s.Require().NoError(err)
			s.False(wb.AllowGrowingSourceFlush())
			buffer := wb.(*l0WriteBuffer)

			failStorage, failMetadata := failure == "storage", failure == "metadata"
			writeCalls, metaCalls := 0, 0
			write := mockey.Mock((*syncmgr.BulkPackWriterV3).Write).To(func(_ *syncmgr.BulkPackWriterV3, _ context.Context, _ *syncmgr.SyncPack) (
				map[int64]*datapb.FieldBinlog, *datapb.FieldBinlog, map[int64]*datapb.FieldBinlog,
				map[int64]*datapb.FieldBinlog, string, int64, *datapb.Statistics, error,
			) {
				writeCalls++
				if failStorage {
					return nil, nil, nil, nil, "", 0, nil, context.DeadlineExceeded
				}
				return nil, nil, nil, nil, "persisted-manifest", 1, nil, nil
			}).Build()
			defer write.UnPatch()
			coord.EXPECT().SaveBinlogPaths(mock.Anything, mock.Anything).
				RunAndReturn(func(_ context.Context, req *datapb.SaveBinlogPathsRequest) error {
					metaCalls++
					s.False(req.GetFlushed(), "a data sync must leave the segment Growing")
					s.EqualValues(3, req.GetCheckPoints()[0].GetNumOfRows())
					s.EqualValues(200, req.GetCheckPoints()[0].GetPosition().GetTimestamp())
					if failMetadata {
						return context.DeadlineExceeded
					}
					return nil
				})

			// Hold the real task before I/O so an empty write buffer with an
			// in-flight task cannot be mistaken for durable data.
			runTask := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(runTask) }) }
			var submitted syncmgr.Task
			var futures []*conc.Future[struct{}]
			defer func() {
				release()
				_ = conc.AwaitAll(futures...)
			}()
			manager.EXPECT().SyncData(mock.Anything, mock.Anything, mock.Anything).
				RunAndReturn(func(ctx context.Context, task syncmgr.Task, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
					submitted = task
					return conc.Go(func() (struct{}, error) {
						<-runTask
						err := task.Run(ctx)
						for _, callback := range callbacks {
							if callbackErr := callback(err); callbackErr != nil {
								return struct{}{}, callbackErr
							}
						}
						return struct{}{}, err
					}), nil
				})
			position := func(ts uint64) *msgpb.MsgPosition {
				return &msgpb.MsgPosition{ChannelName: s.channelName, MsgID: []byte{1}, Timestamp: ts}
			}
			s.Require().NoError(wb.CreateNewGrowingSegment(CreateGrowingSegmentInfo{
				SegmentID: segmentID, PartitionID: 10, StartPos: position(100),
				SchemaVersion: 100, StorageVersion: storage.StorageV3,
			}))
			_, msg := s.composeTextInsertMsg(segmentID, 3)
			inserts, err := PrepareInsert(schema, s.pkSchema, []*msgstream.InsertMsg{msg})
			s.Require().NoError(err)
			s.Require().NoError(wb.BufferData(inserts, nil, position(100), position(200), 100))
			s.EqualValues(100, wb.GetCheckpoint().GetTimestamp(), "buffered inserts pin the safe point")
			futures = buffer.syncSegments(ctx, []int64{segmentID})
			s.Require().Len(futures, 1)
			s.False(buffer.hasWriteBufferInsertPayload(segmentID))
			s.Require().NoError(wb.BufferData(nil, nil, position(200), position(500), 100))
			s.EqualValues(100, wb.GetCheckpoint().GetTimestamp(), "in-flight sync still pins it")
			release()
			err = conc.AwaitAll(futures...)
			if failure != "none" {
				s.ErrorIs(err, context.DeadlineExceeded)
				s.EqualValues(100, wb.GetCheckpoint().GetTimestamp(), "failed storage or metadata commit cannot release the checkpoint")
				seg, ok := mc.GetSegmentByID(segmentID)
				s.Require().True(ok)
				s.Zero(seg.FlushedRows())
				if failure == "storage" {
					s.Zero(metaCalls, "failed storage must not publish segment metadata")
				}
				// Re-submit the retained task after the injected outage ends.
				failStorage, failMetadata = false, false
				futures = buffer.submitSyncTasks(ctx, []syncmgr.Task{submitted})
				s.Require().NoError(conc.AwaitAll(futures...))
			} else {
				s.Require().NoError(err)
			}
			s.Positive(writeCalls)
			s.Positive(metaCalls)
			seg, ok := mc.GetSegmentByID(segmentID)
			s.Require().True(ok)
			s.Equal(commonpb.SegmentState_Growing, seg.State())
			s.EqualValues(3, seg.FlushedRows())
			s.Zero(seg.SyncingRows())
			s.EqualValues(500, wb.GetCheckpoint().GetTimestamp())
			s.Require().NoError(wb.BufferData(nil, nil, position(500), position(600), 100))
			s.EqualValues(600, wb.GetCheckpoint().GetTimestamp(), "idle TimeTicks advance without another flush")

			_, next := s.composeTextInsertMsg(segmentID, 1)
			nextInserts, err := PrepareInsert(schema, s.pkSchema, []*msgstream.InsertMsg{next})
			s.Require().NoError(err)
			s.Require().NoError(wb.BufferData(nextInserts, nil, position(600), position(700), 100))
			s.Require().NoError(wb.BufferData(nil, nil, position(700), position(800), 100))
			s.EqualValues(600, wb.GetCheckpoint().GetTimestamp(), "new unflushed data restores the safe bound")
		})
	}
}
