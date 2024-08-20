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

package proxy

import (
	"context"
	"fmt"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type flushTaskByStreamingService struct {
	*flushTask
	chMgr channelsMgr
}

func (t *flushTaskByStreamingService) Execute(ctx context.Context) error {
	coll2Segments := make(map[string]*schemapb.LongArray)
	flushColl2Segments := make(map[string]*schemapb.LongArray)
	coll2SealTimes := make(map[string]int64)
	coll2FlushTs := make(map[string]Timestamp)
	channelCps := make(map[string]*msgpb.MsgPosition)

	flushTs := t.BeginTs()
	log.Info("flushTaskByStreamingService.Execute", zap.Int("collectionNum", len(t.CollectionNames)), zap.Uint64("flushTs", flushTs))
	timeOfSeal, _ := tsoutil.ParseTS(flushTs)
	for _, collName := range t.CollectionNames {
		collID, err := globalMetaCache.GetCollectionID(t.ctx, t.DbName, collName)
		if err != nil {
			return merr.WrapErrAsInputErrorWhen(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
		}
		vchannels, err := t.chMgr.getVChannels(collID)
		if err != nil {
			return err
		}
		onFlushSegmentIDs := make([]int64, 0)

		// Ask the streamingnode to flush segments.
		for _, vchannel := range vchannels {
			segmentIDs, err := t.sendManualFlushToWAL(ctx, collID, vchannel, flushTs)
			if err != nil {
				return err
			}
			onFlushSegmentIDs = append(onFlushSegmentIDs, segmentIDs...)
		}

		// Ask datacoord to get flushed segment infos.
		flushReq := &datapb.FlushRequest{
			Base: commonpbutil.UpdateMsgBase(
				t.Base,
				commonpbutil.WithMsgType(commonpb.MsgType_Flush),
			),
			CollectionID: collID,
		}
		resp, err := t.dataCoord.Flush(ctx, flushReq)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return fmt.Errorf("failed to call flush to data coordinator: %s", err.Error())
		}

		// Remove the flushed segments from onFlushSegmentIDs
		for _, segID := range resp.GetFlushSegmentIDs() {
			for i, id := range onFlushSegmentIDs {
				if id == segID {
					onFlushSegmentIDs = append(onFlushSegmentIDs[:i], onFlushSegmentIDs[i+1:]...)
					break
				}
			}
		}

		coll2Segments[collName] = &schemapb.LongArray{Data: onFlushSegmentIDs}
		flushColl2Segments[collName] = &schemapb.LongArray{Data: resp.GetFlushSegmentIDs()}
		coll2SealTimes[collName] = timeOfSeal.Unix()
		coll2FlushTs[collName] = flushTs
		channelCps = resp.GetChannelCps()
	}
	// TODO: refactor to use streaming service
	SendReplicateMessagePack(ctx, t.replicateMsgStream, t.FlushRequest)
	t.result = &milvuspb.FlushResponse{
		Status:          merr.Success(),
		DbName:          t.GetDbName(),
		CollSegIDs:      coll2Segments,
		FlushCollSegIDs: flushColl2Segments,
		CollSealTimes:   coll2SealTimes,
		CollFlushTs:     coll2FlushTs,
		ChannelCps:      channelCps,
	}
	return nil
}

// sendManualFlushToWAL sends a manual flush message to WAL.
func (t *flushTaskByStreamingService) sendManualFlushToWAL(ctx context.Context, collID int64, vchannel string, flushTs uint64) ([]int64, error) {
	logger := log.With(zap.Int64("collectionID", collID), zap.String("vchannel", vchannel))
	flushMsg, err := message.NewManualFlushMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.ManualFlushMessageHeader{
			CollectionId: collID,
			FlushTs:      flushTs,
		}).
		WithBody(&message.ManualFlushMessageBody{}).
		BuildMutable()
	if err != nil {
		logger.Warn("build manual flush message failed", zap.Error(err))
		return nil, err
	}

	appendResult, err := streaming.WAL().RawAppend(ctx, flushMsg, streaming.AppendOption{
		BarrierTimeTick: flushTs,
	})
	if err != nil {
		logger.Warn("append manual flush message to wal failed", zap.Error(err))
		return nil, err
	}

	var flushMsgResponse message.ManualFlushExtraResponse
	if err := appendResult.GetExtra(&flushMsgResponse); err != nil {
		logger.Warn("get extra from append result failed", zap.Error(err))
		return nil, err
	}
	logger.Info("append manual flush message to wal successfully")

	return flushMsgResponse.GetSegmentIds(), nil
}
