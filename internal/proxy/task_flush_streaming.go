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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (t *flushTask) Execute(ctx context.Context) error {
	coll2Segments := make(map[string]*schemapb.LongArray)
	flushColl2Segments := make(map[string]*schemapb.LongArray)
	coll2SealTimes := make(map[string]int64)
	coll2FlushTs := make(map[string]Timestamp)
	channelCps := make(map[string]*msgpb.MsgPosition)

	mlog.Info(ctx, "flushTaskByStreamingService.Execute", mlog.Int("collectionNum", len(t.CollectionNames)))
	for _, collName := range t.CollectionNames {
		collID, err := t.GetMetaCache().GetCollectionID(t.ctx, t.DbName, collName)
		if err != nil {
			return err
		}

		// DataCoord broadcasts ManualFlush to all collection VChannels and waits
		// for consuming-side L1 and L0 completion before returning.
		flushReq := &datapb.FlushRequest{
			Base: commonpbutil.UpdateMsgBase(
				t.Base,
				commonpbutil.WithMsgType(commonpb.MsgType_Flush),
			),
			CollectionID: collID,
		}
		resp, err := t.mixCoord.Flush(ctx, flushReq)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return merr.Wrap(err, "failed to call flush to data coordinator")
		}

		coll2Segments[collName] = &schemapb.LongArray{Data: resp.GetSegmentIDs()}
		flushColl2Segments[collName] = &schemapb.LongArray{Data: resp.GetFlushSegmentIDs()}
		coll2SealTimes[collName] = resp.GetTimeOfSeal()
		coll2FlushTs[collName] = resp.GetFlushTs()
		channelCps = resp.GetChannelCps()
	}
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
