// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (c *Core) broadcastTruncateCollection(ctx context.Context, req *milvuspb.TruncateCollectionRequest) error {
	broadcaster, err := c.startBroadcastWithCollectionLock(ctx, req.GetDbName(), req.GetCollectionName())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	// get collection info
	coll, err := c.meta.GetCollectionByName(ctx, req.GetDbName(), req.GetCollectionName(), typeutil.MaxTimestamp)
	if err != nil {
		return err
	}

	header := &messagespb.TruncateCollectionMessageHeader{
		DbId:         coll.DBID,
		CollectionId: coll.CollectionID,
	}
	body := &messagespb.TruncateCollectionMessageBody{}

	channels := make([]string, 0, len(coll.VirtualChannelNames)+1)
	channels = append(channels, streaming.WAL().ControlChannel())
	channels = append(channels, coll.VirtualChannelNames...)
	msg := message.NewTruncateCollectionMessageBuilderV2().
		WithHeader(header).
		WithBody(body).
		WithBroadcast(channels).
		MustBuildBroadcast()
	if _, err := broadcaster.Broadcast(ctx, msg); err != nil {
		return err
	}
	return nil
}

// truncateCollectionV2AckCallback is called when the truncate collection message is acknowledged
func (c *DDLCallback) truncateCollectionV2AckCallback(ctx context.Context, result message.BroadcastResultTruncateCollectionMessageV2) error {
	msg := result.Message
	header := msg.Header()

	flushTsList := make(map[string]uint64)
	for vchannel, result := range result.Results {
		if funcutil.IsControlChannel(vchannel) {
			continue
		}
		flushTs := result.TimeTick
		flushTsList[vchannel] = flushTs
	}

	// Drop segments that were updated before the flush timestamp
	err := c.mixCoord.DropSegmentsByTime(ctx, header.CollectionId, flushTsList)
	if err != nil {
		return err
	}

	// manually update current target to sync QueryCoord's view
	err = c.mixCoord.ManualUpdateCurrentTarget(ctx, header.CollectionId)
	if err != nil {
		return err
	}

	return nil
}
