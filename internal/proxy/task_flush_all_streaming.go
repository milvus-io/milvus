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

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func (t *flushAllTask) Execute(ctx context.Context) error {
	resp, err := t.mixCoord.FlushAll(ctx, &datapb.FlushAllRequest{
		Base: commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_Flush)),
	})
	if err = merr.CheckRPCCall(resp, err); err != nil {
		return merr.WrapErrServiceInternalErr(err, "failed to call flush all to data coordinator")
	}
	t.result = &milvuspb.FlushAllResponse{
		Status:       merr.Success(),
		FlushAllMsgs: resp.GetFlushAllMsgs(),
		ClusterInfo:  resp.GetClusterInfo(),
	}

	// Assign the flush all ts to the result for compatibility.
	// Use the max time tick of the flush all messages as the flush all ts.
	t.result.FlushAllTs = lo.MaxBy(message.MilvusMessagesToImmutableMessages(lo.Values(resp.GetFlushAllMsgs())), func(a, b message.ImmutableMessage) bool {
		return a.TimeTick() > b.TimeTick()
	}).TimeTick()

	return nil
}
