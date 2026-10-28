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

package recovery

import (
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// broadcastAckExtraLocked returns the result that the ack of msg carries, nil
// for most messages. Only a primary key index CreateIndex on a data vchannel
// carries one: the outcome of the index on the vchannel. It runs right after
// the modules observed msg, under r.mu, so the vchannel meta holds the
// decision at msg.
//
// A replayed CreateIndex can get a different outcome than its first ack when
// the vchannel meta was persisted ahead of the global checkpoint and a later
// DropIndex cleared pk_index. The coordinator ignores it: it keeps the first
// ack of each vchannel, and the DropIndex exists only after the callback of
// the CreateIndex, so after every vchannel acked.
func (r *recoveryStorageImpl) broadcastAckExtraLocked(msg message.ImmutableMessage) *anypb.Any {
	if msg.MessageType() != message.MessageTypeCreateIndex || funcutil.IsControlChannel(msg.VChannel()) {
		return nil
	}
	header := message.MustAsImmutableCreateIndexMessageV2(msg).Header()
	if header.GetIndexType() != common.PrimaryKeyIndexType {
		return nil
	}
	outcome := pkindexpb.CreateIndexAckResult_NOT_EMPTY
	if !paramtable.Get().StreamingCfg.PKIndexEnabled.GetAsBool() {
		outcome = pkindexpb.CreateIndexAckResult_DISABLED
	} else if state, ok := r.vchannelManager.PKIndexState(msg.VChannel()); ok &&
		state.PKIndex.GetIndexId() == header.GetIndexId() &&
		state.PKIndex.GetEnableTimeTick() == msg.TimeTick() {
		outcome = pkindexpb.CreateIndexAckResult_ENABLED
	}
	extra, err := anypb.New(&pkindexpb.CreateIndexAckResult{Outcome: outcome})
	if err != nil {
		panic(err) // the marshal of a fixed message can not fail.
	}
	return extra
}
