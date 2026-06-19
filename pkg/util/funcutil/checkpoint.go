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

package funcutil

import (
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
)

// DroppedChannelCheckpointTimestamp is the reserved timestamp value used to
// mark a channel checkpoint as dropped. No legitimate channel checkpoint may
// carry this value.
//
// The sole writer is datacoord.(*meta).MarkChannelCheckpointDropped, invoked
// when a vchannel is dropped. The sentinel is sticky:
// datacoord.(*meta).UpdateChannelCheckpoint refuses to overwrite it because
// oldTs < newTs fails when oldTs == DroppedChannelCheckpointTimestamp. A
// sentinel therefore remains visible to readers (e.g. GetRecoveryInfo) until
// the collection meta is fully dropped, at which point the channel ceases to
// be returned at all.
const DroppedChannelCheckpointTimestamp uint64 = math.MaxUint64

// IsDroppedChannelCheckpoint reports whether the given checkpoint position is
// the dropped-channel sentinel.
//
// Callers that consume channel seek positions (notably QueryCoord's target
// builder) MUST treat a sentinel as an abnormal metadata state — not a normal
// "start from this position" instruction — and refuse to use it as a seek
// point.
func IsDroppedChannelCheckpoint(pos *msgpb.MsgPosition) bool {
	return pos != nil && pos.GetTimestamp() == DroppedChannelCheckpointTimestamp
}
