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

package messageutil

import (
	"slices"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

// SplitShardRole is what one copy of a SplitShard broadcast means to the
// vchannel that receives it.
type SplitShardRole int

const (
	// SplitShardRoleNone: the vchannel is not named by the message.
	SplitShardRoleNone SplitShardRole = iota
	// SplitShardRoleSource: the copy fences the DML of the vchannel. Its time
	// tick is T_switch.
	SplitShardRoleSource
	// SplitShardRoleTarget: the copy is the genesis of the vchannel.
	SplitShardRoleTarget
	// SplitShardRoleControl: the copy on the control channel. It has no effect
	// on a WAL; it orders the ack callback.
	SplitShardRoleControl
)

// SplitShardRoleOf returns the role of the copy of a SplitShard message that
// is addressed to vchannel.
func SplitShardRoleOf(header *message.SplitShardMessageHeader, vchannel string) SplitShardRole {
	switch {
	case vchannel == "":
		return SplitShardRoleNone
	case funcutil.IsControlChannel(vchannel):
		return SplitShardRoleControl
	case header.GetSourceVchannel() == vchannel:
		return SplitShardRoleSource
	case slices.Contains(header.GetTargetVchannels(), vchannel):
		return SplitShardRoleTarget
	default:
		return SplitShardRoleNone
	}
}

// splitShardRoleOfMessage returns the role of a message, or SplitShardRoleNone
// if the message is not a SplitShard message. The message must be addressed
// to a vchannel: a broadcast message that is not yet split into its copies
// has no role.
func splitShardRoleOfMessage(msg message.BasicMessage) SplitShardRole {
	if msg.MessageType() != message.MessageTypeSplitShard {
		return SplitShardRoleNone
	}
	switch msg := msg.(type) {
	case message.ImmutableMessage:
		return SplitShardRoleOf(message.MustAsImmutableSplitShardMessageV2(msg).Header(), msg.VChannel())
	case message.MutableMessage:
		return SplitShardRoleOf(message.MustAsMutableSplitShardMessageV2(msg).Header(), msg.VChannel())
	default:
		return SplitShardRoleNone
	}
}

// IsSplitShardGenesis reports whether msg is the copy of a SplitShard message
// that creates the vchannel it is addressed to.
func IsSplitShardGenesis(msg message.BasicMessage) bool {
	return splitShardRoleOfMessage(msg) == SplitShardRoleTarget
}

// IsSplitShardFence reports whether msg is the copy of a SplitShard message
// that fences the vchannel it is addressed to.
func IsSplitShardFence(msg message.BasicMessage) bool {
	return splitShardRoleOfMessage(msg) == SplitShardRoleSource
}

// SplitShardGenesisMessage exposes the creation payload of a target vchannel
// to the existing shard initialization code, the way RestoreCollectionMessage
// does. It is an in-memory projection, never another WAL append. The creation
// origin is exactly the WAL position of the SplitShard copy on the target.
//
// It returns nil for a copy that is not addressed to a target vchannel.
func SplitShardGenesisMessage(msg message.ImmutableMessage) message.ImmutableCreateCollectionMessageV1 {
	if msg.MessageType() != message.MessageTypeSplitShard {
		return nil
	}
	split := message.MustAsImmutableSplitShardMessageV2(msg)
	if SplitShardRoleOf(split.Header(), msg.VChannel()) != SplitShardRoleTarget {
		return nil
	}
	body := split.MustBody()
	created := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(body.GetGenesisHeader()).WithBody(body.GetGenesis()).
		WithVChannel(msg.VChannel()).MustBuildMutable().
		WithTimeTick(msg.TimeTick()).WithLastConfirmed(msg.LastConfirmedMessageID()).
		IntoImmutableMessage(msg.MessageID())
	return message.MustAsImmutableCreateCollectionMessageV1(created)
}
