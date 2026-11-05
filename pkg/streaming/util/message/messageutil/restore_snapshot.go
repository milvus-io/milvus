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

import "github.com/milvus-io/milvus/pkg/v3/streaming/util/message"

// RestoreCollectionMessage exposes the creation payload to existing shard
// initialization code. It is an in-memory projection, never another WAL append.
// The creation origin is exactly the RestoreSnapshot message's WAL position.
func RestoreCollectionMessage(msg message.ImmutableMessage) message.ImmutableCreateCollectionMessageV1 {
	restore := message.MustAsImmutableRestoreSnapshotMessageV2(msg)
	body := restore.MustBody()
	if body.GetCollection() == nil {
		return nil // Legacy restore into an already-created collection.
	}
	created := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(body.GetCollectionHeader()).WithBody(body.GetCollection()).
		WithVChannel(msg.VChannel()).MustBuildMutable().
		WithTimeTick(msg.TimeTick()).WithLastConfirmed(msg.LastConfirmedMessageID()).
		IntoImmutableMessage(msg.MessageID())
	return message.MustAsImmutableCreateCollectionMessageV1(created)
}
