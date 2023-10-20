/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package msgstream

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

func TestFlushMsg(t *testing.T) {
	var msg TsMsg = &FlushMsg{
		FlushRequest: milvuspb.FlushRequest{
			Base: &commonpb.MsgBase{
				MsgType:       commonpb.MsgType_Flush,
				MsgID:         100,
				Timestamp:     1000,
				SourceID:      10000,
				TargetID:      100000,
				ReplicateInfo: nil,
			},
			DbName:          "unit_db",
			CollectionNames: []string{"col1", "col2"},
		},
	}
	assert.EqualValues(t, 100, msg.ID())
	msg.SetID(200)
	assert.EqualValues(t, 200, msg.ID())
	assert.Equal(t, commonpb.MsgType_Flush, msg.Type())
	assert.EqualValues(t, 10000, msg.SourceID())

	msgBytes, err := msg.Marshal(msg)
	assert.NoError(t, err)

	var newMsg TsMsg = &FlushMsg{}
	_, err = newMsg.Unmarshal("1")
	assert.Error(t, err)

	newMsg, err = newMsg.Unmarshal(msgBytes)
	assert.NoError(t, err)
	assert.EqualValues(t, 200, newMsg.ID())
	assert.EqualValues(t, 1000, newMsg.BeginTs())
	assert.EqualValues(t, 1000, newMsg.EndTs())

	assert.True(t, msg.Size() > 0)
}

func TestLoadCollection(t *testing.T) {
	var msg TsMsg = &LoadCollectionMsg{
		LoadCollectionRequest: milvuspb.LoadCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:       commonpb.MsgType_LoadCollection,
				MsgID:         100,
				Timestamp:     1000,
				SourceID:      10000,
				TargetID:      100000,
				ReplicateInfo: nil,
			},
			DbName:         "unit_db",
			CollectionName: "col1",
		},
	}
	assert.EqualValues(t, 100, msg.ID())
	msg.SetID(200)
	assert.EqualValues(t, 200, msg.ID())
	assert.Equal(t, commonpb.MsgType_LoadCollection, msg.Type())
	assert.EqualValues(t, 10000, msg.SourceID())

	msgBytes, err := msg.Marshal(msg)
	assert.NoError(t, err)

	var newMsg TsMsg = &LoadCollectionMsg{}
	_, err = newMsg.Unmarshal("1")
	assert.Error(t, err)

	newMsg, err = newMsg.Unmarshal(msgBytes)
	assert.NoError(t, err)
	assert.EqualValues(t, 200, newMsg.ID())
	assert.EqualValues(t, 1000, newMsg.BeginTs())
	assert.EqualValues(t, 1000, newMsg.EndTs())

	assert.True(t, msg.Size() > 0)
}

func TestReleaseCollection(t *testing.T) {
	var msg TsMsg = &ReleaseCollectionMsg{
		ReleaseCollectionRequest: milvuspb.ReleaseCollectionRequest{
			Base: &commonpb.MsgBase{
				MsgType:       commonpb.MsgType_ReleaseCollection,
				MsgID:         100,
				Timestamp:     1000,
				SourceID:      10000,
				TargetID:      100000,
				ReplicateInfo: nil,
			},
			DbName:         "unit_db",
			CollectionName: "col1",
		},
	}
	assert.EqualValues(t, 100, msg.ID())
	msg.SetID(200)
	assert.EqualValues(t, 200, msg.ID())
	assert.Equal(t, commonpb.MsgType_ReleaseCollection, msg.Type())
	assert.EqualValues(t, 10000, msg.SourceID())

	msgBytes, err := msg.Marshal(msg)
	assert.NoError(t, err)

	var newMsg TsMsg = &ReleaseCollectionMsg{}
	_, err = newMsg.Unmarshal("1")
	assert.Error(t, err)

	newMsg, err = newMsg.Unmarshal(msgBytes)
	assert.NoError(t, err)
	assert.EqualValues(t, 200, newMsg.ID())
	assert.EqualValues(t, 1000, newMsg.BeginTs())
	assert.EqualValues(t, 1000, newMsg.EndTs())

	assert.True(t, msg.Size() > 0)
}
