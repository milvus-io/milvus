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

package meta

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

// A shard split source's recovery info carries its split target vchannels next
// to a seek position past the fence; the QueryNode recovers the split's
// children only when the field is set. A target QueryCoord saved and restores
// across a restart keeps the two together.
func TestCollectionTargetKeepsTheSplitSignalAcrossARestart(t *testing.T) {
	src := &DmChannel{VchannelInfo: &datapb.VchannelInfo{
		CollectionID:        1,
		ChannelName:         "src",
		SeekPosition:        &msgpb.MsgPosition{ChannelName: "src", Timestamp: 200},
		SplitTargetChannels: []string{"t1", "t2"},
	}}
	other := &DmChannel{VchannelInfo: &datapb.VchannelInfo{
		CollectionID: 1,
		ChannelName:  "v9",
		SeekPosition: &msgpb.MsgPosition{ChannelName: "v9", Timestamp: 200},
	}}
	target := NewCollectionTarget(nil, map[string]*DmChannel{"src": src, "v9": other}, []int64{100})

	// what the catalog stores and a restarted QueryCoord reads back.
	saved, err := proto.Marshal(target.toPbMsg())
	require.NoError(t, err)
	restoredPb := &querypb.CollectionTarget{}
	require.NoError(t, proto.Unmarshal(saved, restoredPb))
	restored := FromPbCollectionTarget(restoredPb)

	assert.Equal(t, []string{"t1", "t2"}, restored.GetAllDmChannels()["src"].GetSplitTargetChannels())
	assert.Equal(t, uint64(200), restored.GetAllDmChannels()["src"].GetSeekPosition().GetTimestamp())
	assert.Empty(t, restored.GetAllDmChannels()["v9"].GetSplitTargetChannels())
}
