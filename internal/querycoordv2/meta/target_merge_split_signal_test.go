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

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

// Merging the recovery infos of one channel keeps the earliest seek, and must
// keep the split signal with it: a channel any info reports as an unfinished
// split source stays one, whichever info's seek wins.
func TestMergeDmChannelInfoKeepsTheSplitSignal(t *testing.T) {
	info := func(ts uint64, splitTargets ...string) *datapb.VchannelInfo {
		return &datapb.VchannelInfo{
			CollectionID:        1,
			ChannelName:         "src",
			SeekPosition:        &msgpb.MsgPosition{ChannelName: "src", Timestamp: ts},
			SplitTargetChannels: splitTargets,
		}
	}
	for name, infos := range map[string][]*datapb.VchannelInfo{
		"signal on the later seek":                      {info(200, "t1", "t2"), info(100)},
		"signal on the earlier seek":                    {info(100, "t1", "t2"), info(200)},
		"signal on both":                                {info(200, "t1", "t2"), info(100, "t2", "t1")},
		"signal only on a later info, whose seek wins":  {info(200), info(100, "t1", "t2")},
		"signal only on a later info, whose seek loses": {info(100), info(200, "t1", "t2")},
	} {
		merged := MergeDmChannelInfo(infos)
		assert.Equal(t, uint64(100), merged.GetSeekPosition().GetTimestamp(), name)
		assert.ElementsMatch(t, []string{"t1", "t2"}, merged.GetSplitTargetChannels(), name)
	}
	assert.Empty(t, MergeDmChannelInfo([]*datapb.VchannelInfo{info(200), info(100)}).GetSplitTargetChannels())
}
