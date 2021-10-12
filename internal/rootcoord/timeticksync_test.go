// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package rootcoord

import (
	"context"
	"sync"
	"testing"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/stretchr/testify/assert"
)

func TestTimetickSync_sendToChannel(t *testing.T) {
	tt := newTimeTickSync(nil)
	tt.sendToChannel()

	ctt := &internalpb.ChannelTimeTickMsg{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_TimeTick,
		},
	}

	cttm := newChannelTimeTickMsg(ctt)
	tt.proxyTimeTick[1] = cttm
	tt.sendToChannel()

	tt.proxyTimeTick[2] = nil
	tt.sendToChannel()
}

func TestTimetickSync_RemoveDdlTimeTick(t *testing.T) {
	tt := newTimeTickSync(nil)
	tt.AddDdlTimeTick(uint64(1), "1")
	tt.AddDdlTimeTick(uint64(2), "2")
	tt.RemoveDdlTimeTick(uint64(1), "1")
	assert.Equal(t, tt.ddlMinTs, uint64(2))
}

func TestTimetickSync_UpdateTimeTick(t *testing.T) {
	tt := newTimeTickSync(nil)

	ctt := &internalpb.ChannelTimeTickMsg{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_TimeTick,
		},
		DefaultTimestamp: 0,
	}

	err := tt.UpdateTimeTick(ctt, "1")
	assert.Nil(t, err)

	ctt.ChannelNames = append(ctt.ChannelNames, "a")
	err = tt.UpdateTimeTick(ctt, "1")
	assert.Error(t, err)

	core := &Core{
		ctx:       context.TODO(),
		cancel:    nil,
		ddlLock:   sync.Mutex{},
		msFactory: nil,
		session: &sessionutil.Session{
			ServerID: 100,
		},
	}
	tt.core = core

	ctt.Timestamps = append(ctt.Timestamps, uint64(2))
	ctt.Base.SourceID = int64(1)
	cttm := newChannelTimeTickMsg(ctt)
	tt.proxyTimeTick[ctt.Base.SourceID] = cttm
	ctt.DefaultTimestamp = uint64(200)
	tt.ddlMinTs = uint64(100)
	err = tt.UpdateTimeTick(ctt, "1")
	assert.Nil(t, err)

	tt.ddlMinTs = uint64(300)
	tt.proxyTimeTick[ctt.Base.SourceID].in.DefaultTimestamp = uint64(1)
	tt.core.session.ServerID = int64(1)
	err = tt.UpdateTimeTick(ctt, "1")
	assert.Nil(t, err)
}

func Test_minTimeTick(t *testing.T) {
	tts := make([]uint64, 2)
	tts[0] = uint64(5)
	tts[1] = uint64(3)

	ret := minTimeTick(tts...)
	assert.Equal(t, ret, tts[1])
}
