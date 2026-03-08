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

package rootcoord

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestTimetickSync(t *testing.T) {
	ctx := context.Background()
	sourceID := int64(100)
	factory := dependency.NewDefaultFactory(true)

	//chanMap := map[typeutil.UniqueID][]string{
	//	int64(1): {"rootcoord-dml_0"},
	//}

	paramtable.Get().Save(Params.RootCoordCfg.DmlChannelNum.Key, "2")
	paramtable.Get().Save(Params.CommonCfg.RootCoordDml.Key, "rootcoord-dml")
	ttSync := newTimeTickSync(context.TODO(), ctx, sourceID, factory, nil)

	var wg sync.WaitGroup
	wg.Add(1)
	t.Run("sendToChannel", func(t *testing.T) {
		defer wg.Done()
		result := ttSync.sendToChannel()
		assert.False(t, result)

		ttSync.sess2ChanTsMap[1] = nil
		result = ttSync.sendToChannel()
		assert.False(t, result)

		msg := &internalpb.ChannelTimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_TimeTick,
			},
		}
		ttSync.sess2ChanTsMap[1] = newChanTsMsg(msg, 1)
		result = ttSync.sendToChannel()
		assert.True(t, result)
	})

	wg.Add(1)
	t.Run("UpdateTimeTick", func(t *testing.T) {
		defer wg.Done()
		msg := &internalpb.ChannelTimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_TimeTick,
				SourceID: int64(1),
			},
			DefaultTimestamp: 0,
		}

		err := ttSync.updateTimeTick(msg, "1")
		assert.NoError(t, err)

		msg.ChannelNames = append(msg.ChannelNames, "a")
		err = ttSync.updateTimeTick(msg, "1")
		assert.Error(t, err)

		msg.Timestamps = append(msg.Timestamps, uint64(2))
		msg.DefaultTimestamp = uint64(200)
		cttMsg := newChanTsMsg(msg, 1)
		ttSync.sess2ChanTsMap[msg.Base.SourceID] = cttMsg

		err = ttSync.updateTimeTick(msg, "1")
		assert.NoError(t, err)

		ttSync.sourceID = int64(1)
		err = ttSync.updateTimeTick(msg, "1")
		assert.NoError(t, err)
	})

	wg.Add(1)
	t.Run("minTimeTick", func(t *testing.T) {
		defer wg.Done()
		tts := make([]uint64, 2)
		tts[0] = uint64(5)
		tts[1] = uint64(3)

		ret := minTimeTick(tts...)
		assert.Equal(t, ret, tts[1])
	})
	wg.Wait()
}

func TestMultiTimetickSync(t *testing.T) {
	ctx := context.Background()

	factory := dependency.NewDefaultFactory(true)

	//chanMap := map[typeutil.UniqueID][]string{
	//	int64(1): {"rootcoord-dml_0"},
	//}

	paramtable.Get().Save(Params.RootCoordCfg.DmlChannelNum.Key, "1")
	paramtable.Get().Save(Params.CommonCfg.RootCoordDml.Key, "rootcoord-dml")
	ttSync := newTimeTickSync(context.TODO(), ctx, UniqueID(0), factory, nil)

	var wg sync.WaitGroup

	wg.Add(1)
	t.Run("UpdateTimeTick", func(t *testing.T) {
		defer wg.Done()

		// suppose this is rooit
		ttSync.addSession(&sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 1}})

		// suppose this is proxy1
		ttSync.addSession(&sessionutil.Session{SessionRaw: sessionutil.SessionRaw{ServerID: 2}})

		msg := &internalpb.ChannelTimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_TimeTick,
				SourceID: int64(1),
			},
			DefaultTimestamp: 100,
		}

		err := ttSync.updateTimeTick(msg, "1")
		assert.NoError(t, err)

		msg2 := &internalpb.ChannelTimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType:  commonpb.MsgType_TimeTick,
				SourceID: int64(2),
			},
			DefaultTimestamp: 102,
		}
		err = ttSync.updateTimeTick(msg2, "2")
		assert.NoError(t, err)

		// make sure result works
		result := <-ttSync.sendChan
		assert.True(t, len(result) == 2)
	})

	wg.Wait()
}

func Test_ttHistogram_get(t *testing.T) {
	h := newTtHistogram()
	assert.Equal(t, typeutil.ZeroTimestamp, h.get("not_exist"))
	h.update("ch1", 100)
	assert.Equal(t, Timestamp(100), h.get("ch1"))
	h.update("ch2", 1000)
	assert.Equal(t, Timestamp(1000), h.get("ch2"))
	h.remove("ch1", "ch2", "not_exist")
	assert.Equal(t, typeutil.ZeroTimestamp, h.get("ch1"))
	assert.Equal(t, typeutil.ZeroTimestamp, h.get("ch2"))
}

func TestTimetickSyncWithExistChannels(t *testing.T) {
	ctx := context.Background()
	sourceID := int64(100)

	factory := dependency.NewDefaultFactory(true)

	//chanMap := map[typeutil.UniqueID][]string{
	//	int64(1): {"rootcoord-dml_0"},
	//}

	paramtable.Get().Save(Params.CommonCfg.RootCoordDml.Key, "rootcoord-dml")
	chans := map[UniqueID][]string{}

	chans[UniqueID(100)] = []string{"by-dev-rootcoord-dml_4", "by-dev-rootcoord-dml_8"}
	chans[UniqueID(102)] = []string{"by-dev-rootcoord-dml_2", "by-dev-rootcoord-dml_9"}
	ttSync := newTimeTickSync(context.TODO(), ctx, sourceID, factory, chans)

	var wg sync.WaitGroup
	wg.Add(1)
	t.Run("sendToChannel", func(t *testing.T) {
		defer wg.Done()
		ttSync.sendToChannel()

		ttSync.sess2ChanTsMap[1] = nil
		ttSync.sendToChannel()

		msg := &internalpb.ChannelTimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_TimeTick,
			},
		}
		ttSync.sess2ChanTsMap[1] = newChanTsMsg(msg, 1)
		ttSync.sendToChannel()
	})

	wg.Add(1)
	t.Run("assign channels", func(t *testing.T) {
		defer wg.Done()
		channels := ttSync.getDmlChannelNames(int(4))
		assert.Equal(t, channels, []string{"by-dev-rootcoord-dml_0", "by-dev-rootcoord-dml_1", "by-dev-rootcoord-dml_3", "by-dev-rootcoord-dml_5"})

		channels = ttSync.getDmlChannelNames(int(4))
		assert.Equal(t, channels, []string{"by-dev-rootcoord-dml_6", "by-dev-rootcoord-dml_7", "by-dev-rootcoord-dml_0", "by-dev-rootcoord-dml_1"})
	})

	// test get new channels
}

func TestTimetickSyncInvalidName(t *testing.T) {
	ctx := context.Background()
	sourceID := int64(100)

	factory := dependency.NewDefaultFactory(true)

	//chanMap := map[typeutil.UniqueID][]string{
	//	int64(1): {"rootcoord-dml_0"},
	//}

	paramtable.Get().Save(Params.CommonCfg.RootCoordDml.Key, "rootcoord-dml")
	chans := map[UniqueID][]string{}
	chans[UniqueID(100)] = []string{"rootcoord-dml4"}
	assert.Panics(t, func() {
		newTimeTickSync(context.TODO(), ctx, sourceID, factory, chans)
	})

	chans = map[UniqueID][]string{}
	chans[UniqueID(102)] = []string{"rootcoord-dml_a"}
	assert.Panics(t, func() {
		newTimeTickSync(context.TODO(), ctx, sourceID, factory, chans)
	})
}
