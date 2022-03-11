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
	"errors"
	"sync"
	"testing"

	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/internal/util/funcutil"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDmlChannels(t *testing.T) {
	const (
		dmlChanPrefix      = "rootcoord-dml"
		totalDmlChannelNum = 2
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	factory := msgstream.NewPmsFactory()
	Params.Init()

	m := map[string]interface{}{
		"pulsarAddress":  Params.PulsarCfg.Address,
		"receiveBufSize": 1024,
		"pulsarBufSize":  1024}
	err := factory.SetParams(m)
	assert.Nil(t, err)

	dml := newDmlChannels(ctx, factory, dmlChanPrefix, totalDmlChannelNum)
	chanNames := dml.listChannels()
	assert.Equal(t, 0, len(chanNames))

	randStr := funcutil.RandomString(8)
	assert.Panics(t, func() { dml.addChannels(randStr) })
	assert.Panics(t, func() { dml.broadcast([]string{randStr}, nil) })
	assert.Panics(t, func() { dml.broadcastMark([]string{randStr}, nil) })
	assert.Panics(t, func() { dml.removeChannels(randStr) })

	// dml_xxx_0 => {chanName0, chanName2}
	// dml_xxx_1 => {chanName1}
	chanName0 := dml.getChannelName()
	dml.addChannels(chanName0)
	assert.Equal(t, 1, dml.getChannelNum())

	chanName1 := dml.getChannelName()
	dml.addChannels(chanName1)
	assert.Equal(t, 2, dml.getChannelNum())

	chanName2 := dml.getChannelName()
	dml.addChannels(chanName2)
	assert.Equal(t, 2, dml.getChannelNum())

	dml.removeChannels(chanName0)
	assert.Equal(t, 2, dml.getChannelNum())

	dml.removeChannels(chanName1)
	assert.Equal(t, 1, dml.getChannelNum())

	dml.removeChannels(chanName0)
	assert.Equal(t, 0, dml.getChannelNum())
}

func TestDmChannelsFailure(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	t.Run("Test newDmlChannels", func(t *testing.T) {
		defer wg.Done()
		mockFactory := &FailMessageStreamFactory{}
		assert.Panics(t, func() { newDmlChannels(context.TODO(), mockFactory, "test-newdmlchannel-root", 1) })
	})

	wg.Add(1)
	t.Run("Test broadcast", func(t *testing.T) {
		defer wg.Done()
		mockFactory := &FailMessageStreamFactory{errBroadcast: true}
		dml := newDmlChannels(context.TODO(), mockFactory, "test-newdmlchannel-root", 1)
		chanName0 := dml.getChannelName()
		dml.addChannels(chanName0)
		require.Equal(t, 1, dml.getChannelNum())

		err := dml.broadcast([]string{chanName0}, nil)
		assert.Error(t, err)

		v, err := dml.broadcastMark([]string{chanName0}, nil)
		assert.Empty(t, v)
		assert.Error(t, err)
	})
	wg.Wait()
}

// FailMessageStreamFactory mock MessageStreamFactory failure
type FailMessageStreamFactory struct {
	msgstream.Factory
	errBroadcast bool
}

func (f *FailMessageStreamFactory) NewMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	if f.errBroadcast {
		return &FailMsgStream{errBroadcast: true}, nil
	}
	return nil, errors.New("mocked failure")
}

func (f *FailMessageStreamFactory) NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return nil, errors.New("mocked failure")
}

type FailMsgStream struct {
	msgstream.MsgStream
	errBroadcast bool
}

func (ms *FailMsgStream) Start()                                       {}
func (ms *FailMsgStream) Close()                                       {}
func (ms *FailMsgStream) Chan() <-chan *msgstream.MsgPack              { return nil }
func (ms *FailMsgStream) AsProducer(channels []string)                 {}
func (ms *FailMsgStream) AsConsumer(channels []string, subName string) {}
func (ms *FailMsgStream) AsReader(channels []string, subName string)   {}
func (ms *FailMsgStream) AsConsumerWithPosition(channels []string, subName string, position mqwrapper.SubscriptionInitialPosition) {
}
func (ms *FailMsgStream) SetRepackFunc(repackFunc msgstream.RepackFunc)                   {}
func (ms *FailMsgStream) ComputeProduceChannelIndexes(tsMsgs []msgstream.TsMsg) [][]int32 { return nil }
func (ms *FailMsgStream) GetProduceChannels() []string                                    { return nil }
func (ms *FailMsgStream) Produce(*msgstream.MsgPack) error                                { return nil }
func (ms *FailMsgStream) ProduceMark(*msgstream.MsgPack) (map[string][]msgstream.MessageID, error) {
	return nil, nil
}
func (ms *FailMsgStream) Broadcast(*msgstream.MsgPack) error {
	if ms.errBroadcast {
		return errors.New("broadcast error")
	}
	return nil
}
func (ms *FailMsgStream) BroadcastMark(*msgstream.MsgPack) (map[string][]msgstream.MessageID, error) {
	if ms.errBroadcast {
		return nil, errors.New("broadcastMark error")
	}
	return nil, nil
}
func (ms *FailMsgStream) Next(ctx context.Context, channelName string) (msgstream.TsMsg, error) {
	return nil, nil
}
func (ms *FailMsgStream) HasNext(channelName string) bool                         { return true }
func (ms *FailMsgStream) Seek(offset []*msgstream.MsgPosition) error              { return nil }
func (ms *FailMsgStream) SeekReaders(msgPositions []*msgstream.MsgPosition) error { return nil }
