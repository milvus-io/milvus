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

package msgdispatcher

import (
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"golang.org/x/net/context"

	"github.com/milvus-io/milvus/pkg/mq/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
)

func TestDispatcher(t *testing.T) {
	ctx := context.Background()
	t.Run("test base", func(t *testing.T) {
		d, err := NewDispatcher(ctx, newMockFactory(), true, "mock_pchannel_0", nil,
			"mock_subName_0", common.SubscriptionPositionEarliest, nil, nil)
		assert.NoError(t, err)
		assert.NotPanics(t, func() {
			d.Handle(start)
			d.Handle(pause)
			d.Handle(resume)
			d.Handle(terminate)
		})

		pos := &msgstream.MsgPosition{
			ChannelName: "mock_vchannel_0",
			MsgGroup:    "mock_msg_group",
			Timestamp:   100,
		}
		d.curTs.Store(pos.GetTimestamp())
		curTs := d.CurTs()
		assert.Equal(t, pos.Timestamp, curTs)
	})

	t.Run("test AsConsumer fail", func(t *testing.T) {
		ms := msgstream.NewMockMsgStream(t)
		ms.EXPECT().AsConsumer(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(errors.New("mock error"))
		ms.EXPECT().Close().Return()
		factory := &msgstream.MockMqFactory{
			NewMsgStreamFunc: func(ctx context.Context) (msgstream.MsgStream, error) {
				return ms, nil
			},
		}
		d, err := NewDispatcher(ctx, factory, true, "mock_pchannel_0", nil,
			"mock_subName_0", common.SubscriptionPositionEarliest, nil, nil)

		assert.Error(t, err)
		assert.Nil(t, d)
	})

	t.Run("test target", func(t *testing.T) {
		d, err := NewDispatcher(ctx, newMockFactory(), true, "mock_pchannel_0", nil,
			"mock_subName_0", common.SubscriptionPositionEarliest, nil, nil)
		assert.NoError(t, err)
		output := make(chan *msgstream.MsgPack, 1024)
		getTarget := func(vchannel string, pos *Pos, ch chan *msgstream.MsgPack) *target {
			target := newTarget(vchannel, pos)
			target.ch = ch
			return target
		}
		d.AddTarget(getTarget("mock_vchannel_0", nil, output))
		d.AddTarget(getTarget("mock_vchannel_1", nil, nil))
		num := d.TargetNum()
		assert.Equal(t, 2, num)

		target, err := d.GetTarget("mock_vchannel_0")
		assert.NoError(t, err)
		assert.Equal(t, cap(output), cap(target.ch))

		d.CloseTarget("mock_vchannel_0")

		select {
		case <-time.After(1 * time.Second):
			assert.Fail(t, "timeout, didn't receive close message")
		case _, ok := <-target.ch:
			assert.False(t, ok)
		}

		num = d.TargetNum()
		assert.Equal(t, 1, num)
	})

	t.Run("test concurrent send and close", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			output := make(chan *msgstream.MsgPack, 1024)
			target := newTarget("mock_vchannel_0", nil)
			target.ch = output
			assert.Equal(t, cap(output), cap(target.ch))
			wg := &sync.WaitGroup{}
			for j := 0; j < 100; j++ {
				wg.Add(1)
				go func() {
					err := target.send(&MsgPack{})
					assert.NoError(t, err)
					wg.Done()
				}()
				wg.Add(1)
				go func() {
					target.close()
					wg.Done()
				}()
			}
			wg.Wait()
		}
	})
}

func BenchmarkDispatcher_handle(b *testing.B) {
	d, err := NewDispatcher(context.Background(), newMockFactory(), true, "mock_pchannel_0", nil,
		"mock_subName_0", common.SubscriptionPositionEarliest, nil, nil)
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		d.Handle(start)
		d.Handle(pause)
		d.Handle(resume)
		d.Handle(terminate)
	}
	// BenchmarkDispatcher_handle-12    	    9568	    122123 ns/op
	// PASS
}
