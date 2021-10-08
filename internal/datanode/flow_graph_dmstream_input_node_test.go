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

package datanode

import (
	"context"
	"errors"
	"testing"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/stretchr/testify/assert"
)

type mockMsgStreamFactory struct {
	SetParamsReturnNil  bool
	NewMsgStreamNoError bool
}

var _ msgstream.Factory = &mockMsgStreamFactory{}

func (mm *mockMsgStreamFactory) SetParams(params map[string]interface{}) error {
	if !mm.SetParamsReturnNil {
		return errors.New("Set Params Error")
	}

	return nil
}

func (mm *mockMsgStreamFactory) NewMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	if !mm.NewMsgStreamNoError {
		return nil, errors.New("New MsgStream error")
	}
	return &mockTtMsgStream{}, nil
}

func (mm *mockMsgStreamFactory) NewTtMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return &mockTtMsgStream{}, nil
}

func (mm *mockMsgStreamFactory) NewQueryMsgStream(ctx context.Context) (msgstream.MsgStream, error) {
	return nil, nil
}

type mockTtMsgStream struct {
}

func (mtm *mockTtMsgStream) Start() {}
func (mtm *mockTtMsgStream) Close() {}
func (mtm *mockTtMsgStream) Chan() <-chan *msgstream.MsgPack {
	return make(chan *msgstream.MsgPack, 100)
}

func (mtm *mockTtMsgStream) AsProducer(channels []string)                  {}
func (mtm *mockTtMsgStream) AsConsumer(channels []string, subName string)  {}
func (mtm *mockTtMsgStream) SetRepackFunc(repackFunc msgstream.RepackFunc) {}
func (mtm *mockTtMsgStream) ComputeProduceChannelIndexes(tsMsgs []msgstream.TsMsg) [][]int32 {
	return make([][]int32, 0)
}

func (mtm *mockTtMsgStream) GetProduceChannels() []string {
	return make([]string, 0)
}
func (mtm *mockTtMsgStream) Produce(*msgstream.MsgPack) error {
	return nil
}
func (mtm *mockTtMsgStream) Broadcast(*msgstream.MsgPack) error {
	return nil
}
func (mtm *mockTtMsgStream) BroadcastMark(*msgstream.MsgPack) (map[string][]msgstream.MessageID, error) {
	return map[string][]msgstream.MessageID{}, nil
}
func (mtm *mockTtMsgStream) Consume() *msgstream.MsgPack {
	return nil
}
func (mtm *mockTtMsgStream) Seek(offset []*internalpb.MsgPosition) error {
	return nil
}

func TestNewDmInputNode(t *testing.T) {
	ctx := context.Background()
	_, err := newDmInputNode(ctx, &mockMsgStreamFactory{}, 0, "abc_adc", new(internalpb.MsgPosition))
	assert.Nil(t, err)
}
