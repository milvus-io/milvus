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

package proxy

import (
	"testing"

	"github.com/milvus-io/milvus/internal/msgstream"

	"github.com/stretchr/testify/assert"
)

func TestChannelsMgrImpl_getChannels(t *testing.T) {
	master := newMockGetChannelsService()
	query := newMockGetChannelsService()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgrImpl(master.GetChannels, nil, query.GetChannels, nil, factory)
	defer mgr.removeAllDMLStream()

	collID := UniqueID(getUniqueIntGeneratorIns().get())
	_, err := mgr.getChannels(collID)
	assert.NotEqual(t, nil, err)

	err = mgr.createDMLMsgStream(collID)
	assert.Equal(t, nil, err)

	_, err = mgr.getChannels(collID)
	assert.Equal(t, nil, err)
}

func TestChannelsMgrImpl_getVChannels(t *testing.T) {
	master := newMockGetChannelsService()
	query := newMockGetChannelsService()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgrImpl(master.GetChannels, nil, query.GetChannels, nil, factory)
	defer mgr.removeAllDMLStream()

	collID := UniqueID(getUniqueIntGeneratorIns().get())
	_, err := mgr.getVChannels(collID)
	assert.NotEqual(t, nil, err)

	err = mgr.createDMLMsgStream(collID)
	assert.Equal(t, nil, err)

	_, err = mgr.getVChannels(collID)
	assert.Equal(t, nil, err)
}

func TestChannelsMgrImpl_createDMLMsgStream(t *testing.T) {
	master := newMockGetChannelsService()
	query := newMockGetChannelsService()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgrImpl(master.GetChannels, nil, query.GetChannels, nil, factory)
	defer mgr.removeAllDMLStream()

	collID := UniqueID(getUniqueIntGeneratorIns().get())
	_, err := mgr.getChannels(collID)
	assert.NotEqual(t, nil, err)
	_, err = mgr.getVChannels(collID)
	assert.NotEqual(t, nil, err)

	err = mgr.createDMLMsgStream(collID)
	assert.Equal(t, nil, err)

	_, err = mgr.getChannels(collID)
	assert.Equal(t, nil, err)
	_, err = mgr.getVChannels(collID)
	assert.Equal(t, nil, err)
}

func TestChannelsMgrImpl_getDMLMsgStream(t *testing.T) {
	master := newMockGetChannelsService()
	query := newMockGetChannelsService()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgrImpl(master.GetChannels, nil, query.GetChannels, nil, factory)
	defer mgr.removeAllDMLStream()

	collID := UniqueID(getUniqueIntGeneratorIns().get())
	_, err := mgr.getDMLStream(collID)
	assert.NotEqual(t, nil, err)

	err = mgr.createDMLMsgStream(collID)
	assert.Equal(t, nil, err)

	_, err = mgr.getDMLStream(collID)
	assert.Equal(t, nil, err)
}

func TestChannelsMgrImpl_removeDMLMsgStream(t *testing.T) {
	master := newMockGetChannelsService()
	query := newMockGetChannelsService()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgrImpl(master.GetChannels, nil, query.GetChannels, nil, factory)
	defer mgr.removeAllDMLStream()

	collID := UniqueID(getUniqueIntGeneratorIns().get())
	_, err := mgr.getDMLStream(collID)
	assert.NotEqual(t, nil, err)

	err = mgr.removeDMLStream(collID)
	assert.NotEqual(t, nil, err)

	err = mgr.createDMLMsgStream(collID)
	assert.Equal(t, nil, err)

	_, err = mgr.getDMLStream(collID)
	assert.Equal(t, nil, err)

	err = mgr.removeDMLStream(collID)
	assert.Equal(t, nil, err)

	_, err = mgr.getDMLStream(collID)
	assert.NotEqual(t, nil, err)
}

func TestChannelsMgrImpl_removeAllDMLMsgStream(t *testing.T) {
	master := newMockGetChannelsService()
	query := newMockGetChannelsService()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgrImpl(master.GetChannels, nil, query.GetChannels, nil, factory)
	defer mgr.removeAllDMLStream()

	num := 10
	for i := 0; i < num; i++ {
		collID := UniqueID(getUniqueIntGeneratorIns().get())
		err := mgr.createDMLMsgStream(collID)
		assert.Equal(t, nil, err)
	}
}

func TestChannelsMgrImpl_createDQLMsgStream(t *testing.T) {
	master := newMockGetChannelsService()
	query := newMockGetChannelsService()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgrImpl(master.GetChannels, nil, query.GetChannels, nil, factory)
	defer mgr.removeAllDMLStream()

	collID := UniqueID(getUniqueIntGeneratorIns().get())

	err := mgr.createDQLStream(collID)
	assert.Equal(t, nil, err)
}

func TestChannelsMgrImpl_getDQLMsgStream(t *testing.T) {
	master := newMockGetChannelsService()
	query := newMockGetChannelsService()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgrImpl(master.GetChannels, nil, query.GetChannels, nil, factory)
	defer mgr.removeAllDMLStream()

	collID := UniqueID(getUniqueIntGeneratorIns().get())
	_, err := mgr.getDQLStream(collID)
	assert.NotEqual(t, nil, err)

	err = mgr.createDQLStream(collID)
	assert.Equal(t, nil, err)

	_, err = mgr.getDQLStream(collID)
	assert.Equal(t, nil, err)
}

func TestChannelsMgrImpl_removeDQLMsgStream(t *testing.T) {
	master := newMockGetChannelsService()
	query := newMockGetChannelsService()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgrImpl(master.GetChannels, nil, query.GetChannels, nil, factory)
	defer mgr.removeAllDMLStream()

	collID := UniqueID(getUniqueIntGeneratorIns().get())
	_, err := mgr.getDQLStream(collID)
	assert.NotEqual(t, nil, err)

	err = mgr.removeDQLStream(collID)
	assert.NotEqual(t, nil, err)

	err = mgr.createDQLStream(collID)
	assert.Equal(t, nil, err)

	_, err = mgr.getDQLStream(collID)
	assert.Equal(t, nil, err)

	err = mgr.removeDQLStream(collID)
	assert.Equal(t, nil, err)

	_, err = mgr.getDQLStream(collID)
	assert.NotEqual(t, nil, err)
}

func TestChannelsMgrImpl_removeAllDQLMsgStream(t *testing.T) {
	master := newMockGetChannelsService()
	query := newMockGetChannelsService()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgrImpl(master.GetChannels, nil, query.GetChannels, nil, factory)
	defer mgr.removeAllDMLStream()

	num := 10
	for i := 0; i < num; i++ {
		collID := UniqueID(getUniqueIntGeneratorIns().get())
		err := mgr.createDQLStream(collID)
		assert.Equal(t, nil, err)
	}
}
