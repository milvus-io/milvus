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

package proxynode

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/milvus-io/milvus/internal/util/funcutil"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"go.uber.org/zap"
)

type insertChannelsMap struct {
	collectionID2InsertChannels map[UniqueID]int      // the value of map is the location of insertChannels & insertMsgStreams
	insertChannels              [][]string            // it's a little confusing to use []string as the key of map
	insertMsgStreams            []msgstream.MsgStream // maybe there's a better way to implement Set, just agilely now
	droppedBitMap               []int                 // 0 -> normal, 1 -> dropped
	usageHistogram              []int                 // message stream can be closed only when the use count is zero
	// TODO: use fine grained lock
	mtx          sync.RWMutex
	nodeInstance *ProxyNode
	msFactory    msgstream.Factory
}

func (m *insertChannelsMap) CreateInsertMsgStream(collID UniqueID, channels []string) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	_, ok := m.collectionID2InsertChannels[collID]
	if ok {
		return errors.New("impossible and forbidden to create message stream twice")
	}
	sort.Slice(channels, func(i, j int) bool {
		return channels[i] <= channels[j]
	})
	for loc, existedChannels := range m.insertChannels {
		if m.droppedBitMap[loc] == 0 && funcutil.SortedSliceEqual(existedChannels, channels) {
			m.collectionID2InsertChannels[collID] = loc
			m.usageHistogram[loc]++
			return nil
		}
	}
	m.insertChannels = append(m.insertChannels, channels)
	m.collectionID2InsertChannels[collID] = len(m.insertChannels) - 1

	stream, _ := m.msFactory.NewMsgStream(context.Background())
	stream.AsProducer(channels)
	log.Debug("proxynode", zap.Strings("proxynode AsProducer: ", channels))
	stream.SetRepackFunc(insertRepackFunc)
	stream.Start()
	m.insertMsgStreams = append(m.insertMsgStreams, stream)
	m.droppedBitMap = append(m.droppedBitMap, 0)
	m.usageHistogram = append(m.usageHistogram, 1)

	return nil
}

func (m *insertChannelsMap) CloseInsertMsgStream(collID UniqueID) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	loc, ok := m.collectionID2InsertChannels[collID]
	if !ok {
		return fmt.Errorf("cannot find collection with id %d", collID)
	}
	if m.droppedBitMap[loc] != 0 {
		return errors.New("insert message stream already closed")
	}
	if m.usageHistogram[loc] <= 0 {
		return errors.New("insert message stream already closed")
	}

	m.usageHistogram[loc]--
	if m.usageHistogram[loc] <= 0 {
		m.insertMsgStreams[loc].Close()
		m.droppedBitMap[loc] = 1
		log.Warn("close insert message stream ...")
	}

	delete(m.collectionID2InsertChannels, collID)

	return nil
}

func (m *insertChannelsMap) GetInsertChannels(collID UniqueID) ([]string, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	loc, ok := m.collectionID2InsertChannels[collID]
	if !ok {
		return nil, fmt.Errorf("cannot find collection with id: %d", collID)
	}

	if m.droppedBitMap[loc] != 0 {
		return nil, errors.New("insert message stream already closed")
	}
	ret := append([]string(nil), m.insertChannels[loc]...)
	return ret, nil
}

func (m *insertChannelsMap) GetInsertMsgStream(collID UniqueID) (msgstream.MsgStream, error) {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	loc, ok := m.collectionID2InsertChannels[collID]
	if !ok {
		return nil, fmt.Errorf("cannot find collection with id: %d", collID)
	}

	if m.droppedBitMap[loc] != 0 {
		return nil, errors.New("insert message stream already closed")
	}

	return m.insertMsgStreams[loc], nil
}

func (m *insertChannelsMap) CloseAllMsgStream() {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	for loc, stream := range m.insertMsgStreams {
		if m.droppedBitMap[loc] == 0 && m.usageHistogram[loc] >= 1 {
			stream.Close()
		}
	}

	m.collectionID2InsertChannels = make(map[UniqueID]int)
	m.insertChannels = make([][]string, 0)
	m.insertMsgStreams = make([]msgstream.MsgStream, 0)
	m.droppedBitMap = make([]int, 0)
	m.usageHistogram = make([]int, 0)
}

func newInsertChannelsMap(node *ProxyNode) *insertChannelsMap {
	return &insertChannelsMap{
		collectionID2InsertChannels: make(map[UniqueID]int),
		insertChannels:              make([][]string, 0),
		insertMsgStreams:            make([]msgstream.MsgStream, 0),
		droppedBitMap:               make([]int, 0),
		usageHistogram:              make([]int, 0),
		nodeInstance:                node,
		msFactory:                   node.msFactory,
	}
}

var globalInsertChannelsMap *insertChannelsMap
var initGlobalInsertChannelsMapOnce sync.Once

// change to singleton mode later? Such as GetInsertChannelsMapInstance like GetConfAdapterMgrInstance.
func initGlobalInsertChannelsMap(node *ProxyNode) {
	initGlobalInsertChannelsMapOnce.Do(func() {
		globalInsertChannelsMap = newInsertChannelsMap(node)
	})
}
