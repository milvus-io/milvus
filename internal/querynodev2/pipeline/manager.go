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

package pipeline

import (
	"fmt"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

//Manager manage pipeline in querynode
type Manager interface {
	Num() int
	Add(collectionID UniqueID, channel string) (Pipeline, error)
	Get(channel string) Pipeline
	Remove(channels ...string)
	Start(channels ...string) error
	Close()
}
type manager struct {
	channel2Pipeline map[string]Pipeline
	dataManager      *DataManager
	delegators       *typeutil.ConcurrentMap[string, delegator.ShardDelegator]

	tSafeManager TSafeManager
	msFactory    msgstream.Factory
	mu           sync.Mutex
}

func (m *manager) Num() int {
	return len(m.channel2Pipeline)
}

//Add pipeline for each channel of collection
func (m *manager) Add(collectionID UniqueID, channel string) (Pipeline, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Debug("start create pipeine",
		zap.Int64("collectionID", collectionID),
		zap.String("channel", channel),
	)
	collection := m.dataManager.Collection.Get(collectionID)
	if collection == nil {
		return nil, segments.WrapCollectionNotFound(collectionID)
	}

	if pipeline, ok := m.channel2Pipeline[channel]; ok {
		return pipeline, nil
	}

	//get shard delegator for add growing in pipeline
	delegator, ok := m.delegators.Get(channel)
	if !ok {
		return nil, WrapErrShardDelegatorNotFound(channel)
	}

	newPipeLine, err := NewPipeLine(collectionID, channel, m.dataManager, m.tSafeManager, m.msFactory, delegator)
	if err != nil {
		return nil, WrapErrNewPipelineFailed(err)
	}

	m.channel2Pipeline[channel] = newPipeLine
	return newPipeLine, nil
}

func (m *manager) Get(channel string) Pipeline {
	m.mu.Lock()
	defer m.mu.Unlock()

	pipeline, ok := m.channel2Pipeline[channel]
	if !ok {
		log.Warn("pipeline not existed",
			zap.String("channel", channel),
		)
		return nil
	}

	return pipeline
}

//Remove pipeline from Manager by channel
func (m *manager) Remove(channels ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, channel := range channels {
		if pipeline, ok := m.channel2Pipeline[channel]; ok {
			pipeline.Close()
			delete(m.channel2Pipeline, channel)
		} else {
			log.Warn("pipeline to be removed doesn't existed", zap.Any("channel", channel))
		}
	}
}

//Start pipeline by channel
func (m *manager) Start(channels ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	//check pipelie all exist before start
	for _, channel := range channels {
		if _, ok := m.channel2Pipeline[channel]; !ok {
			return WrapErrStartPipeline(fmt.Sprintf("pipeline with channel %s not exist", channel))
		}
	}

	for _, channel := range channels {
		m.channel2Pipeline[channel].Start()
	}
	return nil
}

//Close all pipeline of Manager
func (m *manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, pipeline := range m.channel2Pipeline {
		pipeline.Close()
	}
}

func NewManager(dataManager *DataManager,
	tSafeManager TSafeManager,
	msFactory msgstream.Factory,
	delegators *typeutil.ConcurrentMap[string, delegator.ShardDelegator],
) Manager {
	return &manager{
		channel2Pipeline: make(map[string]Pipeline),
		dataManager:      dataManager,
		delegators:       delegators,
		tSafeManager:     tSafeManager,
		msFactory:        msFactory,
		mu:               sync.Mutex{},
	}
}
