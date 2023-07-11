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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgdispatcher"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// Manager manage pipeline in querynode
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
	dispatcher   msgdispatcher.Client
	mu           sync.RWMutex
}

func (m *manager) Num() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.channel2Pipeline)
}

// Add pipeline for each channel of collection
func (m *manager) Add(collectionID UniqueID, channel string) (Pipeline, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	log.Debug("start create pipeine",
		zap.Int64("collectionID", collectionID),
		zap.String("channel", channel),
	)
	tr := timerecord.NewTimeRecorder("add dmChannel")
	collection := m.dataManager.Collection.Get(collectionID)
	if collection == nil {
		return nil, merr.WrapErrCollectionNotFound(collectionID)
	}

	if pipeline, ok := m.channel2Pipeline[channel]; ok {
		return pipeline, nil
	}

	//get shard delegator for add growing in pipeline
	delegator, ok := m.delegators.Get(channel)
	if !ok {
		return nil, merr.WrapErrShardDelegatorNotFound(channel)
	}

	newPipeLine, err := NewPipeLine(collectionID, channel, m.dataManager, m.tSafeManager, m.dispatcher, delegator)
	if err != nil {
		return nil, merr.WrapErrServiceUnavailable(err.Error(), "failed to create new pipeline")
	}

	m.channel2Pipeline[channel] = newPipeLine
	metrics.QueryNodeNumFlowGraphs.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Inc()
	metrics.QueryNodeNumDmlChannels.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Inc()
	metrics.QueryNodeWatchDmlChannelLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Observe(float64(tr.ElapseSpan().Milliseconds()))
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

// Remove pipeline from Manager by channel
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
	metrics.QueryNodeNumFlowGraphs.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Dec()
	metrics.QueryNodeNumDmlChannels.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Dec()
}

// Start pipeline by channel
func (m *manager) Start(channels ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	//check pipelie all exist before start
	for _, channel := range channels {
		if _, ok := m.channel2Pipeline[channel]; !ok {
			reason := fmt.Sprintf("pipeline with channel %s not exist", channel)
			return merr.WrapErrServiceUnavailable(reason, "pipine start failed")
		}
	}

	for _, channel := range channels {
		m.channel2Pipeline[channel].Start()
	}
	return nil
}

// Close all pipeline of Manager
func (m *manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, pipeline := range m.channel2Pipeline {
		pipeline.Close()
	}
}

func NewManager(dataManager *DataManager,
	tSafeManager TSafeManager,
	dispatcher msgdispatcher.Client,
	delegators *typeutil.ConcurrentMap[string, delegator.ShardDelegator],
) Manager {
	return &manager{
		channel2Pipeline: make(map[string]Pipeline),
		dataManager:      dataManager,
		delegators:       delegators,
		tSafeManager:     tSafeManager,
		dispatcher:       dispatcher,
	}
}
