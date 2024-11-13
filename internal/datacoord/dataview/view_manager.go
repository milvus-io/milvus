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

package dataview

import (
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type PullNewDataViewFunction func(collectionID int64) (*DataView, error)

type ViewManager interface {
	Get(collectionID int64) (*DataView, error)
	GetVersion(collectionID int64) int64

	Start()
	Close()
}

type dataViewManager struct {
	pullFn       PullNewDataViewFunction
	currentViews *typeutil.ConcurrentMap[int64, *DataView]

	closeOnce sync.Once
	closeChan chan struct{}
}

func NewDataViewManager(pullFn PullNewDataViewFunction) ViewManager {
	initUpdateChan()
	return &dataViewManager{
		pullFn:       pullFn,
		currentViews: typeutil.NewConcurrentMap[int64, *DataView](),
		closeChan:    make(chan struct{}),
	}
}

func (m *dataViewManager) Get(collectionID int64) (*DataView, error) {
	if view, ok := m.currentViews.Get(collectionID); ok {
		return view, nil
	}
	view, err := m.pullFn(collectionID)
	if err != nil {
		return nil, err
	}
	m.currentViews.GetOrInsert(collectionID, view)
	return view, nil
}

func (m *dataViewManager) GetVersion(collectionID int64) int64 {
	if view, ok := m.currentViews.Get(collectionID); ok {
		return view.Version
	}
	return InitialDataViewVersion
}

func (m *dataViewManager) Start() {
	ticker := time.NewTicker(paramtable.Get().DataCoordCfg.DataViewUpdateInterval.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-m.closeChan:
			log.Info("data view manager exited")
			return
		case <-ticker.C:
			// periodically update all data view
			for _, collectionID := range m.currentViews.Keys() {
				m.TryUpdateDataView(collectionID)
			}
		case collectionID := <-updateChan:
			m.TryUpdateDataView(collectionID)
		}
	}
}

func (m *dataViewManager) Close() {
	m.closeOnce.Do(func() {
		close(m.closeChan)
	})
}

func (m *dataViewManager) update(view *DataView) {
	_, ok := m.currentViews.GetOrInsert(view.CollectionID, view)
	if ok {
		log.Info("update new data view", zap.Int64("collectionID", view.CollectionID), zap.Int64("version", view.Version))
	}
}

func (m *dataViewManager) TryUpdateDataView(collectionID int64) {
	newView, err := m.pullFn(collectionID)
	if err != nil {
		log.Warn("pull new data view failed", zap.Int64("collectionID", collectionID), zap.Error(err))
		// notify to trigger pull again
		NotifyUpdate(collectionID)
		return
	}

	currentView, ok := m.currentViews.Get(collectionID)
	if !ok {
		m.currentViews.GetOrInsert(collectionID, newView)
		return
	}
	// no-op if the incoming version is less than the current version.
	if newView.Version <= currentView.Version {
		return
	}

	// check if channel info has been updated.
	for channel, new := range newView.Channels {
		current, ok := currentView.Channels[channel]
		if !ok {
			m.update(newView)
			return
		}
		if !funcutil.SliceSetEqual(new.GetLevelZeroSegmentIds(), current.GetLevelZeroSegmentIds()) ||
			!funcutil.SliceSetEqual(new.GetUnflushedSegmentIds(), current.GetUnflushedSegmentIds()) ||
			!funcutil.SliceSetEqual(new.GetFlushedSegmentIds(), current.GetFlushedSegmentIds()) ||
			!funcutil.SliceSetEqual(new.GetIndexedSegmentIds(), current.GetIndexedSegmentIds()) ||
			!funcutil.SliceSetEqual(new.GetDroppedSegmentIds(), current.GetDroppedSegmentIds()) {
			m.update(newView)
			return
		}
		if !typeutil.MapEqual(new.GetPartitionStatsVersions(), current.GetPartitionStatsVersions()) {
			m.update(newView)
			return
		}
		// TODO: It might be too frequent.
		if new.GetSeekPosition().GetTimestamp() > current.GetSeekPosition().GetTimestamp() {
			m.update(newView)
			return
		}
	}

	// check if segment info has been updated.
	if !typeutil.MapEqual(newView.Segments, currentView.Segments) {
		m.currentViews.GetOrInsert(collectionID, newView)
	}
}
