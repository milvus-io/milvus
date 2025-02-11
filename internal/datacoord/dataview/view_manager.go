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
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type PullNewDataViewFunction func(collectionID int64) (*DataView, error)

type ViewManager interface {
	Get(collectionID int64) (*DataView, error)
	GetVersion(collectionID int64) int64
	Remove(collectionID int64)

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

	v, ok := m.currentViews.GetOrInsert(collectionID, view)
	if !ok {
		log.Info("update new data view", zap.Int64("collectionID", collectionID), zap.Int64("version", view.Version))
	}
	return v, nil
}

func (m *dataViewManager) GetVersion(collectionID int64) int64 {
	if view, ok := m.currentViews.Get(collectionID); ok {
		return view.Version
	}
	return InitialDataViewVersion
}

func (m *dataViewManager) Remove(collectionID int64) {
	if view, ok := m.currentViews.GetAndRemove(collectionID); ok {
		log.Info("data view removed", zap.Int64("collectionID", collectionID), zap.Int64("version", view.Version))
	}
}

func (m *dataViewManager) Start() {
	ticker := time.NewTicker(paramtable.Get().DataCoordCfg.DataViewCheckInterval.GetAsDuration(time.Second))
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

func (m *dataViewManager) update(view *DataView, reason string) {
	m.currentViews.Insert(view.CollectionID, view)
	log.Info("update new data view", zap.Int64("collectionID", view.CollectionID), zap.Int64("version", view.Version), zap.String("reason", reason))
}

func (m *dataViewManager) TryUpdateDataView(collectionID int64) {
	newView, err := m.pullFn(collectionID)
	if err != nil {
		log.Warn("pull new data view failed", zap.Int64("collectionID", collectionID), zap.Error(err))
		// notify to trigger retry
		NotifyUpdate(collectionID)
		return
	}

	curView, ok := m.currentViews.Get(collectionID)
	if !ok {
		// update due to data view is empty
		m.update(newView, "init data view")
		return
	}
	// no-op if the incoming version is less than the current version.
	if newView.Version <= curView.Version {
		log.Warn("stale version, skip update", zap.Int64("collectionID", collectionID),
			zap.Int64("new", newView.Version), zap.Int64("current", curView.Version))
		return
	}

	for channel, new := range newView.Channels {
		cur, ok := curView.Channels[channel]
		if !ok {
			// update due to channel info is empty
			m.update(newView, "init vchannel info")
			return
		}
		// Check whether the VChannelInfo is equal.
		// To prevent frequent updates of the dataView due to channel cp changes,
		// here we ignore the channel cp. This is acceptable because
		// we have the mechanism of forcibly updating the dataView as a fallback.
		curCP, newCP := cur.GetSeekPosition(), new.GetSeekPosition()
		cur.SeekPosition, new.SeekPosition = nil, nil
		if !proto.Equal(cur, new) {
			cur.SeekPosition, new.SeekPosition = curCP, newCP
			// update due to channel info changed
			m.update(newView, "vchannel info changed")
			return
		}
		cur.SeekPosition, new.SeekPosition = curCP, newCP
	}

	if !typeutil.MapEqual(newView.Segments, curView.Segments) {
		// update due to segments list changed
		m.update(newView, "segment list changed")
		return
	}

	// Force update data view.
	// DataView's Version is from Time.UnixNano(), so here we just use version as data view time.
	curTime := time.Unix(0, curView.Version)
	newTime := time.Unix(0, newView.Version)
	if newTime.Sub(curTime) > paramtable.Get().DataCoordCfg.ForceUpdateDataViewInterval.GetAsDuration(time.Second) {
		// force update
		m.update(newView, fmt.Sprintf("force update, curTime=%v, newTime=%v", curTime, newTime))
	}
}
