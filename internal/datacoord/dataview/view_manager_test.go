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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func init() {
	paramtable.Init()
}

func TestNewDataViewManager_Get(t *testing.T) {
	pullFn := func(collectionID int64) (*DataView, error) {
		return &DataView{
			CollectionID: collectionID,
			Channels:     nil,
			Segments:     nil,
			Version:      time.Now().UnixNano(),
		}, nil
	}
	manager := NewDataViewManager(pullFn)

	collectionID := int64(1)
	// No data view
	version := manager.GetVersion(collectionID)
	assert.Equal(t, InitialDataViewVersion, version)

	// Lazy get data view
	v1, err := manager.Get(collectionID)
	assert.NoError(t, err)
	assert.NotEqual(t, InitialDataViewVersion, v1)
	version = manager.GetVersion(v1.CollectionID)
	assert.Equal(t, v1.Version, version)

	// Get again, data view should not update
	v2, err := manager.Get(collectionID)
	assert.NoError(t, err)
	assert.Equal(t, v1, v2)
}

func TestNewDataViewManager_TryUpdateDataView(t *testing.T) {
	manager := NewDataViewManager(nil)
	go manager.Start()
	defer manager.Close()

	collectionID := int64(1)

	// Update due to data view is empty
	v1 := &DataView{
		CollectionID: collectionID,
		Version:      time.Now().UnixNano(),
	}
	manager.(*dataViewManager).pullFn = func(collectionID int64) (*DataView, error) {
		return v1, nil
	}
	NotifyUpdate(collectionID)
	assert.Eventually(t, func() bool {
		version := manager.GetVersion(collectionID)
		return version == v1.Version
	}, 1*time.Second, 10*time.Millisecond)

	// Update due to channel info is empty
	v2 := &DataView{
		CollectionID: collectionID,
		Channels: map[string]*datapb.VchannelInfo{"ch0": {
			CollectionID: collectionID,
			ChannelName:  "ch0",
		}},
		Version: time.Now().UnixNano(),
	}
	manager.(*dataViewManager).pullFn = func(collectionID int64) (*DataView, error) {
		return v2, nil
	}
	NotifyUpdate(collectionID)
	assert.Eventually(t, func() bool {
		version := manager.GetVersion(collectionID)
		return version == v2.Version
	}, 1*time.Second, 10*time.Millisecond)

	// Update due to segments list changed
	v3 := &DataView{
		CollectionID: collectionID,
		Channels: map[string]*datapb.VchannelInfo{"ch0": {
			CollectionID:        collectionID,
			ChannelName:         "ch0",
			UnflushedSegmentIds: []int64{100, 200},
		}},
		Version: time.Now().UnixNano(),
	}
	manager.(*dataViewManager).pullFn = func(collectionID int64) (*DataView, error) {
		return v3, nil
	}
	NotifyUpdate(collectionID)
	assert.Eventually(t, func() bool {
		version := manager.GetVersion(collectionID)
		return version == v3.Version
	}, 1*time.Second, 10*time.Millisecond)

	// Update due to partition stats changed
	v4 := &DataView{
		CollectionID: collectionID,
		Channels: map[string]*datapb.VchannelInfo{"ch0": {
			CollectionID:           collectionID,
			ChannelName:            "ch0",
			UnflushedSegmentIds:    []int64{100, 200},
			PartitionStatsVersions: map[int64]int64{1000: 2000},
		}},
		Version: time.Now().UnixNano(),
	}
	manager.(*dataViewManager).pullFn = func(collectionID int64) (*DataView, error) {
		return v4, nil
	}
	NotifyUpdate(collectionID)
	assert.Eventually(t, func() bool {
		version := manager.GetVersion(collectionID)
		return version == v4.Version
	}, 1*time.Second, 10*time.Millisecond)

	// Update due to segments list changed
	v5 := &DataView{
		CollectionID: collectionID,
		Channels: map[string]*datapb.VchannelInfo{"ch0": {
			CollectionID:           collectionID,
			ChannelName:            "ch0",
			UnflushedSegmentIds:    []int64{100, 200},
			PartitionStatsVersions: map[int64]int64{1000: 2000},
		}},
		Segments: map[int64]struct{}{
			300: {},
		},
		Version: time.Now().UnixNano(),
	}
	manager.(*dataViewManager).pullFn = func(collectionID int64) (*DataView, error) {
		return v5, nil
	}
	NotifyUpdate(collectionID)
	assert.Eventually(t, func() bool {
		version := manager.GetVersion(collectionID)
		return version == v5.Version
	}, 1*time.Second, 10*time.Millisecond)

	// Check force update
	v6 := &DataView{
		CollectionID: collectionID,
		Channels: map[string]*datapb.VchannelInfo{"ch0": {
			CollectionID:           collectionID,
			ChannelName:            "ch0",
			UnflushedSegmentIds:    []int64{100, 200},
			PartitionStatsVersions: map[int64]int64{1000: 2000},
		}},
		Version: time.Now().Add(paramtable.Get().DataCoordCfg.ForceUpdateDataViewInterval.GetAsDuration(time.Second)).UnixNano(),
	}
	manager.(*dataViewManager).pullFn = func(collectionID int64) (*DataView, error) {
		return v6, nil
	}
	NotifyUpdate(collectionID)
	assert.Eventually(t, func() bool {
		version := manager.GetVersion(collectionID)
		return version == v6.Version
	}, 1*time.Second, 10*time.Millisecond)

	// Won't update anymore
	NotifyUpdate(collectionID)
	assert.Never(t, func() bool {
		version := manager.GetVersion(collectionID)
		return version != v6.Version
	}, 100*time.Millisecond, 10*time.Millisecond)
}

func TestNewDataViewManager_TryUpdateDataView_Failed(t *testing.T) {
	manager := NewDataViewManager(nil)
	go manager.Start()
	defer manager.Close()

	collectionID := int64(1)

	manager.(*dataViewManager).pullFn = func(collectionID int64) (*DataView, error) {
		return nil, fmt.Errorf("mock err")
	}
	NotifyUpdate(collectionID)
	assert.Never(t, func() bool {
		version := manager.GetVersion(collectionID)
		return version > InitialDataViewVersion
	}, 100*time.Millisecond, 10*time.Millisecond)
}
