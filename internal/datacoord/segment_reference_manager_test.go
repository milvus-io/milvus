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

package datacoord

import (
	"errors"
	"path"
	"strconv"
	"testing"

	"github.com/milvus-io/milvus/internal/kv"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/stretchr/testify/assert"
)

func Test_SegmentReferenceManager(t *testing.T) {
	var segRefer *SegmentReferenceManager
	var err error
	Params.Init()
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	assert.Nil(t, err)
	etcdKV := etcdkv.NewEtcdKV(etcdCli, "unittest")

	t.Run("NewSegmentReferenceManager", func(t *testing.T) {
		var segRefer *SegmentReferenceManager
		var err error
		var locKey string
		nodeID := int64(1)
		locKey = path.Join(segmentReferPrefix, strconv.FormatInt(nodeID, 10), strconv.FormatInt(2, 10))
		err = etcdKV.Save(locKey, strconv.FormatInt(nodeID, 10))
		assert.NoError(t, err)

		segRefer, err = NewSegmentReferenceManager(etcdKV, []UniqueID{nodeID})
		assert.NoError(t, err)
		assert.NotNil(t, segRefer)
		err = etcdKV.Remove(locKey)
		assert.NoError(t, err)

		locKey = path.Join(segmentReferPrefix, strconv.FormatInt(nodeID, 10), "segID")
		err = etcdKV.Save(locKey, strconv.FormatInt(nodeID, 10))
		assert.NoError(t, err)
		segRefer, err = NewSegmentReferenceManager(etcdKV, []UniqueID{nodeID})
		assert.Error(t, err)
		assert.Nil(t, segRefer)
		err = etcdKV.Remove(locKey)
		assert.NoError(t, err)

		locKey = path.Join(segmentReferPrefix, "nodeID", strconv.FormatInt(3, 10))
		err = etcdKV.Save(locKey, strconv.FormatInt(nodeID, 10))
		assert.NoError(t, err)
		segRefer, err = NewSegmentReferenceManager(etcdKV, []UniqueID{nodeID})
		assert.Error(t, err)
		assert.Nil(t, segRefer)
		err = etcdKV.Remove(locKey)
		assert.NoError(t, err)

		locKey = path.Join(segmentReferPrefix, "nodeID")
		err = etcdKV.Save(locKey, strconv.FormatInt(nodeID, 10))
		assert.NoError(t, err)
		segRefer, err = NewSegmentReferenceManager(etcdKV, nil)
		assert.Error(t, err)
		assert.Nil(t, segRefer)
		err = etcdKV.Remove(locKey)
		assert.NoError(t, err)

		locKey = path.Join(segmentReferPrefix, strconv.FormatInt(nodeID, 10), strconv.FormatInt(2, 10))
		err = etcdKV.Save(locKey, strconv.FormatInt(nodeID, 10))
		assert.NoError(t, err)
		segRefer, err = NewSegmentReferenceManager(etcdKV, nil)
		assert.NoError(t, err)
		assert.NotNil(t, segRefer)
		has := segRefer.HasSegmentLock(2)
		assert.False(t, has)
	})

	segIDs := []UniqueID{1, 2, 3, 4, 5}
	nodeID := UniqueID(1)
	segRefer, err = NewSegmentReferenceManager(etcdKV, nil)
	assert.NoError(t, err)
	assert.NotNil(t, segRefer)
	var has bool

	t.Run("AddSegmentsLock", func(t *testing.T) {
		err = segRefer.AddSegmentsLock(segIDs, nodeID)
		assert.NoError(t, err)

		for _, segID := range segIDs {
			has = segRefer.HasSegmentLock(segID)
			assert.True(t, has)
		}
	})

	t.Run("ReleaseSegmentsLock", func(t *testing.T) {
		err = segRefer.ReleaseSegmentsLock(segIDs, nodeID)
		assert.NoError(t, err)

		for _, segID := range segIDs {
			has = segRefer.HasSegmentLock(segID)
			assert.False(t, has)
		}

		err = segRefer.ReleaseSegmentsLock([]UniqueID{6}, nodeID)
		assert.NoError(t, err)

		has = segRefer.HasSegmentLock(6)
		assert.False(t, has)
	})

	t.Run("ReleaseSegmentsLockByNodeID", func(t *testing.T) {
		segIDs = []UniqueID{10, 11, 12, 13, 14, 15}
		nodeID = 2
		err = segRefer.AddSegmentsLock(segIDs, nodeID)
		assert.NoError(t, err)

		for _, segID := range segIDs {
			has = segRefer.HasSegmentLock(segID)
			assert.True(t, has)
		}

		err = segRefer.ReleaseSegmentsLockByNodeID(UniqueID(2))
		assert.NoError(t, err)

		for _, segID := range segIDs {
			has = segRefer.HasSegmentLock(segID)
			assert.False(t, has)
		}

		err = segRefer.ReleaseSegmentsLockByNodeID(UniqueID(11))
		assert.NoError(t, err)
	})

	t.Run("RecoverySegReferManager", func(t *testing.T) {
		segIDs = []UniqueID{16, 17, 18, 19, 20}
		err = segRefer.AddSegmentsLock(segIDs, UniqueID(3))
		assert.NoError(t, err)

		for _, segID := range segIDs {
			has = segRefer.HasSegmentLock(segID)
			assert.True(t, has)
		}

		segIDs2 := []UniqueID{21, 22, 23, 24, 25}
		err = segRefer.AddSegmentsLock(segIDs2, UniqueID(4))
		assert.NoError(t, err)

		for _, segID := range segIDs2 {
			has = segRefer.HasSegmentLock(segID)
			assert.True(t, has)
		}

		err = segRefer.recoverySegReferManager([]int64{4, 5})
		assert.NoError(t, err)

		for _, segID := range segIDs {
			has = segRefer.HasSegmentLock(segID)
			assert.False(t, has)
		}

		err = segRefer.ReleaseSegmentsLockByNodeID(4)
		assert.NoError(t, err)

		for _, segID := range segIDs2 {
			has = segRefer.HasSegmentLock(segID)
			assert.False(t, has)
		}
	})

	t.Run("HasSegmentLock", func(t *testing.T) {
		exist := segRefer.HasSegmentLock(UniqueID(1))
		assert.False(t, exist)
	})

	t.Run("GetHasReferLockSegmentIDs", func(t *testing.T) {
		segIDs = []UniqueID{26, 27, 28, 29, 30}
		err = segRefer.AddSegmentsLock(segIDs, UniqueID(5))
		assert.NoError(t, err)

		for _, segID := range segIDs {
			has = segRefer.HasSegmentLock(segID)
			assert.True(t, has)
		}

		segmentIDs := segRefer.GetHasReferLockSegmentIDs()
		assert.Equal(t, 5, len(segmentIDs))
		assert.ElementsMatch(t, segIDs, segmentIDs)

		err = segRefer.ReleaseSegmentsLockByNodeID(UniqueID(5))
		assert.NoError(t, err)

		for _, segID := range segIDs {
			has = segRefer.HasSegmentLock(segID)
			assert.False(t, has)
		}

		segIDs = segRefer.GetHasReferLockSegmentIDs()
		assert.Equal(t, 0, len(segIDs))
	})
}

type etcdKVMock struct {
	kv.BaseKV

	Fail int
}

func (em *etcdKVMock) MultiSave(data map[string]string) error {
	if em.Fail > 0 {
		return errors.New("error occurred")
	}
	return nil
}

func (em *etcdKVMock) MultiRemove(keys []string) error {
	if em.Fail > 0 {
		return errors.New("error occurred")
	}
	return nil
}

func (em *etcdKVMock) LoadWithPrefix(prefix string) ([]string, []string, error) {
	if em.Fail > 2 {
		return nil, nil, errors.New("error occurs")
	}
	if em.Fail > 1 {
		return []string{"key"}, []string{"value"}, nil
	}
	return []string{"meta/segmentRefer/1/2"}, []string{"1"}, nil
}

func TestSegmentReferenceManager_Error(t *testing.T) {
	emKV := &etcdKVMock{
		Fail: 3,
	}

	t.Run("NewSegmentReferenceManager", func(t *testing.T) {
		segRefer, err := NewSegmentReferenceManager(emKV, nil)
		assert.Error(t, err)
		assert.Nil(t, segRefer)

		emKV2 := &etcdKVMock{Fail: 2}
		segRefer, err = NewSegmentReferenceManager(emKV2, nil)
		assert.Error(t, err)
		assert.Nil(t, segRefer)

		emKV3 := &etcdKVMock{Fail: 1}
		segRefer, err = NewSegmentReferenceManager(emKV3, nil)
		assert.Error(t, err)
		assert.Nil(t, segRefer)
	})

	segRefer := &SegmentReferenceManager{
		etcdKV: emKV,
	}

	t.Run("AddSegmentsLock", func(t *testing.T) {
		err := segRefer.AddSegmentsLock([]UniqueID{1}, 1)
		assert.Error(t, err)
	})

	t.Run("ReleaseSegmentsLock", func(t *testing.T) {
		err := segRefer.ReleaseSegmentsLock([]UniqueID{1}, 1)
		assert.Error(t, err)
	})

	t.Run("ReleaseSegmentsLockByNodeID", func(t *testing.T) {
		err := segRefer.ReleaseSegmentsLockByNodeID(1)
		assert.Error(t, err)
	})

	t.Run("recoverySegReferManager", func(t *testing.T) {
		segRefer.segmentsLock = map[UniqueID][]*SegmentLock{
			2: {
				{
					segmentID: 2,
					nodeID:    2,
					locKey:    "1/2/3",
				},
			},
		}
		err := segRefer.recoverySegReferManager([]UniqueID{1})
		assert.Error(t, err)
	})
}
