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
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/mocks"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/kv/predicates"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var (
	logID        = int64(99)
	collectionID = int64(2)
	partitionID  = int64(1)
	segmentID    = int64(1)
	segmentID2   = int64(11)
	fieldID      = int64(1)
	rootPath     = "a"

	k1 = buildFieldBinlogPath(collectionID, partitionID, segmentID, fieldID)
	k2 = buildFieldDeltalogPath(collectionID, partitionID, segmentID, fieldID)
	k3 = buildFieldStatslogPath(collectionID, partitionID, segmentID, fieldID)
	k4 = buildSegmentPath(collectionID, partitionID, segmentID2)
	k5 = buildSegmentPath(collectionID, partitionID, segmentID)
	k6 = buildFieldBinlogPath(collectionID, partitionID, segmentID2, fieldID)
	k7 = buildFieldDeltalogPath(collectionID, partitionID, segmentID2, fieldID)
	k8 = buildFieldStatslogPath(collectionID, partitionID, segmentID2, fieldID)

	keys = map[string]struct{}{
		k1: {},
		k2: {},
		k3: {},
		k4: {},
		k5: {},
		k6: {},
		k7: {},
		k8: {},
	}

	invalidSegment = &datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		PartitionID:  partitionID,
		NumOfRows:    100,
		State:        commonpb.SegmentState_Flushed,
		Binlogs: []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 5,
						LogPath:    "badpath",
					},
				},
			},
		},
	}

	binlogs = []*datapb.FieldBinlog{
		{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{
				{
					EntriesNum: 5,
					LogID:      logID,
				},
			},
		},
	}

	deltalogs = []*datapb.FieldBinlog{
		{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{
				{
					EntriesNum: 5,
					LogID:      logID,
				},
			},
		},
	}
	statslogs = []*datapb.FieldBinlog{
		{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{
				{
					EntriesNum: 5,
					LogID:      logID,
				},
			},
		},
	}

	getlogs = func(id int64) []*datapb.FieldBinlog {
		return []*datapb.FieldBinlog{
			{
				FieldID: 1,
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 5,
						LogID:      id,
					},
				},
			},
		}
	}

	segment1 = &datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		PartitionID:  partitionID,
		NumOfRows:    100,
		State:        commonpb.SegmentState_Flushed,
		Binlogs:      binlogs,
		Deltalogs:    deltalogs,
		Statslogs:    statslogs,
	}

	droppedSegment = &datapb.SegmentInfo{
		ID:           segmentID2,
		CollectionID: collectionID,
		PartitionID:  partitionID,
		NumOfRows:    100,
		State:        commonpb.SegmentState_Dropped,
		Binlogs:      getlogs(logID),
		Deltalogs:    getlogs(logID),
		Statslogs:    getlogs(logID),
	}
)

func Test_ListSegments(t *testing.T) {
	t.Run("load failed", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything).Return(errors.New("error"))

		catalog := NewCatalog(metakv, rootPath, "")
		ret, err := catalog.ListSegments(context.TODO())
		assert.Nil(t, ret)
		assert.Error(t, err)
	})

	verifySegments := func(t *testing.T, logID int64, ret []*datapb.SegmentInfo) {
		assert.Equal(t, 1, len(ret))
		segment := ret[0]
		assert.Equal(t, segmentID, segment.ID)
		assert.Equal(t, collectionID, segment.CollectionID)
		assert.Equal(t, partitionID, segment.PartitionID)

		assert.Equal(t, 1, len(segment.Binlogs))
		assert.Equal(t, fieldID, segment.Binlogs[0].FieldID)
		assert.Equal(t, 1, len(segment.Binlogs[0].Binlogs))
		assert.Equal(t, logID, segment.Binlogs[0].Binlogs[0].LogID)
		// set log path to empty and only store log id
		assert.Equal(t, "", segment.Binlogs[0].Binlogs[0].LogPath)

		assert.Equal(t, 1, len(segment.Deltalogs))
		assert.Equal(t, fieldID, segment.Deltalogs[0].FieldID)
		assert.Equal(t, 1, len(segment.Deltalogs[0].Binlogs))
		assert.Equal(t, logID, segment.Deltalogs[0].Binlogs[0].LogID)
		// set log path to empty and only store log id
		assert.Equal(t, "", segment.Deltalogs[0].Binlogs[0].LogPath)

		assert.Equal(t, 1, len(segment.Statslogs))
		assert.Equal(t, fieldID, segment.Statslogs[0].FieldID)
		assert.Equal(t, 1, len(segment.Statslogs[0].Binlogs))
		assert.Equal(t, logID, segment.Statslogs[0].Binlogs[0].LogID)
		// set log path to empty and only store log id
		assert.Equal(t, "", segment.Statslogs[0].Binlogs[0].LogPath)
	}

	t.Run("test compatibility", func(t *testing.T) {
		segBytes, err := proto.Marshal(segment1)
		assert.NoError(t, err)

		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(s string, i int, f func([]byte, []byte) error) error {
			if strings.HasPrefix(k5, s) {
				return f([]byte(k5), segBytes)
			}
			return nil
		})

		catalog := NewCatalog(metakv, rootPath, "")
		ret, err := catalog.ListSegments(context.TODO())
		assert.NotNil(t, ret)
		assert.NoError(t, err)

		verifySegments(t, logID, ret)
	})

	t.Run("list successfully", func(t *testing.T) {
		var savedKvs map[string]string

		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().MultiSave(mock.Anything).RunAndReturn(func(m map[string]string) error {
			savedKvs = m
			return nil
		})

		catalog := NewCatalog(metakv, rootPath, "")
		err := catalog.AddSegment(context.TODO(), segment1)
		assert.NoError(t, err)

		metakv.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(s string, i int, f func([]byte, []byte) error) error {
			if strings.HasPrefix(k5, s) {
				return f([]byte(k5), []byte(savedKvs[k5]))
			}

			if strings.HasPrefix(k1, s) {
				return f([]byte(k1), []byte(savedKvs[k1]))
			}

			if strings.HasPrefix(k2, s) {
				return f([]byte(k2), []byte(savedKvs[k2]))
			}
			if strings.HasPrefix(k3, s) {
				return f([]byte(k3), []byte(savedKvs[k3]))
			}
			return errors.New("should not reach here")
		})

		ret, err := catalog.ListSegments(context.TODO())
		assert.NotNil(t, ret)
		assert.NoError(t, err)

		verifySegments(t, logID, ret)
	})
}

func Test_AddSegments(t *testing.T) {
	t.Run("generate binlog kvs failed", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().MultiSave(mock.Anything).Return(errors.New("error")).Maybe()

		catalog := NewCatalog(metakv, rootPath, "")

		err := catalog.AddSegment(context.TODO(), invalidSegment)
		assert.Error(t, err)
	})

	t.Run("save error", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().MultiSave(mock.Anything).Return(errors.New("error"))

		catalog := NewCatalog(metakv, rootPath, "")
		err := catalog.AddSegment(context.TODO(), segment1)
		assert.Error(t, err)
	})

	t.Run("save successfully", func(t *testing.T) {
		savedKvs := make(map[string]string)
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().MultiSave(mock.Anything).RunAndReturn(func(m map[string]string) error {
			savedKvs = m
			return nil
		})
		metakv.EXPECT().Load(mock.Anything).RunAndReturn(func(s string) (string, error) {
			if v, ok := savedKvs[s]; ok {
				return v, nil
			}
			return "", errors.New("key not found")
		})

		catalog := NewCatalog(metakv, rootPath, "")
		err := catalog.AddSegment(context.TODO(), segment1)
		assert.NoError(t, err)
		adjustedSeg, err := catalog.LoadFromSegmentPath(segment1.CollectionID, segment1.PartitionID, segment1.ID)
		assert.NoError(t, err)
		// Check that num of rows is corrected from 100 to 5.
		assert.Equal(t, int64(100), segment1.GetNumOfRows())
		assert.Equal(t, int64(5), adjustedSeg.GetNumOfRows())

		_, ok := savedKvs[k4]
		assert.False(t, ok)
		assert.Equal(t, 4, len(savedKvs))
		verifySavedKvsForSegment(t, savedKvs)
	})

	t.Run("no need to store log path", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		catalog := NewCatalog(metakv, rootPath, "")

		validFieldBinlog := []*datapb.FieldBinlog{{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{
				{
					LogID:   1,
					LogPath: "",
				},
			},
		}}

		invalidFieldBinlog := []*datapb.FieldBinlog{{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{
				{
					LogID:   1,
					LogPath: "no need to store",
				},
			},
		}}

		segment := &datapb.SegmentInfo{
			ID:           segmentID,
			CollectionID: collectionID,
			PartitionID:  partitionID,
			NumOfRows:    100,
			State:        commonpb.SegmentState_Flushed,
		}

		segment.Statslogs = invalidFieldBinlog
		err := catalog.AddSegment(context.TODO(), segment)
		assert.Error(t, err)
		segment.Statslogs = validFieldBinlog

		segment.Binlogs = invalidFieldBinlog
		err = catalog.AddSegment(context.TODO(), segment)
		assert.Error(t, err)
		segment.Binlogs = validFieldBinlog

		segment.Deltalogs = invalidFieldBinlog
		err = catalog.AddSegment(context.TODO(), segment)
		assert.Error(t, err)
	})
}

func Test_AlterSegments(t *testing.T) {
	t.Run("generate binlog kvs failed", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().MultiSave(mock.Anything).Return(errors.New("error")).Maybe()

		catalog := NewCatalog(metakv, rootPath, "")
		err := catalog.AlterSegments(context.TODO(), []*datapb.SegmentInfo{invalidSegment}, metastore.BinlogsIncrement{
			Segment: invalidSegment,
		})
		assert.Error(t, err)
	})

	t.Run("save error", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().MultiSave(mock.Anything).Return(errors.New("error"))

		catalog := NewCatalog(metakv, rootPath, "")
		err := catalog.AlterSegments(context.TODO(), []*datapb.SegmentInfo{segment1})

		assert.Error(t, err)
	})

	t.Run("save successfully", func(t *testing.T) {
		var savedKvs map[string]string
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().MultiSave(mock.Anything).RunAndReturn(func(m map[string]string) error {
			savedKvs = m
			return nil
		})

		catalog := NewCatalog(metakv, rootPath, "")
		err := catalog.AlterSegments(context.TODO(), []*datapb.SegmentInfo{})
		assert.NoError(t, err)

		err = catalog.AlterSegments(context.TODO(), []*datapb.SegmentInfo{segment1}, metastore.BinlogsIncrement{
			Segment: segment1,
		})
		assert.NoError(t, err)

		_, ok := savedKvs[k4]
		assert.False(t, ok)
		assert.Equal(t, 4, len(savedKvs))
		verifySavedKvsForSegment(t, savedKvs)
	})

	t.Run("save large ops successfully", func(t *testing.T) {
		savedKvs := make(map[string]string)
		opGroupCount := 0
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().MultiSave(mock.Anything).RunAndReturn(func(m map[string]string) error {
			maps.Copy(savedKvs, m)
			opGroupCount++
			return nil
		})
		metakv.EXPECT().Load(mock.Anything).RunAndReturn(func(s string) (string, error) {
			if v, ok := savedKvs[s]; ok {
				return v, nil
			}
			return "", errors.New("key not found")
		})

		catalog := NewCatalog(metakv, rootPath, "")
		err := catalog.AlterSegments(context.TODO(), []*datapb.SegmentInfo{})
		assert.NoError(t, err)

		var binlogXL []*datapb.FieldBinlog
		for i := 0; i < 255; i++ {
			binlogXL = append(binlogXL, &datapb.FieldBinlog{
				FieldID: int64(i),
				Binlogs: []*datapb.Binlog{
					{
						EntriesNum: 5,
						LogID:      logID,
					},
				},
			})
		}

		segmentXL := &datapb.SegmentInfo{
			ID:           segmentID,
			CollectionID: collectionID,
			PartitionID:  partitionID,
			NumOfRows:    100,
			State:        commonpb.SegmentState_Flushed,
			Binlogs:      binlogXL,
			Deltalogs:    deltalogs,
			Statslogs:    statslogs,
		}

		err = catalog.AlterSegments(context.TODO(), []*datapb.SegmentInfo{segmentXL},
			metastore.BinlogsIncrement{
				Segment: segmentXL,
			})
		assert.NoError(t, err)
		assert.Equal(t, 255+3, len(savedKvs))
		assert.Equal(t, 3, opGroupCount)

		adjustedSeg, err := catalog.LoadFromSegmentPath(segmentXL.CollectionID, segmentXL.PartitionID, segmentXL.ID)
		assert.NoError(t, err)
		// Check that num of rows is corrected from 100 to 1275.
		assert.Equal(t, int64(100), segmentXL.GetNumOfRows())
		assert.Equal(t, int64(5), adjustedSeg.GetNumOfRows())
	})

	t.Run("invalid log id", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		catalog := NewCatalog(metakv, rootPath, "")

		segment := &datapb.SegmentInfo{
			ID:           segmentID,
			CollectionID: collectionID,
			PartitionID:  partitionID,
			NumOfRows:    100,
			State:        commonpb.SegmentState_Flushed,
		}

		invalidLogWithZeroLogID := []*datapb.FieldBinlog{{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{
				{
					LogID:   0,
					LogPath: "mock_log_path",
				},
			},
		}}

		segment.Statslogs = invalidLogWithZeroLogID
		err := catalog.AlterSegments(context.TODO(), []*datapb.SegmentInfo{segment}, metastore.BinlogsIncrement{Segment: segment})
		assert.Error(t, err)
		t.Logf("%v", err)

		segment.Deltalogs = invalidLogWithZeroLogID
		err = catalog.AlterSegments(context.TODO(), []*datapb.SegmentInfo{segment}, metastore.BinlogsIncrement{Segment: segment})
		assert.Error(t, err)
		t.Logf("%v", err)

		segment.Binlogs = invalidLogWithZeroLogID
		err = catalog.AlterSegments(context.TODO(), []*datapb.SegmentInfo{segment}, metastore.BinlogsIncrement{Segment: segment})
		assert.Error(t, err)
		t.Logf("%v", err)
	})
}

func Test_DropSegment(t *testing.T) {
	t.Run("remove failed", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().MultiSaveAndRemoveWithPrefix(mock.Anything, mock.Anything).Return(errors.New("error"))

		catalog := NewCatalog(metakv, rootPath, "")
		err := catalog.DropSegment(context.TODO(), segment1)
		assert.Error(t, err)
	})

	t.Run("remove successfully", func(t *testing.T) {
		removedKvs := make(map[string]struct{}, 0)
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().MultiSaveAndRemoveWithPrefix(mock.Anything, mock.Anything).RunAndReturn(func(m map[string]string, s []string, p ...predicates.Predicate) error {
			for _, key := range s {
				removedKvs[key] = struct{}{}
			}
			return nil
		})

		catalog := NewCatalog(metakv, rootPath, "")
		err := catalog.DropSegment(context.TODO(), segment1)
		assert.NoError(t, err)

		segKey := buildSegmentPath(segment1.GetCollectionID(), segment1.GetPartitionID(), segment1.GetID())
		binlogPreix := fmt.Sprintf("%s/%d/%d/%d", SegmentBinlogPathPrefix, segment1.GetCollectionID(), segment1.GetPartitionID(), segment1.GetID())
		deltalogPreix := fmt.Sprintf("%s/%d/%d/%d", SegmentDeltalogPathPrefix, segment1.GetCollectionID(), segment1.GetPartitionID(), segment1.GetID())
		statelogPreix := fmt.Sprintf("%s/%d/%d/%d", SegmentStatslogPathPrefix, segment1.GetCollectionID(), segment1.GetPartitionID(), segment1.GetID())

		assert.Equal(t, 4, len(removedKvs))
		for _, k := range []string{segKey, binlogPreix, deltalogPreix, statelogPreix} {
			_, ok := removedKvs[k]
			assert.True(t, ok)
		}
	})
}

func Test_SaveDroppedSegmentsInBatch_SaveError(t *testing.T) {
	txn := mocks.NewMetaKv(t)
	txn.EXPECT().MultiSave(mock.Anything).Return(errors.New("mock error"))

	catalog := NewCatalog(txn, rootPath, "")
	segments := []*datapb.SegmentInfo{
		{
			ID:           1,
			CollectionID: 1000,
		},
	}
	err := catalog.SaveDroppedSegmentsInBatch(context.TODO(), segments)
	assert.Error(t, err)
}

func Test_SaveDroppedSegmentsInBatch_MultiSave(t *testing.T) {
	var (
		count  = 0
		kvSize = 0
	)
	txn := mocks.NewMetaKv(t)
	txn.EXPECT().
		MultiSave(mock.Anything).
		Run(func(kvs map[string]string) {
			count++
			kvSize += len(kvs)
		}).
		Return(nil)

	catalog := NewCatalog(txn, rootPath, "")

	// no segments
	{
		var segments []*datapb.SegmentInfo
		err := catalog.SaveDroppedSegmentsInBatch(context.TODO(), segments)
		assert.NoError(t, err)
		assert.Equal(t, 0, count)
		assert.Equal(t, 0, kvSize)
	}

	// testing for no splitting
	{
		segments1 := []*datapb.SegmentInfo{
			{
				ID:           1,
				CollectionID: 1000,
				PartitionID:  100,
			},
		}

		err := catalog.SaveDroppedSegmentsInBatch(context.TODO(), segments1)
		assert.NoError(t, err)
		assert.Equal(t, 1, count)
		assert.Equal(t, 1, kvSize)
	}

	// testing for reaching max operation
	{
		segments2 := make([]*datapb.SegmentInfo, 129)
		for i := 0; i < 129; i++ {
			segments2[i] = &datapb.SegmentInfo{
				ID:           int64(i),
				CollectionID: 1000,
				PartitionID:  100,
			}
		}

		count = 0
		kvSize = 0
		err := catalog.SaveDroppedSegmentsInBatch(context.TODO(), segments2)
		assert.NoError(t, err)
		assert.Equal(t, 2, count)
		assert.Equal(t, 129, kvSize)
	}
}

func TestChannelCP(t *testing.T) {
	mockVChannel := "fake-by-dev-rootcoord-dml-1-testchannelcp-v0"
	mockPChannel := "fake-by-dev-rootcoord-dml-1"

	pos := &msgpb.MsgPosition{
		ChannelName: mockPChannel,
		MsgID:       []byte{},
		Timestamp:   1000,
	}
	k := buildChannelCPKey(mockVChannel)
	v, err := proto.Marshal(pos)
	assert.NoError(t, err)

	t.Run("ListChannelCheckpoint", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().Save(mock.Anything, mock.Anything).Return(nil)
		catalog := NewCatalog(txn, rootPath, "")
		err := catalog.SaveChannelCheckpoint(context.TODO(), mockVChannel, pos)
		assert.NoError(t, err)

		txn.EXPECT().LoadWithPrefix(mock.Anything).Return([]string{k}, []string{string(v)}, nil)
		res, err := catalog.ListChannelCheckpoint(context.TODO())
		assert.NoError(t, err)
		assert.True(t, len(res) > 0)
	})

	t.Run("ListChannelCheckpoint failed", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		catalog := NewCatalog(txn, rootPath, "")
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, errors.New("mock error"))
		_, err = catalog.ListChannelCheckpoint(context.TODO())
		assert.Error(t, err)
	})

	t.Run("SaveChannelCheckpoint", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().Save(mock.Anything, mock.Anything).Return(nil)
		catalog := NewCatalog(txn, rootPath, "")
		err := catalog.SaveChannelCheckpoint(context.TODO(), mockVChannel, pos)
		assert.NoError(t, err)
	})

	t.Run("SaveChannelCheckpoint failed", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		catalog := NewCatalog(txn, rootPath, "")
		txn.EXPECT().Save(mock.Anything, mock.Anything).Return(errors.New("mock error"))
		err = catalog.SaveChannelCheckpoint(context.TODO(), mockVChannel, &msgpb.MsgPosition{})
		assert.Error(t, err)
	})

	t.Run("SaveChannelCheckpoints", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().MultiSave(mock.Anything).Return(nil)
		catalog := NewCatalog(txn, rootPath, "")
		err := catalog.SaveChannelCheckpoints(context.TODO(), []*msgpb.MsgPosition{pos})
		assert.NoError(t, err)
	})

	t.Run("SaveChannelCheckpoints failed", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		catalog := NewCatalog(txn, rootPath, "")
		txn.EXPECT().MultiSave(mock.Anything).Return(errors.New("mock error"))
		err = catalog.SaveChannelCheckpoints(context.TODO(), []*msgpb.MsgPosition{pos})
		assert.Error(t, err)
	})

	t.Run("DropChannelCheckpoint", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().Save(mock.Anything, mock.Anything).Return(nil)
		catalog := NewCatalog(txn, rootPath, "")
		err := catalog.SaveChannelCheckpoint(context.TODO(), mockVChannel, pos)
		assert.NoError(t, err)

		txn.EXPECT().Remove(mock.Anything).Return(nil)
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, nil)
		err = catalog.DropChannelCheckpoint(context.TODO(), mockVChannel)
		assert.NoError(t, err)
		res, err := catalog.ListChannelCheckpoint(context.TODO())
		assert.NoError(t, err)
		assert.True(t, len(res) == 0)
	})

	t.Run("DropChannelCheckpoint failed", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		catalog := NewCatalog(txn, rootPath, "")
		txn.EXPECT().Remove(mock.Anything).Return(errors.New("mock error"))
		err = catalog.DropChannelCheckpoint(context.TODO(), mockVChannel)
		assert.Error(t, err)
	})
}

func Test_MarkChannelDeleted_SaveError(t *testing.T) {
	txn := mocks.NewMetaKv(t)
	txn.EXPECT().
		Save(mock.Anything, mock.Anything).
		Return(errors.New("mock error"))

	catalog := NewCatalog(txn, rootPath, "")
	err := catalog.MarkChannelDeleted(context.TODO(), "test_channel_1")
	assert.Error(t, err)
}

func Test_MarkChannelAdded_SaveError(t *testing.T) {
	txn := mocks.NewMetaKv(t)
	txn.EXPECT().
		Save(mock.Anything, mock.Anything).
		Return(errors.New("mock error"))

	catalog := NewCatalog(txn, rootPath, "")
	err := catalog.MarkChannelAdded(context.TODO(), "test_channel_1")
	assert.Error(t, err)
}

func Test_ChannelExists_SaveError(t *testing.T) {
	txn := mocks.NewMetaKv(t)
	txn.EXPECT().
		Load(mock.Anything).
		Return("", errors.New("mock error"))

	catalog := NewCatalog(txn, rootPath, "")
	assert.False(t, catalog.ChannelExists(context.TODO(), "test_channel_1"))
}

func Test_parseBinlogKey(t *testing.T) {
	catalog := NewCatalog(nil, "", "")

	t.Run("parse collection id fail", func(t *testing.T) {
		ret1, ret2, ret3, err := catalog.parseBinlogKey("root/err/1/1/1", 5)
		assert.Error(t, err)
		assert.Equal(t, int64(0), ret1)
		assert.Equal(t, int64(0), ret2)
		assert.Equal(t, int64(0), ret3)
	})

	t.Run("parse partition id fail", func(t *testing.T) {
		ret1, ret2, ret3, err := catalog.parseBinlogKey("root/1/err/1/1", 5)
		assert.Error(t, err)
		assert.Equal(t, int64(0), ret1)
		assert.Equal(t, int64(0), ret2)
		assert.Equal(t, int64(0), ret3)
	})

	t.Run("parse segment id fail", func(t *testing.T) {
		ret1, ret2, ret3, err := catalog.parseBinlogKey("root/1/1/err/1", 5)
		assert.Error(t, err)
		assert.Equal(t, int64(0), ret1)
		assert.Equal(t, int64(0), ret2)
		assert.Equal(t, int64(0), ret3)
	})

	t.Run("miss field", func(t *testing.T) {
		ret1, ret2, ret3, err := catalog.parseBinlogKey("root/1/1/", 5)
		assert.Error(t, err)
		assert.Equal(t, int64(0), ret1)
		assert.Equal(t, int64(0), ret2)
		assert.Equal(t, int64(0), ret3)
	})

	t.Run("test ok", func(t *testing.T) {
		ret1, ret2, ret3, err := catalog.parseBinlogKey("root/1/1/1/1", 5)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), ret1)
		assert.Equal(t, int64(1), ret2)
		assert.Equal(t, int64(1), ret3)
	})
}

func verifyBinlogs(t *testing.T, binlogBytes []byte) {
	binlogs := &datapb.FieldBinlog{}
	err := proto.Unmarshal(binlogBytes, binlogs)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(binlogs.Binlogs))
	assert.Equal(t, int64(99), binlogs.Binlogs[0].GetLogID())
	assert.Equal(t, "", binlogs.Binlogs[0].GetLogPath())
}

func verifySegmentInfo(t *testing.T, segmentInfoBytes []byte) {
	segmentInfo := &datapb.SegmentInfo{}
	err := proto.Unmarshal(segmentInfoBytes, segmentInfo)
	assert.NoError(t, err)
	assert.Nil(t, segmentInfo.Binlogs)
	assert.Nil(t, segmentInfo.Binlogs)
	assert.Nil(t, segmentInfo.Deltalogs)
	assert.Equal(t, collectionID, segmentInfo.CollectionID)
	assert.Equal(t, segmentID, segmentInfo.ID)
}

func verifySavedKvsForSegment(t *testing.T, savedKvs map[string]string) {
	for k := range savedKvs {
		_, ok := keys[k]
		assert.True(t, ok)
	}

	for _, k := range []string{k1, k2, k3} {
		ret, ok := savedKvs[k]
		assert.True(t, ok)
		verifyBinlogs(t, []byte(ret))
	}

	ret, ok := savedKvs[k5]
	assert.True(t, ok)
	verifySegmentInfo(t, []byte(ret))
}

func verifySegmentInfo2(t *testing.T, segmentInfoBytes []byte) {
	segmentInfo := &datapb.SegmentInfo{}
	err := proto.Unmarshal(segmentInfoBytes, segmentInfo)
	assert.NoError(t, err)
	assert.Nil(t, segmentInfo.Binlogs)
	assert.Nil(t, segmentInfo.Binlogs)
	assert.Nil(t, segmentInfo.Deltalogs)
	assert.Equal(t, collectionID, segmentInfo.CollectionID)
	assert.Equal(t, segmentID2, segmentInfo.ID)
}

func verifySavedKvsForDroppedSegment(t *testing.T, savedKvs map[string]string) {
	for k := range savedKvs {
		_, ok := keys[k]
		assert.True(t, ok)
	}

	for _, k := range []string{k6, k7, k8} {
		ret, ok := savedKvs[k]
		assert.True(t, ok)
		verifyBinlogs(t, []byte(ret))
	}

	ret, ok := savedKvs[k4]
	assert.True(t, ok)
	verifySegmentInfo2(t, []byte(ret))
}

func TestCatalog_CreateIndex(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().Save(mock.Anything, mock.Anything).Return(nil)

		catalog := &Catalog{
			MetaKv: metakv,
		}

		err := catalog.CreateIndex(context.Background(), &model.Index{})
		assert.NoError(t, err)
	})

	t.Run("failed", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().Save(mock.Anything, mock.Anything).Return(errors.New("error"))

		catalog := &Catalog{
			MetaKv: metakv,
		}

		err := catalog.CreateIndex(context.Background(), &model.Index{})
		assert.Error(t, err)
	})
}

func TestCatalog_ListIndexes(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().LoadWithPrefix(mock.Anything).RunAndReturn(func(s string) ([]string, []string, error) {
			i := &indexpb.FieldIndex{
				IndexInfo: &indexpb.IndexInfo{
					CollectionID: 0,
					FieldID:      0,
					IndexName:    "",
					IndexID:      0,
					TypeParams:   nil,
					IndexParams:  nil,
				},
				Deleted:    false,
				CreateTime: 0,
			}
			v, err := proto.Marshal(i)
			assert.NoError(t, err)
			return []string{"1"}, []string{string(v)}, nil
		})

		catalog := &Catalog{
			MetaKv: metakv,
		}
		indexes, err := catalog.ListIndexes(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(indexes))
	})

	t.Run("failed", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, errors.New("error"))
		catalog := &Catalog{
			MetaKv: txn,
		}
		_, err := catalog.ListIndexes(context.Background())
		assert.Error(t, err)
	})

	t.Run("unmarshal failed", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return([]string{"1"}, []string{"invalid"}, nil)

		catalog := &Catalog{
			MetaKv: txn,
		}
		_, err := catalog.ListIndexes(context.Background())
		assert.Error(t, err)
	})
}

func TestCatalog_AlterIndexes(t *testing.T) {
	i := &model.Index{
		CollectionID: 0,
		FieldID:      0,
		IndexID:      0,
		IndexName:    "",
		IsDeleted:    false,
		CreateTime:   0,
		TypeParams:   nil,
		IndexParams:  nil,
	}

	metakv := mocks.NewMetaKv(t)
	metakv.EXPECT().MultiSave(mock.Anything).Return(nil)
	catalog := &Catalog{
		MetaKv: metakv,
	}

	err := catalog.AlterIndexes(context.Background(), []*model.Index{i})
	assert.NoError(t, err)
}

func TestCatalog_AlterSegmentMultiIndexes(t *testing.T) {
	segIdx := make([]*model.Index, 100)
	for i := 0; i < 100; i++ {
		segIdx[i] = &model.Index{
			CollectionID: 0,
			FieldID:      0,
			IndexID:      int64(i),
			IndexName:    "",
			IsDeleted:    false,
			CreateTime:   0,
			TypeParams:   nil,
			IndexParams:  nil,
		}
	}

	t.Run("batchAdd", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)

		metakv.EXPECT().MultiSave(mock.Anything).RunAndReturn(func(m map[string]string) error {
			if len(m) > 64 {
				return errors.New("fail")
			}
			return nil
		})

		metakv.EXPECT().MultiSave(mock.Anything).Return(nil)
		catalog := &Catalog{
			MetaKv: metakv,
		}
		err := catalog.AlterIndexes(context.Background(), segIdx)
		fmt.Println(err)
		assert.NoError(t, err)
	})
}

func TestCatalog_DropIndex(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().Remove(mock.Anything).Return(nil)
		catalog := &Catalog{
			MetaKv: metakv,
		}

		err := catalog.DropIndex(context.Background(), 0, 0)
		assert.NoError(t, err)
	})

	t.Run("failed", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().Remove(mock.Anything).Return(errors.New("error"))
		catalog := &Catalog{
			MetaKv: metakv,
		}

		err := catalog.DropIndex(context.Background(), 0, 0)
		assert.Error(t, err)
	})
}

func TestCatalog_CreateSegmentIndex(t *testing.T) {
	segIdx := &model.SegmentIndex{
		SegmentID:     1,
		CollectionID:  2,
		PartitionID:   3,
		NumRows:       1024,
		IndexID:       4,
		BuildID:       5,
		NodeID:        6,
		IndexState:    commonpb.IndexState_Finished,
		FailReason:    "",
		IndexVersion:  0,
		IsDeleted:     false,
		CreateTime:    0,
		IndexFileKeys: nil,
		IndexSize:     0,
	}

	t.Run("success", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().Save(mock.Anything, mock.Anything).Return(nil)
		catalog := &Catalog{
			MetaKv: metakv,
		}

		err := catalog.CreateSegmentIndex(context.Background(), segIdx)
		assert.NoError(t, err)
	})

	t.Run("failed", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().Save(mock.Anything, mock.Anything).Return(errors.New("error"))
		catalog := &Catalog{
			MetaKv: metakv,
		}

		err := catalog.CreateSegmentIndex(context.Background(), segIdx)
		assert.Error(t, err)
	})
}

func TestCatalog_ListSegmentIndexes(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		segIdx := &indexpb.SegmentIndex{
			CollectionID:  0,
			PartitionID:   0,
			SegmentID:     0,
			NumRows:       0,
			IndexID:       0,
			BuildID:       0,
			NodeID:        0,
			IndexVersion:  0,
			State:         0,
			FailReason:    "",
			IndexFileKeys: nil,
			Deleted:       false,
			CreateTime:    0,
			SerializeSize: 0,
		}
		v, err := proto.Marshal(segIdx)
		assert.NoError(t, err)

		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().LoadWithPrefix(mock.Anything).Return([]string{"key"}, []string{string(v)}, nil)
		catalog := &Catalog{
			MetaKv: metakv,
		}

		segIdxes, err := catalog.ListSegmentIndexes(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(segIdxes))
	})

	t.Run("failed", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().LoadWithPrefix(mock.Anything).Return([]string{}, []string{}, errors.New("error"))
		catalog := &Catalog{
			MetaKv: metakv,
		}

		_, err := catalog.ListSegmentIndexes(context.Background())
		assert.Error(t, err)
	})

	t.Run("unmarshal failed", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().LoadWithPrefix(mock.Anything).Return([]string{"key"}, []string{"invalid"}, nil)
		catalog := &Catalog{
			MetaKv: metakv,
		}

		_, err := catalog.ListSegmentIndexes(context.Background())
		assert.Error(t, err)
	})
}

func TestCatalog_AlterSegmentIndexes(t *testing.T) {
	segIdx := &model.SegmentIndex{
		SegmentID:     0,
		CollectionID:  0,
		PartitionID:   0,
		NumRows:       0,
		IndexID:       0,
		BuildID:       0,
		NodeID:        0,
		IndexState:    0,
		FailReason:    "",
		IndexVersion:  0,
		IsDeleted:     false,
		CreateTime:    0,
		IndexFileKeys: nil,
		IndexSize:     0,
	}

	t.Run("add", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().MultiSave(mock.Anything).Return(nil)
		catalog := &Catalog{
			MetaKv: metakv,
		}

		err := catalog.AlterSegmentIndexes(context.Background(), []*model.SegmentIndex{segIdx})
		assert.NoError(t, err)
	})
}

func TestCatalog_DropSegmentIndex(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().Remove(mock.Anything).Return(nil)
		catalog := &Catalog{
			MetaKv: metakv,
		}

		err := catalog.DropSegmentIndex(context.Background(), 0, 0, 0, 0)
		assert.NoError(t, err)
	})

	t.Run("fail", func(t *testing.T) {
		metakv := mocks.NewMetaKv(t)
		metakv.EXPECT().Remove(mock.Anything).Return(errors.New("error"))
		catalog := &Catalog{
			MetaKv: metakv,
		}

		err := catalog.DropSegmentIndex(context.Background(), 0, 0, 0, 0)
		assert.Error(t, err)
	})
}

func BenchmarkCatalog_List1000Segments(b *testing.B) {
	paramtable.Init()
	etcdCli, err := etcd.GetEtcdClient(
		Params.EtcdCfg.UseEmbedEtcd.GetAsBool(),
		Params.EtcdCfg.EtcdUseSSL.GetAsBool(),
		Params.EtcdCfg.Endpoints.GetAsStrings(),
		Params.EtcdCfg.EtcdTLSCert.GetValue(),
		Params.EtcdCfg.EtcdTLSKey.GetValue(),
		Params.EtcdCfg.EtcdTLSCACert.GetValue(),
		Params.EtcdCfg.EtcdTLSMinVersion.GetValue())
	if err != nil {
		log.Fatal(err)
	}
	defer etcdCli.Close()

	randVal := rand.Int()
	dataRootPath := fmt.Sprintf("/test/data/list-segment-%d", randVal)

	etcdkv := etcdkv.NewEtcdKV(etcdCli, rootPath)
	defer etcdkv.Close()

	ctx := context.TODO()
	catalog := NewCatalog(etcdkv, dataRootPath, rootPath)

	generateSegments(ctx, catalog, 10, rootPath)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		segments, err := catalog.ListSegments(ctx)
		assert.NoError(b, err)
		for _, s := range segments {
			assert.NotNil(b, s)
			assert.NotNil(b, s.Binlogs)
			assert.NotNil(b, s.Statslogs)
			assert.NotNil(b, s.Deltalogs)
		}
	}
}

func generateSegments(ctx context.Context, catalog *Catalog, n int, rootPath string) {
	rand.Seed(time.Now().UnixNano())
	var collectionID int64

	for i := 0; i < n; i++ {
		if collectionID%25 == 0 {
			collectionID = rand.Int63()
		}

		v := rand.Int63()
		segment := addSegment(rootPath, collectionID, v, v, v)
		err := catalog.AddSegment(ctx, segment)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func addSegment(rootPath string, collectionID, partitionID, segmentID, fieldID int64) *datapb.SegmentInfo {
	binlogs = []*datapb.FieldBinlog{
		{
			FieldID: fieldID,
			Binlogs: []*datapb.Binlog{
				{
					EntriesNum: 10000,
					LogPath:    metautil.BuildInsertLogPath(rootPath, collectionID, partitionID, segmentID, fieldID, int64(rand.Int())),
				},
			},
		},
	}

	deltalogs = []*datapb.FieldBinlog{
		{
			FieldID: fieldID,
			Binlogs: []*datapb.Binlog{
				{
					EntriesNum: 5,
					LogPath:    metautil.BuildDeltaLogPath(rootPath, collectionID, partitionID, segmentID, int64(rand.Int())),
				},
			},
		},
	}

	statslogs = []*datapb.FieldBinlog{
		{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{
				{
					EntriesNum: 5,
					LogPath:    metautil.BuildStatsLogPath(rootPath, collectionID, partitionID, segmentID, fieldID, int64(rand.Int())),
				},
			},
		},
	}

	return &datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		PartitionID:  partitionID,
		NumOfRows:    10000,
		State:        commonpb.SegmentState_Flushed,
		Binlogs:      binlogs,
		Deltalogs:    deltalogs,
		Statslogs:    statslogs,
	}
}

func getSegment(rootPath string, collectionID, partitionID, segmentID, fieldID int64, binlogNum int) *datapb.SegmentInfo {
	binLogPaths := make([]*datapb.Binlog, binlogNum)
	for i := 0; i < binlogNum; i++ {
		binLogPaths[i] = &datapb.Binlog{
			EntriesNum: 10000,
			LogPath:    metautil.BuildInsertLogPath(rootPath, collectionID, partitionID, segmentID, fieldID, int64(i)),
		}
	}
	binlogs = []*datapb.FieldBinlog{
		{
			FieldID: fieldID,
			Binlogs: binLogPaths,
		},
	}

	deltalogs = []*datapb.FieldBinlog{
		{
			FieldID: fieldID,
			Binlogs: []*datapb.Binlog{
				{
					EntriesNum: 5,
					LogPath:    metautil.BuildDeltaLogPath(rootPath, collectionID, partitionID, segmentID, int64(rand.Int())),
				},
			},
		},
	}

	statslogs = []*datapb.FieldBinlog{
		{
			FieldID: 1,
			Binlogs: []*datapb.Binlog{
				{
					EntriesNum: 5,
					LogPath:    metautil.BuildStatsLogPath(rootPath, collectionID, partitionID, segmentID, fieldID, int64(rand.Int())),
				},
			},
		},
	}

	return &datapb.SegmentInfo{
		ID:           segmentID,
		CollectionID: collectionID,
		PartitionID:  partitionID,
		NumOfRows:    10000,
		State:        commonpb.SegmentState_Flushed,
		Binlogs:      binlogs,
		Deltalogs:    deltalogs,
		Statslogs:    statslogs,
	}
}

var Params = paramtable.Get()

func TestMain(m *testing.M) {
	paramtable.Init()
	code := m.Run()
	os.Exit(code)
}

func TestCatalog_GcConfirm(t *testing.T) {
	kc := &Catalog{}
	txn := mocks.NewMetaKv(t)
	kc.MetaKv = txn

	txn.On("LoadWithPrefix",
		mock.AnythingOfType("string")).
		Return(nil, nil, errors.New("error mock LoadWithPrefix")).
		Once()
	assert.False(t, kc.GcConfirm(context.TODO(), 100, 10000))

	txn.On("LoadWithPrefix",
		mock.AnythingOfType("string")).
		Return(nil, nil, nil)
	assert.True(t, kc.GcConfirm(context.TODO(), 100, 10000))
}

func TestCatalog_Import(t *testing.T) {
	kc := &Catalog{}
	mockErr := errors.New("mock error")

	job := &datapb.ImportJob{
		JobID: 0,
	}
	pit := &datapb.PreImportTask{
		JobID:  0,
		TaskID: 1,
	}
	it := &datapb.ImportTaskV2{
		JobID:  0,
		TaskID: 2,
	}

	t.Run("SaveImportJob", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().Save(mock.Anything, mock.Anything).Return(nil)
		kc.MetaKv = txn
		err := kc.SaveImportJob(job)
		assert.NoError(t, err)

		err = kc.SaveImportJob(nil)
		assert.NoError(t, err)

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().Save(mock.Anything, mock.Anything).Return(mockErr)
		kc.MetaKv = txn
		err = kc.SaveImportJob(job)
		assert.Error(t, err)
	})

	t.Run("ListImportJobs", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		value, err := proto.Marshal(job)
		assert.NoError(t, err)
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, []string{string(value)}, nil)
		kc.MetaKv = txn
		jobs, err := kc.ListImportJobs()
		assert.NoError(t, err)
		assert.Equal(t, 1, len(jobs))

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, []string{"@#%#^#"}, nil)
		kc.MetaKv = txn
		_, err = kc.ListImportJobs()
		assert.Error(t, err)

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, mockErr)
		kc.MetaKv = txn
		_, err = kc.ListImportJobs()
		assert.Error(t, err)
	})

	t.Run("DropImportJob", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().Remove(mock.Anything).Return(nil)
		kc.MetaKv = txn
		err := kc.DropImportJob(job.GetJobID())
		assert.NoError(t, err)

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().Remove(mock.Anything).Return(mockErr)
		kc.MetaKv = txn
		err = kc.DropImportJob(job.GetJobID())
		assert.Error(t, err)
	})

	t.Run("SavePreImportTask", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().Save(mock.Anything, mock.Anything).Return(nil)
		kc.MetaKv = txn
		err := kc.SavePreImportTask(pit)
		assert.NoError(t, err)

		err = kc.SavePreImportTask(nil)
		assert.NoError(t, err)

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().Save(mock.Anything, mock.Anything).Return(mockErr)
		kc.MetaKv = txn
		err = kc.SavePreImportTask(pit)
		assert.Error(t, err)
	})

	t.Run("ListPreImportTasks", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		value, err := proto.Marshal(pit)
		assert.NoError(t, err)
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, []string{string(value)}, nil)
		kc.MetaKv = txn
		tasks, err := kc.ListPreImportTasks()
		assert.NoError(t, err)
		assert.Equal(t, 1, len(tasks))

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, []string{"@#%#^#"}, nil)
		kc.MetaKv = txn
		_, err = kc.ListPreImportTasks()
		assert.Error(t, err)

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, mockErr)
		kc.MetaKv = txn
		_, err = kc.ListPreImportTasks()
		assert.Error(t, err)
	})

	t.Run("DropPreImportTask", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().Remove(mock.Anything).Return(nil)
		kc.MetaKv = txn
		err := kc.DropPreImportTask(pit.GetTaskID())
		assert.NoError(t, err)

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().Remove(mock.Anything).Return(mockErr)
		kc.MetaKv = txn
		err = kc.DropPreImportTask(pit.GetTaskID())
		assert.Error(t, err)
	})

	t.Run("SaveImportTask", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().Save(mock.Anything, mock.Anything).Return(nil)
		kc.MetaKv = txn
		err := kc.SaveImportTask(it)
		assert.NoError(t, err)

		err = kc.SaveImportTask(nil)
		assert.NoError(t, err)

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().Save(mock.Anything, mock.Anything).Return(mockErr)
		kc.MetaKv = txn
		err = kc.SaveImportTask(it)
		assert.Error(t, err)
	})

	t.Run("ListImportTasks", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		value, err := proto.Marshal(it)
		assert.NoError(t, err)
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, []string{string(value)}, nil)
		kc.MetaKv = txn
		tasks, err := kc.ListImportTasks()
		assert.NoError(t, err)
		assert.Equal(t, 1, len(tasks))

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, []string{"@#%#^#"}, nil)
		kc.MetaKv = txn
		_, err = kc.ListImportTasks()
		assert.Error(t, err)

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, mockErr)
		kc.MetaKv = txn
		_, err = kc.ListImportTasks()
		assert.Error(t, err)
	})

	t.Run("DropImportTask", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().Remove(mock.Anything).Return(nil)
		kc.MetaKv = txn
		err := kc.DropImportTask(it.GetTaskID())
		assert.NoError(t, err)

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().Remove(mock.Anything).Return(mockErr)
		kc.MetaKv = txn
		err = kc.DropImportTask(it.GetTaskID())
		assert.Error(t, err)
	})
}

func TestCatalog_AnalyzeTask(t *testing.T) {
	kc := &Catalog{}
	mockErr := errors.New("mock error")

	t.Run("ListAnalyzeTasks", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, mockErr)
		kc.MetaKv = txn

		tasks, err := kc.ListAnalyzeTasks(context.Background())
		assert.Error(t, err)
		assert.Nil(t, tasks)

		task := &indexpb.AnalyzeTask{
			CollectionID:  1,
			PartitionID:   2,
			FieldID:       3,
			FieldName:     "vector",
			FieldType:     schemapb.DataType_FloatVector,
			TaskID:        4,
			Version:       1,
			SegmentIDs:    nil,
			NodeID:        1,
			State:         indexpb.JobState_JobStateFinished,
			FailReason:    "",
			Dim:           8,
			CentroidsFile: "centroids",
		}
		value, err := proto.Marshal(task)
		assert.NoError(t, err)

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return([]string{"key1"}, []string{
			string(value),
		}, nil)
		kc.MetaKv = txn

		tasks, err = kc.ListAnalyzeTasks(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(tasks))

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return([]string{"key1"}, []string{"1234"}, nil)
		kc.MetaKv = txn

		tasks, err = kc.ListAnalyzeTasks(context.Background())
		assert.Error(t, err)
		assert.Nil(t, tasks)
	})

	t.Run("SaveAnalyzeTask", func(t *testing.T) {
		task := &indexpb.AnalyzeTask{
			CollectionID:  1,
			PartitionID:   2,
			FieldID:       3,
			FieldName:     "vector",
			FieldType:     schemapb.DataType_FloatVector,
			TaskID:        4,
			Version:       1,
			SegmentIDs:    nil,
			NodeID:        1,
			State:         indexpb.JobState_JobStateFinished,
			FailReason:    "",
			Dim:           8,
			CentroidsFile: "centroids",
		}

		txn := mocks.NewMetaKv(t)
		txn.EXPECT().Save(mock.Anything, mock.Anything).Return(nil)
		kc.MetaKv = txn

		err := kc.SaveAnalyzeTask(context.Background(), task)
		assert.NoError(t, err)

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().Save(mock.Anything, mock.Anything).Return(mockErr)
		kc.MetaKv = txn

		err = kc.SaveAnalyzeTask(context.Background(), task)
		assert.Error(t, err)
	})

	t.Run("DropAnalyzeTask", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().Remove(mock.Anything).Return(nil)
		kc.MetaKv = txn

		err := kc.DropAnalyzeTask(context.Background(), 1)
		assert.NoError(t, err)

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().Remove(mock.Anything).Return(mockErr)
		kc.MetaKv = txn

		err = kc.DropAnalyzeTask(context.Background(), 1)
		assert.Error(t, err)
	})
}

func Test_PartitionStatsInfo(t *testing.T) {
	kc := &Catalog{}
	mockErr := errors.New("mock error")

	t.Run("ListPartitionStatsInfo", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, mockErr)
		kc.MetaKv = txn

		infos, err := kc.ListPartitionStatsInfos(context.Background())
		assert.Error(t, err)
		assert.Nil(t, infos)

		info := &datapb.PartitionStatsInfo{
			CollectionID:  1,
			PartitionID:   2,
			VChannel:      "ch1",
			Version:       1,
			SegmentIDs:    nil,
			AnalyzeTaskID: 3,
			CommitTime:    10,
		}
		value, err := proto.Marshal(info)
		assert.NoError(t, err)

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return([]string{"key1"}, []string{string(value)}, nil)
		kc.MetaKv = txn

		infos, err = kc.ListPartitionStatsInfos(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(infos))

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return([]string{"key1"}, []string{"1234"}, nil)
		kc.MetaKv = txn

		infos, err = kc.ListPartitionStatsInfos(context.Background())
		assert.Error(t, err)
		assert.Nil(t, infos)
	})

	t.Run("SavePartitionStatsInfo", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().MultiSave(mock.Anything).Return(mockErr)
		kc.MetaKv = txn

		info := &datapb.PartitionStatsInfo{
			CollectionID:  1,
			PartitionID:   2,
			VChannel:      "ch1",
			Version:       1,
			SegmentIDs:    nil,
			AnalyzeTaskID: 3,
			CommitTime:    10,
		}

		err := kc.SavePartitionStatsInfo(context.Background(), info)
		assert.Error(t, err)

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().MultiSave(mock.Anything).Return(nil)
		kc.MetaKv = txn

		err = kc.SavePartitionStatsInfo(context.Background(), info)
		assert.NoError(t, err)
	})

	t.Run("DropPartitionStatsInfo", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().Remove(mock.Anything).Return(mockErr)
		kc.MetaKv = txn

		info := &datapb.PartitionStatsInfo{
			CollectionID:  1,
			PartitionID:   2,
			VChannel:      "ch1",
			Version:       1,
			SegmentIDs:    nil,
			AnalyzeTaskID: 3,
			CommitTime:    10,
		}

		err := kc.DropPartitionStatsInfo(context.Background(), info)
		assert.Error(t, err)

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().Remove(mock.Anything).Return(nil)
		kc.MetaKv = txn

		err = kc.DropPartitionStatsInfo(context.Background(), info)
		assert.NoError(t, err)
	})
}

func Test_CurrentPartitionStatsVersion(t *testing.T) {
	kc := &Catalog{}
	mockErr := errors.New("mock error")
	collID := int64(1)
	partID := int64(2)
	vChannel := "ch1"
	currentVersion := int64(1)

	t.Run("SaveCurrentPartitionStatsVersion", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().Save(mock.Anything, mock.Anything).Return(mockErr)
		kc.MetaKv = txn

		err := kc.SaveCurrentPartitionStatsVersion(context.Background(), collID, partID, vChannel, currentVersion)
		assert.Error(t, err)

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().Save(mock.Anything, mock.Anything).Return(nil)
		kc.MetaKv = txn

		err = kc.SaveCurrentPartitionStatsVersion(context.Background(), collID, partID, vChannel, currentVersion)
		assert.NoError(t, err)
	})

	t.Run("GetCurrentPartitionStatsVersion", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().Load(mock.Anything).Return("", mockErr)
		kc.MetaKv = txn

		version, err := kc.GetCurrentPartitionStatsVersion(context.Background(), collID, partID, vChannel)
		assert.Error(t, err)
		assert.Equal(t, int64(0), version)

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().Load(mock.Anything).Return("1", nil)
		kc.MetaKv = txn

		version, err = kc.GetCurrentPartitionStatsVersion(context.Background(), collID, partID, vChannel)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), version)
	})

	t.Run("DropCurrentPartitionStatsVersion", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().Remove(mock.Anything).Return(mockErr)
		kc.MetaKv = txn

		err := kc.DropCurrentPartitionStatsVersion(context.Background(), collID, partID, vChannel)
		assert.Error(t, err)

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().Remove(mock.Anything).Return(nil)
		kc.MetaKv = txn

		err = kc.DropCurrentPartitionStatsVersion(context.Background(), collID, partID, vChannel)
		assert.NoError(t, err)
	})
}

func Test_StatsTasks(t *testing.T) {
	kc := &Catalog{}
	mockErr := errors.New("mock error")

	t.Run("ListStatsTasks", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return(nil, nil, mockErr)
		kc.MetaKv = txn

		tasks, err := kc.ListStatsTasks(context.Background())
		assert.Error(t, err)
		assert.Nil(t, tasks)

		task := &indexpb.StatsTask{
			CollectionID:  1,
			PartitionID:   2,
			SegmentID:     3,
			InsertChannel: "ch1",
			TaskID:        4,
			Version:       1,
			NodeID:        1,
			State:         indexpb.JobState_JobStateFinished,
			FailReason:    "",
		}
		value, err := proto.Marshal(task)
		assert.NoError(t, err)

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return([]string{"key1"}, []string{string(value)}, nil)
		kc.MetaKv = txn

		tasks, err = kc.ListStatsTasks(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, 1, len(tasks))

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().LoadWithPrefix(mock.Anything).Return([]string{"key1"}, []string{"1234"}, nil)
		kc.MetaKv = txn

		tasks, err = kc.ListStatsTasks(context.Background())
		assert.Error(t, err)
		assert.Nil(t, tasks)
	})

	t.Run("SaveStatsTask", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().Save(mock.Anything, mock.Anything).Return(mockErr)
		kc.MetaKv = txn

		task := &indexpb.StatsTask{
			CollectionID:  1,
			PartitionID:   2,
			SegmentID:     3,
			InsertChannel: "ch1",
			TaskID:        4,
			Version:       1,
			NodeID:        1,
			State:         indexpb.JobState_JobStateFinished,
			FailReason:    "",
		}

		err := kc.SaveStatsTask(context.Background(), task)
		assert.Error(t, err)

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().Save(mock.Anything, mock.Anything).Return(nil)
		kc.MetaKv = txn

		err = kc.SaveStatsTask(context.Background(), task)
		assert.NoError(t, err)
	})

	t.Run("DropStatsTask", func(t *testing.T) {
		txn := mocks.NewMetaKv(t)
		txn.EXPECT().Remove(mock.Anything).Return(mockErr)
		kc.MetaKv = txn

		err := kc.DropStatsTask(context.Background(), 1)
		assert.Error(t, err)

		txn = mocks.NewMetaKv(t)
		txn.EXPECT().Remove(mock.Anything).Return(nil)
		kc.MetaKv = txn

		err = kc.DropStatsTask(context.Background(), 1)
		assert.NoError(t, err)
	})
}
