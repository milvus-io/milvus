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
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestCatalog_AlterCompactionSegments(t *testing.T) {
	compactTo := []*datapb.SegmentInfo{{ID: 100}}
	compactFrom := []*datapb.SegmentInfo{{ID: 1}, {ID: 2}}
	binlogs := []metastore.BinlogsIncrement{{Segment: compactTo[0]}}

	mockey.PatchConvey("success writes compactTo before compactFrom", t, func() {
		kc := &Catalog{}
		var calls [][]*datapb.SegmentInfo
		mockey.Mock((*Catalog).AlterSegments).To(func(_ *Catalog, _ context.Context, segments []*datapb.SegmentInfo, b ...metastore.BinlogsIncrement) error {
			if len(calls) == 0 {
				// first call must be compactTo, carrying the binlog increments
				assert.Len(t, b, 1)
			} else {
				assert.Len(t, b, 0)
			}
			calls = append(calls, segments)
			return nil
		}).Build()

		err := kc.AlterCompactionSegments(context.TODO(), compactTo, compactFrom, binlogs)
		assert.NoError(t, err)
		assert.Len(t, calls, 2)
		assert.Equal(t, compactTo, calls[0])
		assert.Equal(t, compactFrom, calls[1])
	})

	mockey.PatchConvey("compactTo failure aborts before compactFrom", t, func() {
		kc := &Catalog{}
		var calls int
		mockey.Mock((*Catalog).AlterSegments).To(func(_ *Catalog, _ context.Context, _ []*datapb.SegmentInfo, _ ...metastore.BinlogsIncrement) error {
			calls++
			return errors.New("alter compactTo failed")
		}).Build()

		err := kc.AlterCompactionSegments(context.TODO(), compactTo, compactFrom, binlogs)
		assert.Error(t, err)
		assert.Equal(t, 1, calls)
	})

	mockey.PatchConvey("compactFrom failure is returned", t, func() {
		kc := &Catalog{}
		var calls int
		mockey.Mock((*Catalog).AlterSegments).To(func(_ *Catalog, _ context.Context, _ []*datapb.SegmentInfo, _ ...metastore.BinlogsIncrement) error {
			calls++
			if calls == 2 {
				return errors.New("alter compactFrom failed")
			}
			return nil
		}).Build()

		err := kc.AlterCompactionSegments(context.TODO(), compactTo, compactFrom, binlogs)
		assert.Error(t, err)
		assert.Equal(t, 2, calls)
	})
}

func TestCatalog_DropSegmentsAndMarkChannelDeleted(t *testing.T) {
	segments := []*datapb.SegmentInfo{{ID: 1}, {ID: 2}}

	mockey.PatchConvey("success saves segments before marking channel", t, func() {
		kc := &Catalog{}
		var calls []string
		mockey.Mock((*Catalog).SaveDroppedSegmentsInBatch).To(func(_ *Catalog, _ context.Context, segs []*datapb.SegmentInfo) error {
			assert.Equal(t, segments, segs)
			calls = append(calls, "SaveDroppedSegmentsInBatch")
			return nil
		}).Build()
		mockey.Mock((*Catalog).MarkChannelDeleted).To(func(_ *Catalog, _ context.Context, channel string) error {
			assert.Equal(t, "ch1", channel)
			calls = append(calls, "MarkChannelDeleted")
			return nil
		}).Build()

		err := kc.DropSegmentsAndMarkChannelDeleted(context.TODO(), "ch1", segments)
		assert.NoError(t, err)
		assert.Equal(t, []string{"SaveDroppedSegmentsInBatch", "MarkChannelDeleted"}, calls)
	})

	mockey.PatchConvey("segment save failure aborts before channel mark", t, func() {
		kc := &Catalog{}
		var calls []string
		mockey.Mock((*Catalog).SaveDroppedSegmentsInBatch).To(func(_ *Catalog, _ context.Context, _ []*datapb.SegmentInfo) error {
			calls = append(calls, "SaveDroppedSegmentsInBatch")
			return errors.New("save failed")
		}).Build()
		mockey.Mock((*Catalog).MarkChannelDeleted).To(func(_ *Catalog, _ context.Context, _ string) error {
			calls = append(calls, "MarkChannelDeleted")
			return nil
		}).Build()

		err := kc.DropSegmentsAndMarkChannelDeleted(context.TODO(), "ch1", segments)
		assert.Error(t, err)
		assert.Equal(t, []string{"SaveDroppedSegmentsInBatch"}, calls)
	})

	mockey.PatchConvey("channel mark failure is returned", t, func() {
		kc := &Catalog{}
		mockey.Mock((*Catalog).SaveDroppedSegmentsInBatch).To(func(_ *Catalog, _ context.Context, _ []*datapb.SegmentInfo) error {
			return nil
		}).Build()
		mockey.Mock((*Catalog).MarkChannelDeleted).To(func(_ *Catalog, _ context.Context, _ string) error {
			return errors.New("mark failed")
		}).Build()

		err := kc.DropSegmentsAndMarkChannelDeleted(context.TODO(), "ch1", segments)
		assert.Error(t, err)
	})
}

func TestCatalog_DropExternalCollectionRefreshJobAndTasks(t *testing.T) {
	taskIDs := []typeutil.UniqueID{11, 12, 13}

	mockey.PatchConvey("success drops all tasks then the job", t, func() {
		kc := &Catalog{}
		var droppedTasks []typeutil.UniqueID
		var jobDroppedAfterTasks bool
		mockey.Mock((*Catalog).DropExternalCollectionRefreshTask).To(func(_ *Catalog, _ context.Context, taskID typeutil.UniqueID) error {
			droppedTasks = append(droppedTasks, taskID)
			return nil
		}).Build()
		mockey.Mock((*Catalog).DropExternalCollectionRefreshJob).To(func(_ *Catalog, _ context.Context, jobID typeutil.UniqueID) error {
			assert.Equal(t, typeutil.UniqueID(1), jobID)
			jobDroppedAfterTasks = len(droppedTasks) == len(taskIDs)
			return nil
		}).Build()

		err := kc.DropExternalCollectionRefreshJobAndTasks(context.TODO(), 1, taskIDs)
		assert.NoError(t, err)
		assert.Equal(t, taskIDs, droppedTasks)
		assert.True(t, jobDroppedAfterTasks)
	})

	mockey.PatchConvey("task drop failure aborts before job drop", t, func() {
		kc := &Catalog{}
		var dropped int
		var jobDropped bool
		mockey.Mock((*Catalog).DropExternalCollectionRefreshTask).To(func(_ *Catalog, _ context.Context, _ typeutil.UniqueID) error {
			dropped++
			if dropped == 2 {
				return errors.New("drop task failed")
			}
			return nil
		}).Build()
		mockey.Mock((*Catalog).DropExternalCollectionRefreshJob).To(func(_ *Catalog, _ context.Context, _ typeutil.UniqueID) error {
			jobDropped = true
			return nil
		}).Build()

		err := kc.DropExternalCollectionRefreshJobAndTasks(context.TODO(), 1, taskIDs)
		assert.Error(t, err)
		assert.Equal(t, 2, dropped)
		assert.False(t, jobDropped)
	})

	mockey.PatchConvey("job drop failure is returned", t, func() {
		kc := &Catalog{}
		mockey.Mock((*Catalog).DropExternalCollectionRefreshTask).To(func(_ *Catalog, _ context.Context, _ typeutil.UniqueID) error {
			return nil
		}).Build()
		mockey.Mock((*Catalog).DropExternalCollectionRefreshJob).To(func(_ *Catalog, _ context.Context, _ typeutil.UniqueID) error {
			return errors.New("drop job failed")
		}).Build()

		err := kc.DropExternalCollectionRefreshJobAndTasks(context.TODO(), 1, taskIDs)
		assert.Error(t, err)
	})
}

func TestCatalog_DropPartitionStatsAndAnalyzeTask(t *testing.T) {
	info := &datapb.PartitionStatsInfo{CollectionID: 1, PartitionID: 2, VChannel: "vch", Version: 9}

	mockey.PatchConvey("success without rollback drops task then stats", t, func() {
		kc := &Catalog{}
		var calls []string
		mockey.Mock((*Catalog).DropAnalyzeTask).To(func(_ *Catalog, _ context.Context, taskID typeutil.UniqueID) error {
			assert.Equal(t, typeutil.UniqueID(7), taskID)
			calls = append(calls, "DropAnalyzeTask")
			return nil
		}).Build()
		mockey.Mock((*Catalog).SaveCurrentPartitionStatsVersion).To(func(_ *Catalog, _ context.Context, _, _ int64, _ string, _ int64) error {
			calls = append(calls, "SaveCurrentPartitionStatsVersion")
			return nil
		}).Build()
		mockey.Mock((*Catalog).DropPartitionStatsInfo).To(func(_ *Catalog, _ context.Context, i *datapb.PartitionStatsInfo) error {
			assert.Equal(t, info, i)
			calls = append(calls, "DropPartitionStatsInfo")
			return nil
		}).Build()

		err := kc.DropPartitionStatsAndAnalyzeTask(context.TODO(), info, 7, nil)
		assert.NoError(t, err)
		assert.Equal(t, []string{"DropAnalyzeTask", "DropPartitionStatsInfo"}, calls)
	})

	mockey.PatchConvey("success with rollback writes rollback version in between", t, func() {
		kc := &Catalog{}
		var calls []string
		mockey.Mock((*Catalog).DropAnalyzeTask).To(func(_ *Catalog, _ context.Context, _ typeutil.UniqueID) error {
			calls = append(calls, "DropAnalyzeTask")
			return nil
		}).Build()
		mockey.Mock((*Catalog).SaveCurrentPartitionStatsVersion).To(func(_ *Catalog, _ context.Context, collID, partID int64, vChannel string, version int64) error {
			assert.Equal(t, int64(1), collID)
			assert.Equal(t, int64(2), partID)
			assert.Equal(t, "vch", vChannel)
			assert.Equal(t, int64(5), version)
			calls = append(calls, "SaveCurrentPartitionStatsVersion")
			return nil
		}).Build()
		mockey.Mock((*Catalog).DropPartitionStatsInfo).To(func(_ *Catalog, _ context.Context, _ *datapb.PartitionStatsInfo) error {
			calls = append(calls, "DropPartitionStatsInfo")
			return nil
		}).Build()

		rollback := int64(5)
		err := kc.DropPartitionStatsAndAnalyzeTask(context.TODO(), info, 7, &rollback)
		assert.NoError(t, err)
		assert.Equal(t, []string{"DropAnalyzeTask", "SaveCurrentPartitionStatsVersion", "DropPartitionStatsInfo"}, calls)
	})

	mockey.PatchConvey("analyze task drop failure aborts the sequence", t, func() {
		kc := &Catalog{}
		var calls []string
		mockey.Mock((*Catalog).DropAnalyzeTask).To(func(_ *Catalog, _ context.Context, _ typeutil.UniqueID) error {
			calls = append(calls, "DropAnalyzeTask")
			return errors.New("drop analyze task failed")
		}).Build()
		mockey.Mock((*Catalog).DropPartitionStatsInfo).To(func(_ *Catalog, _ context.Context, _ *datapb.PartitionStatsInfo) error {
			calls = append(calls, "DropPartitionStatsInfo")
			return nil
		}).Build()

		rollback := int64(5)
		err := kc.DropPartitionStatsAndAnalyzeTask(context.TODO(), info, 7, &rollback)
		assert.Error(t, err)
		assert.Equal(t, []string{"DropAnalyzeTask"}, calls)
	})

	mockey.PatchConvey("rollback failure aborts before stats drop", t, func() {
		kc := &Catalog{}
		var calls []string
		mockey.Mock((*Catalog).DropAnalyzeTask).To(func(_ *Catalog, _ context.Context, _ typeutil.UniqueID) error {
			calls = append(calls, "DropAnalyzeTask")
			return nil
		}).Build()
		mockey.Mock((*Catalog).SaveCurrentPartitionStatsVersion).To(func(_ *Catalog, _ context.Context, _, _ int64, _ string, _ int64) error {
			calls = append(calls, "SaveCurrentPartitionStatsVersion")
			return errors.New("rollback failed")
		}).Build()
		mockey.Mock((*Catalog).DropPartitionStatsInfo).To(func(_ *Catalog, _ context.Context, _ *datapb.PartitionStatsInfo) error {
			calls = append(calls, "DropPartitionStatsInfo")
			return nil
		}).Build()

		rollback := int64(5)
		err := kc.DropPartitionStatsAndAnalyzeTask(context.TODO(), info, 7, &rollback)
		assert.Error(t, err)
		assert.Equal(t, []string{"DropAnalyzeTask", "SaveCurrentPartitionStatsVersion"}, calls)
	})

	mockey.PatchConvey("stats drop failure is returned", t, func() {
		kc := &Catalog{}
		mockey.Mock((*Catalog).DropAnalyzeTask).To(func(_ *Catalog, _ context.Context, _ typeutil.UniqueID) error {
			return nil
		}).Build()
		mockey.Mock((*Catalog).DropPartitionStatsInfo).To(func(_ *Catalog, _ context.Context, _ *datapb.PartitionStatsInfo) error {
			return errors.New("drop stats failed")
		}).Build()

		err := kc.DropPartitionStatsAndAnalyzeTask(context.TODO(), info, 7, nil)
		assert.Error(t, err)
	})
}
