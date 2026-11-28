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

package syncmgr

import (
	"context"
	"path"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/bytedance/mockey"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

func (s *SyncTaskSuite) TestImportWriterUsesTaskKeyset() {
	seg := s.createSegment(storage.StorageV2)
	s.metacache.EXPECT().GetSegmentByID(s.segmentID).Return(seg, true).Maybe()
	task := s.getSuiteSyncTask(new(SyncPack).WithInsertData([]*storage.InsertData{s.getInsertBuffer()}))
	target := &indexcgopb.StoragePluginContext{EncryptionZoneId: 17, CollectionId: s.collectionID, EncryptionKey: "bmV3"}
	task.pack.WithPluginContext(target)
	task.WithWriteRetryOptions(retry.Attempts(3))
	var got *indexcgopb.StoragePluginContext
	var attempts int
	open := mockey.Mock(packed.NewPackedWriter).To(func(_ []string, _ *arrow.Schema, _, _ int64,
		_ []storagecommon.ColumnGroup, _ *indexpb.StorageConfig, key *indexcgopb.StoragePluginContext,
	) (*packed.PackedWriter, error) {
		attempts++
		got = key
		return nil, merr.ErrSegcoreUnsupported
	}).Build()
	defer open.UnPatch()
	err := task.Run(context.Background())
	s.Require().ErrorIs(err, merr.ErrSegcoreUnsupported)
	s.Require().Equal(1, attempts)
	s.Require().Same(target, got)
}

func (s *SyncTaskSuite) TestBulkPackWriterV2RetriesTransientKeyLookup() {
	s.T().Cleanup(hookutil.SetTestCipher(hookutil.GetCipher()))
	hookutil.InitTestCipher()
	schema := s.schema
	schema.Properties = []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "17"}}
	defer func() { schema.Properties = nil }()

	pack := new(SyncPack).WithCollectionID(s.collectionID).WithPartitionID(s.partitionID).
		WithSegmentID(s.segmentID).WithChannelName(s.channelName).
		WithInsertData([]*storage.InsertData{s.getInsertBuffer()})
	target := &indexcgopb.StoragePluginContext{EncryptionZoneId: 17, CollectionId: s.collectionID, EncryptionKey: "bmV3"}

	var lookups int
	var writerOpened int
	lookupMock := mockey.Mock(hookutil.GetCollectionPluginContext).To(func(_ []*commonpb.KeyValuePair, _ int64) (*indexcgopb.StoragePluginContext, error) {
		lookups++
		if lookups == 1 {
			return nil, merr.ErrServiceUnavailable
		}
		return target, nil
	}).Build()
	defer lookupMock.UnPatch()

	openMock := mockey.Mock(packed.NewPackedWriter).To(func(_ []string, _ *arrow.Schema, _, _ int64,
		_ []storagecommon.ColumnGroup, _ *indexpb.StorageConfig, key *indexcgopb.StoragePluginContext,
	) (*packed.PackedWriter, error) {
		writerOpened++
		s.Require().Same(target, key)
		return nil, retry.Unrecoverable(merr.ErrSegcoreUnsupported)
	}).Build()
	defer openMock.UnPatch()

	bw := NewBulkPackWriterV2(s.metacache, schema, s.chunkManager, s.allocator, packed.DefaultWriteBufferSize, 0,
		&indexpb.StorageConfig{StorageType: "local", RootPath: s.T().TempDir()},
		storagecommon.SplitColumns(schema.GetFields(), map[int64]storagecommon.ColumnStats{}, storagecommon.NewSelectedDataTypePolicy()),
		retry.Attempts(3), retry.Sleep(time.Millisecond), retry.MaxSleepTime(time.Millisecond))

	_, _, err := bw.writeInserts(context.Background(), pack)
	s.Require().ErrorIs(err, merr.ErrSegcoreUnsupported)
	s.Require().Equal(2, lookups, "transient key lookup failure must retry inside retry.Do")
	s.Require().Equal(1, writerOpened)
}

func (s *SyncTaskSuite) TestBulkPackWriterV3RetriesTransientKeyLookup() {
	s.T().Cleanup(hookutil.SetTestCipher(hookutil.GetCipher()))
	hookutil.InitTestCipher()
	schema := s.schema
	schema.Properties = []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "17"}}
	defer func() { schema.Properties = nil }()

	pack := new(SyncPack).WithCollectionID(s.collectionID).WithPartitionID(s.partitionID).
		WithSegmentID(s.segmentID).WithChannelName(s.channelName).
		WithInsertData([]*storage.InsertData{s.getInsertBuffer()})
	target := &indexcgopb.StoragePluginContext{EncryptionZoneId: 17, CollectionId: s.collectionID, EncryptionKey: "bmV3"}

	var lookups int
	var writerOpened int
	lookupMock := mockey.Mock(hookutil.GetCollectionPluginContext).To(func(_ []*commonpb.KeyValuePair, _ int64) (*indexcgopb.StoragePluginContext, error) {
		lookups++
		if lookups == 1 {
			return nil, merr.ErrServiceUnavailable
		}
		return target, nil
	}).Build()
	defer lookupMock.UnPatch()

	openMock := mockey.Mock(packed.NewFFIPackedWriter).To(func(_ string, _ *arrow.Schema,
		_ []storagecommon.ColumnGroup, _ *indexpb.StorageConfig, key *indexcgopb.StoragePluginContext, _ ...map[string]string,
	) (*packed.FFIPackedWriter, error) {
		writerOpened++
		s.Require().Same(target, key)
		return nil, merr.ErrSegcoreUnsupported
	}).Build()
	defer openMock.UnPatch()

	root := s.T().TempDir()
	basePath := path.Join(root, "insert_log/1/2/3")
	bw := NewBulkPackWriterV3(s.metacache, schema, s.chunkManager, s.allocator, packed.DefaultWriteBufferSize, 0,
		&indexpb.StorageConfig{StorageType: "local", RootPath: root},
		storagecommon.SplitColumns(schema.GetFields(), map[int64]storagecommon.ColumnStats{}, storagecommon.NewSelectedDataTypePolicy()),
		packed.MarshalManifestPath(basePath, packed.ManifestEarliest),
		retry.Attempts(3), retry.Sleep(time.Millisecond), retry.MaxSleepTime(time.Millisecond))

	_, _, _, err := bw.writeInserts(context.Background(), pack, basePath)
	s.Require().ErrorIs(err, merr.ErrSegcoreUnsupported)
	s.Require().Equal(2, lookups, "transient key lookup failure must retry inside V3 writeInserts")
	s.Require().Equal(1, writerOpened)
}
