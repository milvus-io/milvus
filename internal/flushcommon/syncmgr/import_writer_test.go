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

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/bytedance/mockey"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
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
	task.WithImportTarget(target).WithWriteRetryOptions(retry.Attempts(3))
	var got *indexcgopb.StoragePluginContext
	var attempts int
	open := mockey.Mock(packed.NewImportPackedWriter).To(func(_ []string, _ *arrow.Schema, _, _ int64,
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
