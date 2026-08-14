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
	"path"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCleanupUnpublishedV3ImportTasksIsIdempotent(t *testing.T) {
	ctx := context.Background()
	importMeta := NewMockImportMeta(t)
	checker := &importChecker{ctx: ctx, importMeta: importMeta}
	job := &importJob{ImportJob: &datapb.ImportJob{JobID: 1}}
	task := newImportTaskV3(&datapb.ImportTaskV3{
		JobId: 1, TaskId: 10, State: datapb.ImportTaskV3_Pending, NodeId: NullNodeID,
	}, importMeta, nil)

	first := importMeta.EXPECT().GetTaskBy(mock.Anything, mock.Anything, mock.Anything).Return([]ImportTask{task}).Once()
	second := importMeta.EXPECT().GetTaskBy(mock.Anything, mock.Anything, mock.Anything).Return(nil).Once()
	mock.InOrder(first, second)
	importMeta.EXPECT().RemoveTask(mock.Anything, int64(10)).Return(nil).Once()

	require.NoError(t, checker.cleanupUnpublishedV3ImportTasks(job))
	require.NoError(t, checker.cleanupUnpublishedV3ImportTasks(job))
}

func TestLoadImportV3ProtoRejectsDigestMismatch(t *testing.T) {
	ctx := context.Background()
	cm := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	prefix := path.Join(importV3Root, "1", "planning", "1")
	ref, _, err := writeImportV3Proto(ctx, cm, prefix, "snapshot", &datapb.PlanningSnapshot{
		FormatVersion: 1,
		JobId:         1,
		Generation:    1,
	})
	require.NoError(t, err)

	err = loadImportV3Proto(ctx, cm, ref, prefix, []byte("crc64-ecma:0000000000000000"), &datapb.PlanningSnapshot{})
	require.Error(t, err)
	require.Equal(t, merr.Code(merr.ErrDataIntegrity), merr.Code(err))
}
