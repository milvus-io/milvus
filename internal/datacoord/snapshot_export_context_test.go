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
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type snapshotExportContextCatalog struct {
	*snapshotExportCatalogFake
	beforeSave func()
}

func (c *snapshotExportContextCatalog) SaveExportSnapshotJob(ctx context.Context, job *datapb.ExportSnapshotJob) error {
	if c.beforeSave != nil {
		c.beforeSave()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.snapshotExportCatalogFake.SaveExportSnapshotJob(ctx, job)
}

func TestSnapshotExportManager_SubmitCancellation(t *testing.T) {
	for _, stage := range []string{"after pin", "during save", "deadline during save", "component shutdown"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer func() { cancel() }()
			mockValidate := mockey.Mock(snapshotstorage.ValidateForeignStorageRequest).Return(nil).Build()
			defer mockValidate.UnPatch()
			mockSnapshot := mockey.Mock((*snapshotMeta).GetSnapshot).Return(
				&datapb.SnapshotInfo{Id: 1, CollectionId: 100, Name: "snapshot-1"}, nil).Build()
			defer mockSnapshot.UnPatch()
			mockPin := mockey.Mock((*snapshotMeta).PinSnapshot).To(
				func(*snapshotMeta, context.Context, int64, string, int64) (int64, int, error) {
					if stage == "after pin" {
						cancel()
					}
					return 7001, 1, nil
				}).Build()
			defer mockPin.UnPatch()
			mockAlloc := mockey.Mock((*restoreAllocatorTarget).AllocID).Return(typeutil.UniqueID(9001), nil).Build()
			defer mockAlloc.UnPatch()
			fatalCalled := false
			mockFatal := mockey.Mock(mlog.Fatal).To(func(context.Context, string, ...mlog.Field) {
				fatalCalled = true
			}).Build()
			defer mockFatal.UnPatch()

			catalog := &snapshotExportContextCatalog{snapshotExportCatalogFake: newSnapshotExportCatalogFake()}
			meta, err := newSnapshotExportMeta(context.Background(), catalog)
			require.NoError(t, err)
			manager := newSnapshotExportManager(context.Background(), meta, &snapshotManager{
				snapshotMeta: &snapshotMeta{}, allocator: &restoreAllocatorTarget{},
			})
			defer manager.Close()
			catalog.beforeSave = func() {
				switch stage {
				case "during save":
					cancel()
				case "deadline during save":
					<-ctx.Done()
				case "component shutdown":
					manager.cancel()
				}
			}

			if stage == "deadline during save" {
				cancel()
				var timeoutCancel context.CancelFunc
				ctx, timeoutCancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
				defer timeoutCancel()
			}
			jobID, err := manager.Submit(ctx, 100, "snapshot-1", "default", "collection-1", "s3://target-bucket/export-root", "")
			require.False(t, fatalCalled)
			if stage == "component shutdown" {
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, jobID)
				require.Empty(t, catalog.jobs)
				return
			}
			require.NoError(t, err)
			require.Error(t, ctx.Err())
			require.Equal(t, int64(9001), jobID)
			job, ok := meta.GetJob(jobID)
			require.True(t, ok)
			require.Equal(t, int64(7001), job.GetPinId())
			require.Equal(t, datapb.ExportSnapshotJobState_ExportSnapshotJobPending, job.GetState())
			reloaded, err := newSnapshotExportMeta(context.Background(), catalog)
			require.NoError(t, err)
			recovered, ok := reloaded.GetJob(jobID)
			require.True(t, ok)
			require.Equal(t, job.GetPinId(), recovered.GetPinId())
		})
	}
}
