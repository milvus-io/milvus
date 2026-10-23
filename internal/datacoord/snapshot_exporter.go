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
	"strings"

	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func commitSnapshotExportMetadata(
	ctx context.Context,
	targetCM storage.ChunkManager,
	targetRoot string,
	metadataURI string,
) error {
	if targetCM == nil {
		return merr.WrapErrServiceInternalMsg("target chunk manager cannot be nil")
	}
	if strings.TrimSpace(targetRoot) == "" || strings.TrimSpace(metadataURI) == "" {
		return merr.WrapErrServiceInternalMsg("target root and metadata URI are required")
	}
	_, err := snapshotstorage.NewSnapshotWriter(targetCM).CommitStagedMetadata(
		ctx,
		snapshotstorage.GetSnapshotStagingMetadataPath(targetRoot),
		snapshotstorage.NormalizeSnapshotObjectPath(metadataURI),
		metadataURI,
	)
	return err
}

func cleanupSnapshotExportStagingMetadata(
	ctx context.Context,
	targetCM storage.ChunkManager,
	targetRoot string,
) error {
	if targetCM == nil || strings.TrimSpace(targetRoot) == "" {
		return nil
	}
	return targetCM.Remove(ctx, snapshotstorage.GetSnapshotStagingMetadataPath(targetRoot))
}
