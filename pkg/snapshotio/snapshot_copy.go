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

package snapshotio

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
	"strings"

	objectstore "github.com/milvus-io/milvus/pkg/v3/objectstorage/store"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const SnapshotExportMaxRequestBytes = 1024 * 1024

// SnapshotExportAttemptURI fences objects written by lost workers.
func SnapshotExportAttemptURI(req *datapb.SnapshotExportCopyRequest) string {
	id := req.GetGenerationId()
	return strings.TrimRight(req.GetTargetUri(), "/") + "/attempts/" + strconv.FormatInt(id, 10)
}

// CopySnapshotExportPlan copies files and reports sizes only after successful
// completion. Metadata copies report zero bytes but still report file completion.
// Callbacks are serialized. It shares the bounded executor with snapshot Restore.
func CopySnapshotExportPlan(ctx context.Context, items []FileCopy, sourceBucket, targetBucket string, targetStore objectstore.ObjectStore, permits chan struct{}, completed func(int64)) error {
	if targetStore == nil {
		return merr.WrapErrServiceInternalMsg("snapshot target store is nil")
	}
	_, err := ExecuteFileCopies(ctx, items, permits, func(copyCtx context.Context, file FileCopy) (FileCopyOutcome, error) {
		size, err := targetStore.Copy(copyCtx, objectstore.CopyRequest{
			Source:      objectstore.Object{Bucket: sourceBucket, Key: file.SourcePath},
			Destination: objectstore.Object{Bucket: targetBucket, Key: file.DestinationPath},
		})
		if file.IsMetadata {
			size = 0
		}
		return FileCopyOutcome{Confirmed: err == nil, Bytes: size}, err
	}, completed)
	return err
}

// SanitizeSnapshotExportReason excludes credentials from worker failure reports.
func SanitizeSnapshotExportReason(err error, req *datapb.SnapshotExportCopyRequest) string {
	if err == nil {
		return ""
	}
	secrets := []string{req.GetExternalSpec(), req.GetSourceStorageConfig().GetAccessKeyID(), req.GetSourceStorageConfig().GetSecretAccessKey(), req.GetSourceStorageConfig().GetGcpCredentialJSON(), req.GetSourceStorageConfig().GetSslCACert()}
	var spec struct {
		Extfs map[string]json.RawMessage `json:"extfs"`
	}
	if json.Unmarshal([]byte(req.GetExternalSpec()), &spec) == nil {
		for _, key := range []string{"access_key_id", "access_key_value", "ssl_ca_cert", "external_id", "credential_json", "source_sas_token"} {
			var v string
			if json.Unmarshal(spec.Extfs[key], &v) == nil {
				secrets = append(secrets, v)
			}
		}
	}
	reason := strings.TrimSpace(err.Error())
	for _, secret := range secrets {
		if secret != "" {
			reason = strings.ReplaceAll(reason, secret, "<redacted>")
			reason = strings.ReplaceAll(reason, url.QueryEscape(secret), "<redacted>")
		}
	}
	return reason
}

// ExecuteSnapshotExport copies one segment range and returns scalar statistics.
// Only DataCoord finalization prepares staging metadata after all writers exit.
// The caller owns clients and supplies a task-local copy concurrency budget.
func ExecuteSnapshotExport(ctx context.Context, req *datapb.SnapshotExportCopyRequest, sourceCM ChunkManager, resolved *ResolvedForeignStorage, permits chan struct{}, progress func(int64, int64, int64)) (*datapb.SnapshotExportCopyResponse, error) {
	snapshot, err := NewSnapshotReader(sourceCM).ReadSnapshot(ctx, req.GetSnapshotMetadataPath(), false)
	if err != nil {
		return nil, err
	}
	if snapshot.SnapshotInfo.GetId() != req.GetSnapshotId() || snapshot.SnapshotInfo.GetCollectionId() != req.GetCollectionId() {
		return nil, merr.WrapErrDataIntegrityMsg("snapshot export source identity changed")
	}
	targetURI, err := BuildStorageConfigSnapshotURI(resolved.ForeignStorageConfig, NormalizeSnapshotObjectPath(SnapshotExportAttemptURI(req)))
	if err != nil {
		return nil, err
	}
	result, err := copyRange(ctx, snapshot, sourceCM, req.GetSourceStorageConfig(), resolved, targetURI, segmentRange{Start: req.GetSegmentStart(), End: req.GetSegmentEnd()}, permits, nil, progress)
	if err != nil {
		return nil, err
	}
	return &datapb.SnapshotExportCopyResponse{TaskId: req.GetTaskId(), State: datapb.ImportTaskStateV2_Completed, Status: merr.Success(), CopiedFiles: result.CopiedFiles, CopiedBytes: result.CopiedBytes, TotalFiles: result.TotalFiles}, nil
}
