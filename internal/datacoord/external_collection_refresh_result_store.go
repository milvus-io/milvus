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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path"
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	externalRefreshTaskResultStorageVersion = int32(1)
	externalRefreshTaskResultRoot           = "external_refresh_results"
)

type externalCollectionRefreshResultStore struct {
	chunkManager storage.ChunkManager
}

type externalCollectionRefreshResultRef struct {
	path     string
	checksum []byte
	size     int
}

func newExternalCollectionRefreshResultStore(chunkManager storage.ChunkManager) *externalCollectionRefreshResultStore {
	if chunkManager == nil {
		return nil
	}
	return &externalCollectionRefreshResultStore{chunkManager: chunkManager}
}

// Save writes an immutable, content-addressed result before its reference is
// published in task metadata. Retrying the same result therefore overwrites
// the same key, while a changed result leaves an orphan that job GC can remove.
func (s *externalCollectionRefreshResultStore) Save(
	ctx context.Context,
	task *datapb.ExternalCollectionRefreshTask,
	keptSegments []int64,
	updatedSegments []*datapb.SegmentInfo,
) (externalCollectionRefreshResultRef, error) {
	// Preserve TaskVersion for rollback compatibility with older readers. Fresh
	// task IDs, rather than this field, identify attempts in current DataCoord.
	result := &datapb.ExternalCollectionRefreshTaskResult{
		CollectionId:    task.GetCollectionId(),
		JobId:           task.GetJobId(),
		TaskId:          task.GetTaskId(),
		TaskVersion:     task.GetVersion(),
		KeptSegments:    append([]int64(nil), keptSegments...),
		UpdatedSegments: cloneProtoSegments(updatedSegments),
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(result)
	if err != nil {
		return externalCollectionRefreshResultRef{}, merr.WrapErrSerializationFailed(
			err,
			"marshal external refresh task result %d",
			task.GetTaskId(),
		)
	}

	checksum := sha256.Sum256(payload)
	resultPath := s.resultPath(task, checksum[:])
	if err := s.chunkManager.Write(ctx, resultPath, payload); err != nil {
		return externalCollectionRefreshResultRef{}, merr.Wrapf(
			err,
			"write external refresh task result %d to %s",
			task.GetTaskId(),
			resultPath,
		)
	}

	return externalCollectionRefreshResultRef{
		path:     resultPath,
		checksum: append([]byte(nil), checksum[:]...),
		size:     len(payload),
	}, nil
}

// Load verifies both the bytes and their embedded task identity before making
// an externally stored result visible to job-level aggregation.
func (s *externalCollectionRefreshResultStore) Load(
	ctx context.Context,
	task *datapb.ExternalCollectionRefreshTask,
) (*datapb.ExternalCollectionRefreshTaskResult, error) {
	if task.GetResultPath() == "" {
		return nil, merr.WrapErrDataIntegrityMsg(
			"external refresh task %d has an empty result path",
			task.GetTaskId(),
		)
	}
	if len(task.GetResultChecksum()) != sha256.Size {
		return nil, merr.WrapErrDataIntegrityMsg(
			"external refresh task %d has invalid result checksum length %d",
			task.GetTaskId(),
			len(task.GetResultChecksum()),
		)
	}

	payload, err := s.chunkManager.Read(ctx, task.GetResultPath())
	if err != nil {
		return nil, merr.Wrapf(
			err,
			"read external refresh task result %d from %s",
			task.GetTaskId(),
			task.GetResultPath(),
		)
	}
	actualChecksum := sha256.Sum256(payload)
	if !bytes.Equal(actualChecksum[:], task.GetResultChecksum()) {
		return nil, merr.WrapErrDataIntegrityMsg(
			"external refresh task %d result checksum mismatch",
			task.GetTaskId(),
		)
	}

	result := &datapb.ExternalCollectionRefreshTaskResult{}
	if err := proto.Unmarshal(payload, result); err != nil {
		return nil, merr.WrapErrDataIntegrity(
			err,
			"unmarshal external refresh task result %d",
			task.GetTaskId(),
		)
	}
	if result.GetCollectionId() != task.GetCollectionId() ||
		result.GetJobId() != task.GetJobId() ||
		result.GetTaskId() != task.GetTaskId() {
		return nil, merr.WrapErrDataIntegrityMsg(
			"external refresh result identity mismatch for task %d",
			task.GetTaskId(),
		)
	}
	return result, nil
}

func (s *externalCollectionRefreshResultStore) Remove(ctx context.Context, resultPath string) error {
	if resultPath == "" {
		return nil
	}
	if err := s.chunkManager.Remove(ctx, resultPath); err != nil {
		return merr.Wrapf(err, "remove external refresh task result %s", resultPath)
	}
	return nil
}

func (s *externalCollectionRefreshResultStore) RemoveJob(ctx context.Context, collectionID, jobID int64) error {
	jobPath := path.Join(
		s.chunkManager.RootPath(),
		externalRefreshTaskResultRoot,
		strconv.FormatInt(collectionID, 10),
		strconv.FormatInt(jobID, 10),
	)
	if err := s.chunkManager.RemoveWithPrefix(ctx, jobPath+"/"); err != nil {
		return merr.Wrapf(err, "remove external refresh job result prefix %s", jobPath)
	}
	// LocalChunkManager removes objects matched by the prefix but leaves the
	// directory tree behind. Removing the root is harmless for remote stores.
	if err := s.chunkManager.Remove(ctx, jobPath); err != nil {
		return merr.Wrapf(err, "remove external refresh job result root %s", jobPath)
	}
	return nil
}

func (s *externalCollectionRefreshResultStore) resultPath(
	task *datapb.ExternalCollectionRefreshTask,
	checksum []byte,
) string {
	return path.Join(
		s.chunkManager.RootPath(),
		externalRefreshTaskResultRoot,
		strconv.FormatInt(task.GetCollectionId(), 10),
		strconv.FormatInt(task.GetJobId(), 10),
		strconv.FormatInt(task.GetTaskId(), 10),
		// Keep the legacy version path component for rollback compatibility. It
		// no longer provides attempt isolation; fresh task IDs do that.
		strconv.FormatInt(task.GetVersion(), 10),
		hex.EncodeToString(checksum)+".pb",
	)
}
