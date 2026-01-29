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

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// refreshExternalCollectionV2AckCallback handles the callback for RefreshExternalCollection DDL message.
// It uses the pre-allocated jobID from the WAL message to ensure idempotency.
// If the job already exists (e.g., from a retry), it will be skipped.
//
// Error handling strategy:
// - Non-retriable errors (collection not found, not external, job already in progress): return nil to avoid blocking WAL
// - Retriable errors (transient failures): return error to trigger WAL retry
func (s *DDLCallbacks) refreshExternalCollectionV2AckCallback(ctx context.Context, result message.BroadcastResultRefreshExternalCollectionMessageV2) error {
	header := result.Message.Header()
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", header.CollectionId),
		zap.String("collectionName", header.CollectionName),
		zap.Int64("jobID", header.JobId),
		zap.String("externalSource", header.ExternalSource),
	)
	log.Info("refreshExternalCollectionV2AckCallback received")

	// Submit refresh job using the pre-allocated jobID from WAL
	// This ensures idempotency - if the job already exists, it will be skipped
	jobID, err := s.externalCollectionRefreshManager.SubmitRefreshJobWithID(
		ctx,
		header.JobId,
		header.CollectionId,
		header.CollectionName,
		header.ExternalSource,
		header.ExternalSpec,
	)
	if err != nil {
		// Classify errors to determine if we should block WAL processing

		// Non-retriable errors: these indicate permanent failures or expected business logic
		// We return nil to allow WAL to proceed without retrying
		if isNonRetriableRefreshError(err) {
			log.Warn("non-retriable error in refresh job submission, proceeding without retry",
				zap.Error(err))
			return nil
		}

		// Retriable errors: return error to trigger WAL retry
		log.Error("retriable error in refresh job submission, will retry",
			zap.Error(err))
		return err
	}

	log.Info("refresh external collection job submitted via DDL callback", zap.Int64("jobID", jobID))
	return nil
}

// isNonRetriableRefreshError checks if the error is a non-retriable business logic error
// that should not block WAL processing.
func isNonRetriableRefreshError(err error) bool {
	// Check for specific error types using errors.Is
	// These errors indicate that the job cannot be submitted due to invalid state or input,
	// so retrying the WAL entry will not help.
	if errors.Is(err, merr.ErrCollectionNotFound) ||
		errors.Is(err, merr.ErrCollectionIllegalSchema) ||
		errors.Is(err, merr.ErrTaskDuplicate) {
		return true
	}
	return false
}
