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

package rootcoord

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"go.uber.org/zap"
)

// DropRLSPolicyRequest holds the parameters for dropping an RLS policy
type DropRLSPolicyRequest struct {
	DbName         string
	CollectionName string
	PolicyName     string
}

// dropRowPolicyTask handles the deletion of RLS policies
type dropRowPolicyTask struct {
	baseTask
	Req *DropRLSPolicyRequest
}

// Prepare validates the request
func (t *dropRowPolicyTask) Prepare(ctx context.Context) error {
	if t.Req == nil {
		return fmt.Errorf("drop row policy request cannot be nil")
	}
	if t.Req.PolicyName == "" {
		return fmt.Errorf("policy name cannot be empty")
	}
	if t.Req.CollectionName == "" {
		return fmt.Errorf("collection name cannot be empty")
	}
	return nil
}

// Execute implements the policy deletion logic
func (t *dropRowPolicyTask) Execute(ctx context.Context) error {
	log.Info("executing dropRowPolicyTask",
		zap.String("policy_name", t.Req.PolicyName),
		zap.String("collection_name", t.Req.CollectionName),
		zap.String("db_name", t.Req.DbName),
	)

	// Get collection metadata
	collMeta, err := t.core.meta.GetCollectionByName(ctx, t.Req.DbName, t.Req.CollectionName, typeutil.MaxTimestamp)
	if err != nil {
		log.Error("failed to get collection",
			zap.String("db_name", t.Req.DbName),
			zap.String("collection_name", t.Req.CollectionName),
			zap.Error(err),
		)
		return errors.Wrapf(err, "collection not found: %s", t.Req.CollectionName)
	}

	// Delete from metastore
	if err := t.core.meta.DeleteRLSPolicy(ctx, collMeta.DBID, collMeta.CollectionID, t.Req.PolicyName); err != nil {
		log.Error("failed to delete RLS policy",
			zap.String("policy_name", t.Req.PolicyName),
			zap.Error(err),
		)
		return errors.Wrap(err, "failed to delete RLS policy")
	}

	log.Info("RLS policy dropped successfully",
		zap.String("policy_name", t.Req.PolicyName),
		zap.Int64("collection_id", collMeta.CollectionID),
	)

	return nil
}

// GetLockerKey returns the locking key for this task
func (t *dropRowPolicyTask) GetLockerKey() LockerKey {
	// Return a task locker key for collection-level locking
	return &taskLockerKey{
		key:   fmt.Sprintf("rls_policy/%s/%s", t.Req.DbName, t.Req.CollectionName),
		rw:    true,
		level: CollectionLock,
		next:  nil,
	}
}
