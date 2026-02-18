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
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
	"go.uber.org/zap"
)

// CreateRLSPolicyRequest holds the parameters for creating an RLS policy
type CreateRLSPolicyRequest struct {
	DbName       string
	CollectionName string
	PolicyName   string
	PolicyType   int32
	Actions      []string
	Roles        []string
	UsingExpr    string
	CheckExpr    string
	Description  string
}

// createRowPolicyTask handles the creation of RLS policies
type createRowPolicyTask struct {
	baseTask
	Req *CreateRLSPolicyRequest
}

// Prepare validates the request
func (t *createRowPolicyTask) Prepare(ctx context.Context) error {
	if t.Req == nil {
		return fmt.Errorf("create row policy request cannot be nil")
	}
	if t.Req.PolicyName == "" {
		return fmt.Errorf("policy name cannot be empty")
	}
	if t.Req.CollectionName == "" {
		return fmt.Errorf("collection name cannot be empty")
	}
	if len(t.Req.Actions) == 0 {
		return fmt.Errorf("actions cannot be empty")
	}
	if len(t.Req.Roles) == 0 {
		return fmt.Errorf("roles cannot be empty")
	}
	return nil
}

// Execute implements the policy creation logic
func (t *createRowPolicyTask) Execute(ctx context.Context) error {
	log.Info("executing createRowPolicyTask",
		zap.String("policy_name", t.Req.PolicyName),
		zap.String("collection_name", t.Req.CollectionName),
		zap.String("db_name", t.Req.DbName),
	)

	// Get collection metadata to verify it exists
	collMeta, err := t.core.meta.GetCollectionByName(ctx, t.Req.DbName, t.Req.CollectionName, typeutil.MaxTimestamp)
	if err != nil {
		log.Error("failed to get collection",
			zap.String("db_name", t.Req.DbName),
			zap.String("collection_name", t.Req.CollectionName),
			zap.Error(err),
		)
		return errors.Wrapf(err, "collection not found: %s", t.Req.CollectionName)
	}

	// Create policy model
	policy := &model.RLSPolicy{
		PolicyName:   t.Req.PolicyName,
		CollectionID: collMeta.CollectionID,
		DBID:         collMeta.DBID,
		PolicyType:   model.RLSPolicyType(t.Req.PolicyType),
		Actions:      t.Req.Actions,
		Roles:        t.Req.Roles,
		UsingExpr:    t.Req.UsingExpr,
		CheckExpr:    t.Req.CheckExpr,
		Description:  t.Req.Description,
		CreatedAt:    uint64(time.Now().UnixMilli()),
	}

	// Validate policy
	if err := policy.Validate(); err != nil {
		log.Error("policy validation failed",
			zap.String("policy_name", t.Req.PolicyName),
			zap.Error(err),
		)
		return errors.Wrapf(err, "invalid policy: %w", err)
	}

	// Save to metastore
	if err := t.core.meta.SaveRLSPolicy(ctx, policy); err != nil {
		log.Error("failed to save RLS policy",
			zap.String("policy_name", t.Req.PolicyName),
			zap.Error(err),
		)
		return errors.Wrap(err, "failed to save RLS policy")
	}

	log.Info("RLS policy created successfully",
		zap.String("policy_name", t.Req.PolicyName),
		zap.Int64("collection_id", policy.CollectionID),
	)

	return nil
}

// GetLockerKey returns the locking key for this task
func (t *createRowPolicyTask) GetLockerKey() LockerKey {
	// Return a task locker key for collection-level locking
	return &taskLockerKey{
		key:   fmt.Sprintf("collection/%s", t.Req.CollectionName),
		rw:    true,
		level: CollectionLock,
		next:  nil,
	}
}
