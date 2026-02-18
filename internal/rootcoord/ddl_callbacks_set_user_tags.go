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

	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"go.uber.org/zap"
)

// SetUserTagsRequest holds parameters for setting user tags
type SetUserTagsRequest struct {
	UserName string
	Tags     map[string]string
}

// setUserTagsTask handles setting or updating user tags
type setUserTagsTask struct {
	baseTask
	Req *SetUserTagsRequest
}

// Prepare validates the request
func (t *setUserTagsTask) Prepare(ctx context.Context) error {
	if t.Req == nil {
		return fmt.Errorf("set user tags request cannot be nil")
	}
	if t.Req.UserName == "" {
		return fmt.Errorf("user name cannot be empty")
	}
	if len(t.Req.Tags) == 0 {
		return fmt.Errorf("tags cannot be empty")
	}
	return nil
}

// Execute implements the user tags setting logic
func (t *setUserTagsTask) Execute(ctx context.Context) error {
	log.Info("executing setUserTagsTask",
		zap.String("user_name", t.Req.UserName),
		zap.Int("num_tags", len(t.Req.Tags)),
	)

	// Create user tags model
	userTags := &model.RLSUserTags{
		UserName: t.Req.UserName,
		Tags:     t.Req.Tags,
	}

	// Save to metastore
	if err := t.core.meta.SaveUserTags(ctx, userTags); err != nil {
		log.Error("failed to save user tags",
			zap.String("user_name", t.Req.UserName),
			zap.Error(err),
		)
		return errors.Wrap(err, "failed to save user tags")
	}

	log.Info("user tags set successfully",
		zap.String("user_name", t.Req.UserName),
		zap.Int("num_tags", len(t.Req.Tags)),
	)

	return nil
}

// GetLockerKey returns the locking key for this task
func (t *setUserTagsTask) GetLockerKey() LockerKey {
	// Return a task locker key for user-level locking
	return &taskLockerKey{
		key:   fmt.Sprintf("user_tags/%s", t.Req.UserName),
		rw:    true,
		level: CollectionLock, // Reuse collection level for simplicity
		next:  nil,
	}
}
