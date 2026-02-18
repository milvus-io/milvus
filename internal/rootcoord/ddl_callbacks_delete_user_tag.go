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
	"go.uber.org/zap"
)

// DeleteUserTagRequest holds parameters for deleting a user tag
type DeleteUserTagRequest struct {
	UserName string
	TagKey   string
}

// deleteUserTagTask handles deletion of a specific user tag
type deleteUserTagTask struct {
	baseTask
	Req *DeleteUserTagRequest
}

// Prepare validates the request
func (t *deleteUserTagTask) Prepare(ctx context.Context) error {
	if t.Req == nil {
		return fmt.Errorf("delete user tag request cannot be nil")
	}
	if t.Req.UserName == "" {
		return fmt.Errorf("user name cannot be empty")
	}
	if t.Req.TagKey == "" {
		return fmt.Errorf("tag key cannot be empty")
	}
	return nil
}

// Execute implements the user tag deletion logic
func (t *deleteUserTagTask) Execute(ctx context.Context) error {
	log.Info("executing deleteUserTagTask",
		zap.String("user_name", t.Req.UserName),
		zap.String("tag_key", t.Req.TagKey),
	)

	// Get existing user tags
	userTags, err := t.core.meta.GetUserTags(ctx, t.Req.UserName)
	if err != nil {
		log.Warn("user tags not found",
			zap.String("user_name", t.Req.UserName),
			zap.Error(err),
		)
		// If user tags don't exist, treat as success
		return nil
	}

	// Delete the specific tag
	userTags.DeleteTag(t.Req.TagKey)

	// If no tags remain, delete the entire user tags entry
	if len(userTags.Tags) == 0 {
		if err := t.core.meta.DeleteUserTags(ctx, t.Req.UserName); err != nil {
			log.Error("failed to delete user tags",
				zap.String("user_name", t.Req.UserName),
				zap.Error(err),
			)
			return errors.Wrap(err, "failed to delete user tags")
		}
	} else {
		// Otherwise update with remaining tags
		if err := t.core.meta.SaveUserTags(ctx, userTags); err != nil {
			log.Error("failed to update user tags",
				zap.String("user_name", t.Req.UserName),
				zap.Error(err),
			)
			return errors.Wrap(err, "failed to update user tags")
		}
	}

	log.Info("user tag deleted successfully",
		zap.String("user_name", t.Req.UserName),
		zap.String("tag_key", t.Req.TagKey),
	)

	return nil
}

// GetLockerKey returns the locking key for this task
func (t *deleteUserTagTask) GetLockerKey() LockerKey {
	// Return a task locker key for user-level locking
	return &taskLockerKey{
		key:   fmt.Sprintf("user_tags/%s", t.Req.UserName),
		rw:    true,
		level: CollectionLock, // Reuse collection level for simplicity
		next:  nil,
	}
}
