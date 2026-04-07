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
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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
	// Validate tag keys and values to prevent expression injection.
	// Tag values are substituted into RLS filter expressions, so they must not
	// contain characters that could break or modify the expression semantics.
	for key, value := range t.Req.Tags {
		if err := validateTagKeyValue(key, value); err != nil {
			return err
		}
	}
	return nil
}

// validateTagKeyValue checks that tag keys and values are safe for use in RLS expressions.
// Uses allowlist approach: only explicitly safe characters are permitted.
func validateTagKeyValue(key, value string) error {
	if key == "" {
		return fmt.Errorf("tag key cannot be empty")
	}
	if len(key) > 128 {
		return fmt.Errorf("tag key too long (max 128 characters)")
	}
	if len(value) > 512 {
		return fmt.Errorf("tag value too long (max 512 characters)")
	}
	// Tag keys: alphanumeric, underscore, hyphen, dot only
	for _, ch := range key {
		if !isAllowedTagKeyChar(ch) {
			return fmt.Errorf("tag key contains disallowed character: %c (allowed: letters, digits, _, -, .)", ch)
		}
	}
	// Tag values: printable characters except expression-sensitive ones.
	// Values are substituted into single-quoted strings in RLS expressions,
	// so we reject any character that could break or modify expression semantics.
	for _, ch := range value {
		if !isAllowedTagValueChar(ch) {
			return fmt.Errorf("tag value contains disallowed character: %c", ch)
		}
	}
	return nil
}

// isAllowedTagKeyChar returns true for characters safe in tag keys.
func isAllowedTagKeyChar(ch rune) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
		(ch >= '0' && ch <= '9') || ch == '_' || ch == '-' || ch == '.'
}

// isAllowedTagValueChar returns true for characters safe in tag values.
// Allows alphanumeric, spaces, and common punctuation; rejects quotes, backslashes,
// parentheses, semicolons, and control characters that could enable expression injection.
func isAllowedTagValueChar(ch rune) bool {
	if ch < 32 { // control characters
		return false
	}
	switch ch {
	case '\'', '"', '\\', '(', ')', ';', '`', '$':
		return false
	}
	return true
}

// Execute implements the user tags setting logic
func (t *setUserTagsTask) Execute(ctx context.Context) error {
	log.Info("executing setUserTagsTask",
		zap.String("user_name", t.Req.UserName),
		zap.Int("num_tags", len(t.Req.Tags)),
	)

	// Merge semantics: AddUserTags only updates provided keys, existing keys are retained.
	existing, err := t.core.meta.GetUserTags(ctx, t.Req.UserName)
	if err != nil {
		// Only initialize a new tag set when the user has no existing tag record.
		// For storage or transient failures, return error to avoid overwriting tags.
		if !errors.Is(err, merr.ErrIoKeyNotFound) {
			return errors.Wrap(err, "failed to load existing user tags")
		}
	}
	if existing == nil {
		tags := make(map[string]string, len(t.Req.Tags))
		for k, v := range t.Req.Tags {
			tags[k] = v
		}
		existing = model.NewRLSUserTags(t.Req.UserName, tags)
	} else {
		existing.Merge(t.Req.Tags)
	}

	// Enforce user tag count limit
	maxTags := paramtable.Get().ProxyCfg.RLSMaxUserTags.GetAsInt()
	if maxTags > 0 && len(existing.Tags) > maxTags {
		return fmt.Errorf("user %s would exceed maximum tags: %d > %d",
			t.Req.UserName, len(existing.Tags), maxTags)
	}

	// Save to metastore
	if err := t.core.meta.SaveUserTags(ctx, existing); err != nil {
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
