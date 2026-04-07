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

package proxy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRLSAuditEventTypes(t *testing.T) {
	assert.Equal(t, RLSAuditEventType("QUERY"), RLSAuditEventQuery)
	assert.Equal(t, RLSAuditEventType("SEARCH"), RLSAuditEventSearch)
	assert.Equal(t, RLSAuditEventType("INSERT"), RLSAuditEventInsert)
	assert.Equal(t, RLSAuditEventType("DELETE"), RLSAuditEventDelete)
	assert.Equal(t, RLSAuditEventType("UPSERT"), RLSAuditEventUpsert)
	assert.Equal(t, RLSAuditEventType("DENIED"), RLSAuditEventDenied)
}

func TestGetRLSAuditLogger(t *testing.T) {
	logger := GetRLSAuditLogger()
	assert.NotNil(t, logger)

	// Should return the same singleton instance
	logger2 := GetRLSAuditLogger()
	assert.Same(t, logger, logger2)
}

func TestRLSAuditLoggerSetEnabled(t *testing.T) {
	logger := GetRLSAuditLogger()

	// Save original state
	originalState := logger.IsEnabled()
	defer logger.SetEnabled(originalState)

	logger.SetEnabled(true)
	assert.True(t, logger.IsEnabled())

	logger.SetEnabled(false)
	assert.False(t, logger.IsEnabled())
}

func TestRLSAuditLoggerLogPolicyApplied(t *testing.T) {
	logger := GetRLSAuditLogger()

	// Save original state
	originalState := logger.IsEnabled()
	defer logger.SetEnabled(originalState)

	ctx := context.Background()

	// Test when enabled
	logger.SetEnabled(true)
	event := &RLSAuditEvent{
		EventType:        RLSAuditEventQuery,
		Timestamp:        time.Now(),
		UserName:         "testuser",
		UserRoles:        []string{"admin", "reader"},
		DBID:             1,
		DBName:           "testdb",
		CollectionID:     100,
		CollectionName:   "testcoll",
		Action:           "query",
		PolicyCount:      2,
		OriginalExpr:     "id > 0",
		MergedExpr:       "(id > 0) AND (owner_id == 'testuser')",
		Allowed:          true,
		ProcessingTimeMs: 5,
	}
	// Should not panic
	logger.LogPolicyApplied(ctx, event)

	// Test with nil event - should not panic
	logger.LogPolicyApplied(ctx, nil)

	// Test when disabled - should not panic
	logger.SetEnabled(false)
	logger.LogPolicyApplied(ctx, event)
}

func TestRLSAuditLoggerLogAccessDenied(t *testing.T) {
	logger := GetRLSAuditLogger()

	// Save original state
	originalState := logger.IsEnabled()
	defer logger.SetEnabled(originalState)

	ctx := context.Background()

	logger.SetEnabled(true)
	event := &RLSAuditEvent{
		EventType:        RLSAuditEventDenied,
		Timestamp:        time.Now(),
		UserName:         "unauthorized_user",
		UserRoles:        []string{},
		DBID:             1,
		DBName:           "testdb",
		CollectionID:     100,
		CollectionName:   "testcoll",
		Action:           "insert",
		PolicyCount:      1,
		Allowed:          false,
		DenialReason:     "no matching permissive policy",
		ProcessingTimeMs: 2,
	}
	// Should not panic
	logger.LogAccessDenied(ctx, event)

	// Test with nil event
	logger.LogAccessDenied(ctx, nil)

	// Test when disabled
	logger.SetEnabled(false)
	logger.LogAccessDenied(ctx, event)
}

func TestRLSAuditLoggerLogInsertCheck(t *testing.T) {
	logger := GetRLSAuditLogger()

	// Save original state
	originalState := logger.IsEnabled()
	defer logger.SetEnabled(originalState)

	ctx := context.Background()

	logger.SetEnabled(true)

	// Test allowed insert
	allowedEvent := &RLSAuditEvent{
		EventType:        RLSAuditEventInsert,
		Timestamp:        time.Now(),
		UserName:         "testuser",
		UserRoles:        []string{"writer"},
		CollectionID:     100,
		CollectionName:   "testcoll",
		PolicyCount:      1,
		Allowed:          true,
		ProcessingTimeMs: 1,
	}
	logger.LogInsertCheck(ctx, allowedEvent)

	// Test denied insert
	deniedEvent := &RLSAuditEvent{
		EventType:        RLSAuditEventInsert,
		Timestamp:        time.Now(),
		UserName:         "unauthorized_user",
		UserRoles:        []string{},
		CollectionID:     100,
		CollectionName:   "testcoll",
		PolicyCount:      1,
		Allowed:          false,
		DenialReason:     "insert check failed",
		ProcessingTimeMs: 1,
	}
	logger.LogInsertCheck(ctx, deniedEvent)

	// Test with nil event
	logger.LogInsertCheck(ctx, nil)
}

func TestRLSAuditLoggerLogDeleteFilter(t *testing.T) {
	logger := GetRLSAuditLogger()

	// Save original state
	originalState := logger.IsEnabled()
	defer logger.SetEnabled(originalState)

	ctx := context.Background()

	logger.SetEnabled(true)
	event := &RLSAuditEvent{
		EventType:        RLSAuditEventDelete,
		Timestamp:        time.Now(),
		UserName:         "testuser",
		UserRoles:        []string{"writer"},
		CollectionID:     100,
		CollectionName:   "testcoll",
		PolicyCount:      1,
		OriginalExpr:     "status == 'inactive'",
		MergedExpr:       "(status == 'inactive') AND (owner_id == 'testuser')",
		ProcessingTimeMs: 3,
	}
	logger.LogDeleteFilter(ctx, event)

	// Test with nil event
	logger.LogDeleteFilter(ctx, nil)
}

func TestRLSAuditLoggerLogCacheInvalidation(t *testing.T) {
	logger := GetRLSAuditLogger()

	// Save original state
	originalState := logger.IsEnabled()
	defer logger.SetEnabled(originalState)

	ctx := context.Background()

	logger.SetEnabled(true)
	// Should not panic
	logger.LogCacheInvalidation(ctx, "CREATE_POLICY", "testdb", "testcoll", "policy1", "")
	logger.LogCacheInvalidation(ctx, "DROP_POLICY", "testdb", "testcoll", "policy1", "")
	logger.LogCacheInvalidation(ctx, "SET_USER_TAGS", "", "", "", "testuser")

	// Test when disabled
	logger.SetEnabled(false)
	logger.LogCacheInvalidation(ctx, "CREATE_POLICY", "testdb", "testcoll", "policy1", "")
}

func TestTruncateExpr(t *testing.T) {
	tests := []struct {
		name     string
		expr     string
		maxLen   int
		expected string
	}{
		{
			name:     "short expr",
			expr:     "id > 0",
			maxLen:   20,
			expected: "id > 0",
		},
		{
			name:     "exact length",
			expr:     "exactly20characters!",
			maxLen:   20,
			expected: "exactly20characters!",
		},
		{
			name:     "truncated",
			expr:     "this is a very long expression that needs truncation",
			maxLen:   20,
			expected: "this is a very long ...",
		},
		{
			name:     "empty string",
			expr:     "",
			maxLen:   10,
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := truncateExpr(tt.expr, tt.maxLen)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNewAuditEvent(t *testing.T) {
	event := NewAuditEvent(RLSAuditEventQuery, "testuser", []string{"admin", "reader"}, 1, 100)

	assert.NotNil(t, event)
	assert.Equal(t, RLSAuditEventQuery, event.EventType)
	assert.Equal(t, "testuser", event.UserName)
	assert.Equal(t, []string{"admin", "reader"}, event.UserRoles)
	assert.Equal(t, int64(1), event.DBID)
	assert.Equal(t, int64(100), event.CollectionID)
	assert.True(t, event.Allowed)
	assert.False(t, event.Timestamp.IsZero())
}

func TestRLSAuditEventFields(t *testing.T) {
	event := &RLSAuditEvent{
		EventType:        RLSAuditEventSearch,
		Timestamp:        time.Now(),
		UserName:         "user1",
		UserRoles:        []string{"role1"},
		DBID:             1,
		DBName:           "db1",
		CollectionID:     100,
		CollectionName:   "coll1",
		Action:           "search",
		PolicyCount:      3,
		OriginalExpr:     "original",
		MergedExpr:       "merged",
		Allowed:          true,
		DenialReason:     "",
		ProcessingTimeMs: 10,
	}

	assert.Equal(t, RLSAuditEventSearch, event.EventType)
	assert.Equal(t, "user1", event.UserName)
	assert.Equal(t, []string{"role1"}, event.UserRoles)
	assert.Equal(t, int64(1), event.DBID)
	assert.Equal(t, "db1", event.DBName)
	assert.Equal(t, int64(100), event.CollectionID)
	assert.Equal(t, "coll1", event.CollectionName)
	assert.Equal(t, "search", event.Action)
	assert.Equal(t, 3, event.PolicyCount)
	assert.Equal(t, "original", event.OriginalExpr)
	assert.Equal(t, "merged", event.MergedExpr)
	assert.True(t, event.Allowed)
	assert.Empty(t, event.DenialReason)
	assert.Equal(t, int64(10), event.ProcessingTimeMs)
}
