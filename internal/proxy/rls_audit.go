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
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

// RLSAuditEventType represents the type of RLS audit event
type RLSAuditEventType string

const (
	// RLSAuditEventQuery indicates RLS was applied to a query
	RLSAuditEventQuery RLSAuditEventType = "QUERY"
	// RLSAuditEventSearch indicates RLS was applied to a search
	RLSAuditEventSearch RLSAuditEventType = "SEARCH"
	// RLSAuditEventInsert indicates RLS was evaluated for an insert
	RLSAuditEventInsert RLSAuditEventType = "INSERT"
	// RLSAuditEventDelete indicates RLS was applied to a delete
	RLSAuditEventDelete RLSAuditEventType = "DELETE"
	// RLSAuditEventUpsert indicates RLS was evaluated for an upsert
	RLSAuditEventUpsert RLSAuditEventType = "UPSERT"
	// RLSAuditEventDenied indicates RLS denied an operation
	RLSAuditEventDenied RLSAuditEventType = "DENIED"
)

// RLSAuditEvent represents an RLS audit event
type RLSAuditEvent struct {
	EventType        RLSAuditEventType
	Timestamp        time.Time
	UserName         string
	UserRoles        []string
	DBID             int64
	DBName           string
	CollectionID     int64
	CollectionName   string
	Action           string
	PolicyCount      int
	OriginalExpr     string
	MergedExpr       string
	Allowed          bool
	DenialReason     string
	ProcessingTimeMs int64
}

// RLSAuditLogger provides structured audit logging for RLS operations
type RLSAuditLogger struct {
	enabled atomic.Bool
}

// globalRLSAuditLogger is the singleton audit logger
var globalRLSAuditLogger = newRLSAuditLogger()

func newRLSAuditLogger() *RLSAuditLogger {
	l := &RLSAuditLogger{}
	l.enabled.Store(true)
	return l
}

// GetRLSAuditLogger returns the global RLS audit logger
func GetRLSAuditLogger() *RLSAuditLogger {
	return globalRLSAuditLogger
}

// SetEnabled enables or disables RLS audit logging
func (l *RLSAuditLogger) SetEnabled(enabled bool) {
	l.enabled.Store(enabled)
}

// IsEnabled returns whether audit logging is enabled
func (l *RLSAuditLogger) IsEnabled() bool {
	return l.enabled.Load()
}

// LogPolicyApplied logs when RLS policies are applied to a query/search
func (l *RLSAuditLogger) LogPolicyApplied(ctx context.Context, event *RLSAuditEvent) {
	if !l.enabled.Load() || event == nil {
		return
	}

	logger := log.Ctx(ctx)

	// Log at Info level for audit trail
	logger.Info("RLS policy applied",
		zap.String("eventType", string(event.EventType)),
		zap.String("user", event.UserName),
		zap.Strings("roles", event.UserRoles),
		zap.Int64("dbId", event.DBID),
		zap.String("dbName", event.DBName),
		zap.Int64("collectionId", event.CollectionID),
		zap.String("collectionName", event.CollectionName),
		zap.String("action", event.Action),
		zap.Int("policyCount", event.PolicyCount),
		zap.String("originalExpr", truncateExpr(event.OriginalExpr, 200)),
		zap.String("mergedExpr", truncateExpr(event.MergedExpr, 200)),
		zap.Bool("allowed", event.Allowed),
		zap.Int64("processingTimeMs", event.ProcessingTimeMs),
	)
}

// LogAccessDenied logs when RLS policies deny access
func (l *RLSAuditLogger) LogAccessDenied(ctx context.Context, event *RLSAuditEvent) {
	if !l.enabled.Load() || event == nil {
		return
	}

	logger := log.Ctx(ctx)

	// Log at Warn level for denied access
	logger.Warn("RLS access denied",
		zap.String("eventType", string(RLSAuditEventDenied)),
		zap.String("user", event.UserName),
		zap.Strings("roles", event.UserRoles),
		zap.Int64("dbId", event.DBID),
		zap.String("dbName", event.DBName),
		zap.Int64("collectionId", event.CollectionID),
		zap.String("collectionName", event.CollectionName),
		zap.String("action", event.Action),
		zap.Int("policyCount", event.PolicyCount),
		zap.String("reason", event.DenialReason),
		zap.Int64("processingTimeMs", event.ProcessingTimeMs),
	)
}

// LogInsertCheck logs RLS check for insert operations
func (l *RLSAuditLogger) LogInsertCheck(ctx context.Context, event *RLSAuditEvent) {
	if !l.enabled.Load() || event == nil {
		return
	}

	logger := log.Ctx(ctx)

	if event.Allowed {
		logger.Info("RLS insert check passed",
			zap.String("eventType", string(RLSAuditEventInsert)),
			zap.String("user", event.UserName),
			zap.Strings("roles", event.UserRoles),
			zap.Int64("dbId", event.DBID),
			zap.String("dbName", event.DBName),
			zap.Int64("collectionId", event.CollectionID),
			zap.String("collectionName", event.CollectionName),
			zap.Int("policyCount", event.PolicyCount),
			zap.Int64("processingTimeMs", event.ProcessingTimeMs),
		)
	} else {
		l.LogAccessDenied(ctx, event)
	}
}

// LogDeleteFilter logs RLS filter applied to delete operations
func (l *RLSAuditLogger) LogDeleteFilter(ctx context.Context, event *RLSAuditEvent) {
	if !l.enabled.Load() || event == nil {
		return
	}

	logger := log.Ctx(ctx)

	logger.Info("RLS delete filter applied",
		zap.String("eventType", string(RLSAuditEventDelete)),
		zap.String("user", event.UserName),
		zap.Strings("roles", event.UserRoles),
		zap.Int64("dbId", event.DBID),
		zap.String("dbName", event.DBName),
		zap.Int64("collectionId", event.CollectionID),
		zap.String("collectionName", event.CollectionName),
		zap.Int("policyCount", event.PolicyCount),
		zap.String("originalExpr", truncateExpr(event.OriginalExpr, 200)),
		zap.String("mergedExpr", truncateExpr(event.MergedExpr, 200)),
		zap.Int64("processingTimeMs", event.ProcessingTimeMs),
	)
}

// LogCacheInvalidation logs RLS cache invalidation events
func (l *RLSAuditLogger) LogCacheInvalidation(ctx context.Context, opType string, dbName, collectionName, policyName, userName string) {
	if !l.enabled.Load() {
		return
	}

	logger := log.Ctx(ctx)

	logger.Info("RLS cache invalidated",
		zap.String("opType", opType),
		zap.String("dbName", dbName),
		zap.String("collectionName", collectionName),
		zap.String("policyName", policyName),
		zap.String("userName", userName),
		zap.Time("timestamp", time.Now()),
	)
}

// truncateExpr truncates expression for logging if too long
func truncateExpr(expr string, maxLen int) string {
	runes := []rune(expr)
	if len(runes) <= maxLen {
		return expr
	}
	return string(runes[:maxLen]) + "..."
}

// NewAuditEvent creates a new audit event with common fields
func NewAuditEvent(eventType RLSAuditEventType, userName string, userRoles []string, dbID, collectionID int64) *RLSAuditEvent {
	return &RLSAuditEvent{
		EventType:    eventType,
		Timestamp:    time.Now(),
		UserName:     userName,
		UserRoles:    userRoles,
		DBID:         dbID,
		CollectionID: collectionID,
		Allowed:      true,
	}
}
