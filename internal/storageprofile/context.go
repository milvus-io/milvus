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

package storageprofile

import "context"

type contextKey int

const (
	attributionContextKey contextKey = iota
	recorderContextKey
	suppressionContextKey
	profileLevelContextKey
)

func WithAttribution(ctx context.Context, attribution Attribution) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, attributionContextKey, attribution.Bounded())
}

func WithProfileLevel(ctx context.Context, level StorageProfileLevel) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, profileLevelContextKey, level)
}

func ProfileLevelFromContext(ctx context.Context) StorageProfileLevel {
	if ctx == nil {
		return StorageProfileDisabled
	}
	level, _ := ctx.Value(profileLevelContextKey).(StorageProfileLevel)
	return level
}

func AttributionFromContext(ctx context.Context) Attribution {
	if ctx == nil {
		return Attribution{}
	}
	attribution, ok := ctx.Value(attributionContextKey).(Attribution)
	if !ok {
		return Attribution{}
	}
	return attribution
}

func WithPhase(ctx context.Context, phase WorkloadPhase, role StorageRole) context.Context {
	attribution := AttributionFromContext(ctx)
	attribution.Phase = phase
	attribution.StorageRole = role
	return WithAttribution(ctx, attribution)
}

func WithBackendKind(ctx context.Context, backend BackendKind) context.Context {
	attribution := AttributionFromContext(ctx)
	if attribution.BackendKind == BackendKindUnknown {
		attribution.BackendKind = backend
	}
	return WithAttribution(ctx, attribution)
}

func WithDefaultAttribution(ctx context.Context, defaults Attribution) context.Context {
	attribution := AttributionFromContext(ctx)
	if attribution.ScopeType == ScopeTypeUnknown {
		attribution.ScopeType = defaults.ScopeType
	}
	if attribution.TenantID == "" {
		attribution.TenantID = defaults.TenantID
	}
	if attribution.UserID == "" {
		attribution.UserID = defaults.UserID
	}
	if attribution.RequestID == "" {
		attribution.RequestID = defaults.RequestID
	}
	if attribution.RequestType == "" {
		attribution.RequestType = defaults.RequestType
	}
	if attribution.TraceID == "" {
		attribution.TraceID = defaults.TraceID
	}
	if attribution.TaskID == "" {
		attribution.TaskID = defaults.TaskID
	}
	if attribution.TaskAttempt == 0 {
		attribution.TaskAttempt = defaults.TaskAttempt
	}
	if attribution.Component == "" || attribution.Component == "unknown" {
		attribution.Component = defaults.Component
	}
	if attribution.NodeID == 0 {
		attribution.NodeID = defaults.NodeID
	}
	if attribution.CollectionID == 0 {
		attribution.CollectionID = defaults.CollectionID
	}
	if attribution.WorkloadClass == WorkloadClassUnknown {
		attribution.WorkloadClass = defaults.WorkloadClass
	}
	if attribution.WorkloadKind == WorkloadKindUnknown {
		attribution.WorkloadKind = defaults.WorkloadKind
	}
	if attribution.WorkloadSubtype == WorkloadSubtypeUnknown {
		attribution.WorkloadSubtype = defaults.WorkloadSubtype
	}
	if attribution.Phase == WorkloadPhaseUnknown {
		attribution.Phase = defaults.Phase
	}
	if attribution.StorageRole == StorageRoleUnknown {
		attribution.StorageRole = defaults.StorageRole
	}
	if attribution.BackendKind == BackendKindUnknown {
		attribution.BackendKind = defaults.BackendKind
	}
	return WithAttribution(ctx, attribution)
}

func WithRecorder(ctx context.Context, recorder Recorder) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if recorder == nil {
		recorder = NoopRecorder()
	}
	return context.WithValue(ctx, recorderContextKey, recorder)
}

func RecorderFromContext(ctx context.Context) Recorder {
	if ctx == nil {
		return NoopRecorder()
	}
	recorder, ok := ctx.Value(recorderContextKey).(Recorder)
	if !ok || recorder == nil {
		return NoopRecorder()
	}
	return recorder
}

func HasActiveRecorder(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	recorder, ok := ctx.Value(recorderContextKey).(Recorder)
	if !ok || recorder == nil {
		return false
	}
	_, noop := recorder.(noopRecorder)
	return !noop
}

func WithSuppressed(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, suppressionContextKey, true)
}

func IsSuppressed(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	suppressed, _ := ctx.Value(suppressionContextKey).(bool)
	return suppressed
}
