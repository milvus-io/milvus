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

package segments

import (
	"context"
	"time"
)

// PhysicalLoadTiming carries optional per-segment recovery timing through the
// QueryView physical load call chain.
type PhysicalLoadTiming struct {
	NewSegment         time.Duration
	LoadSegment        time.Duration
	SealedLoad         time.Duration
	SealedPrepare      time.Duration
	SealedLoadPoolWait time.Duration
	LocalSegmentLoad   time.Duration
	CSegmentLoad       time.Duration
	SyncJSONStats      time.Duration
	SealedPostLoad     time.Duration
	DeltaLogs          time.Duration
	PKCandidate        time.Duration
}

type physicalLoadTimingKey struct{}

func WithPhysicalLoadTiming(ctx context.Context, timing *PhysicalLoadTiming) context.Context {
	if timing == nil {
		return ctx
	}
	return context.WithValue(ctx, physicalLoadTimingKey{}, timing)
}

func PhysicalLoadTimingFromContext(ctx context.Context) *PhysicalLoadTiming {
	timing, _ := ctx.Value(physicalLoadTimingKey{}).(*PhysicalLoadTiming)
	return timing
}
