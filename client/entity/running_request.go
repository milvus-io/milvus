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

package entity

import "time"

// RunningRequest is a Search, HybridSearch or Query that a proxy is
// executing, as reported by ListRunningRequests. ID is the handle that
// CancelRequests takes: an opaque string such as "25-to03zu-rs", to be
// passed back unchanged.
type RunningRequest struct {
	ID             string
	ProxyID        int64
	Type           string // "Search", "HybridSearch" or "Query"
	DBName         string
	CollectionName string
	User           string
	// ClientAddr is the address of the client that issued the request.
	ClientAddr string
	NQ         int64
	TopK       int64
	// Expr is the filter expression, truncated to a bounded prefix.
	Expr      string
	StartTime time.Time
	// Queued is the scheduler queue wait of the request's first task; zero
	// before that task starts.
	Queued  time.Duration
	Elapsed time.Duration
	// State is "Queued", "Running" or "Canceling". A canceling request has
	// been told to stop and leaves the list at its next cancellation check.
	State string
	// TaskIDs are the scheduler task ids the request spawned, for
	// correlating with proxy logs.
	TaskIDs []int64
	TraceID string
	// Cancellable is false for a request type that is listed but cannot be
	// canceled.
	Cancellable bool
}

// RequestNodeResult is how one proxy answered a cluster-wide list or
// cancel. A proxy with a non-nil Err contributed nothing: its requests are
// missing from a list, and the ids it holds could not be canceled.
type RequestNodeResult struct {
	NodeID int64
	Err    error
	// Unimplemented is true when the proxy predates the feature, i.e. a
	// rolling upgrade is in progress.
	Unimplemented bool
}
