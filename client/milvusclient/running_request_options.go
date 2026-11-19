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

package milvusclient

import (
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

// ListRunningRequestsOption is the interface for ListRunningRequests.
type ListRunningRequestsOption interface {
	Request() *milvuspb.ListRunningRequestsRequest
}

type listRunningRequestsOption struct {
	dbName         string
	collectionName string
	user           string
	minElapsed     time.Duration
}

func (opt *listRunningRequestsOption) Request() *milvuspb.ListRunningRequestsRequest {
	return &milvuspb.ListRunningRequestsRequest{
		DbName:         opt.dbName,
		CollectionName: opt.collectionName,
		User:           opt.user,
		MinElapsedMs:   opt.minElapsed.Milliseconds(),
	}
}

// WithDBName keeps only the requests on this database.
func (opt *listRunningRequestsOption) WithDBName(dbName string) *listRunningRequestsOption {
	opt.dbName = dbName
	return opt
}

// WithCollectionName keeps only the requests on this collection.
func (opt *listRunningRequestsOption) WithCollectionName(collectionName string) *listRunningRequestsOption {
	opt.collectionName = collectionName
	return opt
}

// WithUser keeps only the requests issued by this user. A caller without
// the ListRunningRequests privilege sees only their own requests whatever
// is set here.
func (opt *listRunningRequestsOption) WithUser(user string) *listRunningRequestsOption {
	opt.user = user
	return opt
}

// WithMinElapsed keeps only the requests that have been running at least
// this long.
func (opt *listRunningRequestsOption) WithMinElapsed(minElapsed time.Duration) *listRunningRequestsOption {
	opt.minElapsed = minElapsed
	return opt
}

// NewListRunningRequestsOption creates an option with no filter: every
// running request on every database. The filters are combined with AND;
// an unset filter is no constraint, so the client's current database is not
// implied.
func NewListRunningRequestsOption() *listRunningRequestsOption {
	return &listRunningRequestsOption{}
}

// CancelRequestsOption is the interface for CancelRequests.
type CancelRequestsOption interface {
	Request() *milvuspb.CancelRequestsRequest
}

type cancelRequestsOption struct {
	requestIDs []string
	reason     string
	user       string
}

func (opt *cancelRequestsOption) Request() *milvuspb.CancelRequestsRequest {
	return &milvuspb.CancelRequestsRequest{
		RequestIds: opt.requestIDs,
		Reason:     opt.reason,
		User:       opt.user,
	}
}

// WithReason records free text in the audit log and returns it to the
// client whose request was canceled.
func (opt *cancelRequestsOption) WithReason(reason string) *cancelRequestsOption {
	opt.reason = reason
	return opt
}

// WithUser cancels only the requests issued by this user; the ids of other
// users' requests are reported as not found. A caller without the
// CancelRequests privilege can cancel only their own requests whatever is
// set here.
func (opt *cancelRequestsOption) WithUser(user string) *cancelRequestsOption {
	opt.user = user
	return opt
}

// NewCancelRequestsOption creates an option that cancels the given ids, as
// reported by ListRunningRequests. An empty list is rejected by
// CancelRequests without calling the server.
func NewCancelRequestsOption(requestIDs ...string) *cancelRequestsOption {
	return &cancelRequestsOption{requestIDs: requestIDs}
}
