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

package ratelimitutil

import (
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/ratelimitutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type RateLimiterNode struct {
	limiters    *typeutil.ConcurrentMap[internalpb.RateType, *ratelimitutil.Limiter]
	quotaStates *typeutil.ConcurrentMap[milvuspb.QuotaState, commonpb.ErrorCode]
	level       internalpb.RateScope

	// db id, collection id or partition id, cluster id is 0 for the cluster level
	id int64

	// children will be databases if current level is cluster
	// children will be collections if current level is database
	// children will be partitions if current level is collection
	children map[int64]*RateLimiterNode
}

func NewRateLimiterNode() *RateLimiterNode {
	rln := &RateLimiterNode{
		limiters:    typeutil.NewConcurrentMap[internalpb.RateType, *ratelimitutil.Limiter](),
		quotaStates: typeutil.NewConcurrentMap[milvuspb.QuotaState, commonpb.ErrorCode](),
		children:    make(map[int64]*RateLimiterNode),
	}
	return rln
}

// Limit returns true, the request will be rejected.
// Otherwise, the request will pass.
func (rln *RateLimiterNode) Limit(rt internalpb.RateType, n int) (bool, float64) {
	limit, ok := rln.limiters.Get(rt)
	if !ok {
		return false, -1
	}
	return !limit.AllowN(time.Now(), n), float64(limit.Limit())
}

func (rln *RateLimiterNode) Cancel(rt internalpb.RateType, n int) {
	limit, ok := rln.limiters.Get(rt)
	if !ok {
		return
	}
	limit.Cancel(n)
}

func (rln *RateLimiterNode) Check(rt internalpb.RateType, n int) error {
	limit, rate := rln.Limit(rt, n)
	if rate == 0 {
		return rln.GetQuotaExceededError(rt)
	}
	if limit {
		return rln.GetRateLimitError(rate)
	}
	return nil
}

func (rln *RateLimiterNode) GetQuotaExceededError(rt internalpb.RateType) error {
	switch rt {
	case internalpb.RateType_DMLInsert, internalpb.RateType_DMLUpsert, internalpb.RateType_DMLDelete, internalpb.RateType_DMLBulkLoad:
		if errCode, ok := rln.quotaStates.Get(milvuspb.QuotaState_DenyToWrite); ok {
			return merr.WrapErrServiceQuotaExceeded(ratelimitutil.GetQuotaErrorString(errCode))
		}
	case internalpb.RateType_DQLSearch, internalpb.RateType_DQLQuery:
		if errCode, ok := rln.quotaStates.Get(milvuspb.QuotaState_DenyToRead); ok {
			return merr.WrapErrServiceQuotaExceeded(ratelimitutil.GetQuotaErrorString(errCode))
		}
	}
	return nil
}

func (rln *RateLimiterNode) GetRateLimitError(rate float64) error {
	return merr.WrapErrServiceRateLimit(rate, "request is rejected by grpc RateLimiter middleware, please retry later")
}

func TraverseRateLimiterTree(root *RateLimiterNode, fn1 func(internalpb.RateType, *ratelimitutil.Limiter) bool,
	fn2 func(state milvuspb.QuotaState, errCode commonpb.ErrorCode) bool,
) {
	if fn1 != nil {
		root.limiters.Range(fn1)
	}

	if fn2 != nil {
		root.quotaStates.Range(fn2)
	}
	if len(root.children) != 0 {
		for _, child := range root.children {
			TraverseRateLimiterTree(child, fn1, fn2)
		}
	}
}

func (rln *RateLimiterNode) AddChild(key int64, child *RateLimiterNode) {
	rln.children[key] = child
}

func (rln *RateLimiterNode) GetChild(key int64) *RateLimiterNode {
	return rln.children[key]
}

func (rln *RateLimiterNode) GetChildren() map[int64]*RateLimiterNode {
	return rln.children
}

func (rln *RateLimiterNode) GetLimiters() *typeutil.ConcurrentMap[internalpb.RateType, *ratelimitutil.Limiter] {
	return rln.limiters
}

func (rln *RateLimiterNode) SetLimiters(new *typeutil.ConcurrentMap[internalpb.RateType, *ratelimitutil.Limiter]) {
	rln.limiters = new
}

func (rln *RateLimiterNode) GetQuotaStates() *typeutil.ConcurrentMap[milvuspb.QuotaState, commonpb.ErrorCode] {
	return rln.quotaStates
}

func (rln *RateLimiterNode) SetQuotaStates(new *typeutil.ConcurrentMap[milvuspb.QuotaState, commonpb.ErrorCode]) {
	rln.quotaStates = new
}

func (rln *RateLimiterNode) GetID() int64 {
	return rln.id
}

// RateLimiterTree is implemented based on RateLimiterNode to operate multilevel rate limiters
//
// it contains the following four levels generally:
//
//	-> global level
//		-> database level
//			-> collection level
//				-> partition levelearl
type RateLimiterTree struct {
	root *RateLimiterNode
}

// NewRateLimiterTree returns a new RateLimiterTree.
func NewRateLimiterTree(root *RateLimiterNode) *RateLimiterTree {
	return &RateLimiterTree{root: root}
}

// GetRootLimiters get root limiters
func (m *RateLimiterTree) GetRootLimiters() *RateLimiterNode {
	return m.root
}

// SetLimitersAsRoot set limiter as a root of the limiter tree
func (m *RateLimiterTree) SetLimitersAsRoot(newRateLimiter func() *RateLimiterNode) {
	m.root = newRateLimiter()
}

// GetOrCreateDatabaseLimiters get limiter of database level, or create a database limiter if it doesn't exist.
func (m *RateLimiterTree) GetOrCreateDatabaseLimiters(dbID int64, newDBRateLimiter func() *RateLimiterNode) *RateLimiterNode {
	dbRateLimiters := m.root.GetChild(dbID)
	if dbRateLimiters != nil {
		return dbRateLimiters
	}
	dbRateLimiters = newDBRateLimiter()
	m.root.AddChild(dbID, dbRateLimiters)
	return dbRateLimiters
}

func (m *RateLimiterTree) GetCollectionLimiters(dbID, collectionID int64) *RateLimiterNode {
	dbRateLimiters := m.root.GetChild(dbID)

	// database rate limiter not found
	if dbRateLimiters == nil {
		return nil
	}
	return dbRateLimiters.GetChild(collectionID)
}

// GetOrCreateCollectionLimiters create limiter of collection level for all rate types and rate scopes.
// create a database rate limiters if db rate limiter does not exist
func (m *RateLimiterTree) GetOrCreateCollectionLimiters(dbID, collectionID int64,
	newDBRateLimiter func() *RateLimiterNode, newCollectionRateLimiter func() *RateLimiterNode,
) *RateLimiterNode {
	collectionRateLimiters := m.GetCollectionLimiters(dbID, collectionID)
	if collectionRateLimiters != nil {
		return collectionRateLimiters
	}

	collectionRateLimiters = newCollectionRateLimiter()

	dbRateLimiters := m.GetOrCreateDatabaseLimiters(dbID, newDBRateLimiter)
	dbRateLimiters.AddChild(collectionID, collectionRateLimiters)
	return collectionRateLimiters
}

// It checks if the rate limiters exist for the database, collection, and partition,
// returns the corresponding rate limiter tree.
func (m *RateLimiterTree) getPartitionLimiters(dbID, collectionID, partitionID int64) *RateLimiterNode {
	dbRateLimiters := m.root.GetChild(dbID)

	// database rate limiter not found
	if dbRateLimiters == nil {
		return nil
	}

	collectionRateLimiters := dbRateLimiters.GetChild(collectionID)

	// collection rate limiter not found
	if collectionRateLimiters == nil {
		return nil
	}

	return collectionRateLimiters.GetChild(partitionID)
}

// GetOrCreatePartitionLimiters create limiter of partition level for all rate types and rate scopes.
// create a database rate limiters if db rate limiter does not exist
// create a collection rate limiters if collection rate limiter does not exist
func (m *RateLimiterTree) GetOrCreatePartitionLimiters(dbID int64, collectionID int64, partitionID int64,
	newDBRateLimiter func() *RateLimiterNode, newCollectionRateLimiter func() *RateLimiterNode,
	newPartRateLimiter func() *RateLimiterNode,
) *RateLimiterNode {
	partRateLimiters := m.getPartitionLimiters(dbID, collectionID, partitionID)
	if partRateLimiters != nil {
		return partRateLimiters
	}

	partRateLimiters = newPartRateLimiter()
	collectionRateLimiters := m.GetOrCreateCollectionLimiters(dbID, collectionID, newDBRateLimiter, newCollectionRateLimiter)
	collectionRateLimiters.AddChild(partitionID, partRateLimiters)
	return partRateLimiters
}
