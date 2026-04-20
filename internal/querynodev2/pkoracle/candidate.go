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

package pkoracle

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// Candidate is the interface for pk oracle candidate.
//
// Core methods (all implementers must provide meaningful logic):
//
//	MayPkExist, BatchPkExist, ID, Partition, Type, PkCandidateExist
//
// Resource lifecycle methods (no-op is a valid implementation for candidates
// that don't track resources, e.g. ExternalSegmentCandidate, candidateKey):
//
//	UpdatePkCandidate, Stats, Charge, Refund
type Candidate interface {
	// MayPkExist checks whether primary key could exists in this candidate.
	MayPkExist(lc *storage.LocationsCache) bool
	BatchPkExist(lc *storage.BatchLocationsCache) []bool

	ID() int64
	Partition() int64
	Type() commonpb.SegmentState

	// PkCandidateExist reports whether the candidate has been initialized with PK data.
	// This is a precondition for MayPkExist / BatchPkExist calls.
	// BloomFilterSet: true when bloom filter data has been loaded.
	// ExternalSegmentCandidate: always true (deterministic PK-to-segment mapping).
	PkCandidateExist() bool

	// UpdatePkCandidate feeds new primary keys into the candidate.
	// BloomFilterSet: updates the bloom filter with the provided keys.
	// ExternalSegmentCandidate: no-op (virtual PKs are deterministic).
	UpdatePkCandidate(pks []storage.PrimaryKey)

	// Stats returns PK statistics (min/max PK) if available, nil otherwise.
	Stats() *storage.PkStatistics

	// GetMinPk returns the global minimum PK across all statistics (current + historical),
	// or nil if no statistics are available.
	GetMinPk() *storage.PrimaryKey

	// GetMaxPk returns the global maximum PK across all statistics (current + historical),
	// or nil if no statistics are available.
	GetMaxPk() *storage.PrimaryKey

	// Charge charges memory resources consumed by this candidate.
	// No-op is valid for candidates without trackable resources.
	Charge()

	// Refund releases memory resources previously charged by this candidate.
	// No-op is valid for candidates without trackable resources.
	Refund()
}

type candidateWithWorker struct {
	Candidate
	workerID int64
}

// CandidateFilter filter type for candidate.
type CandidateFilter func(candidate candidateWithWorker) bool

// WithSegmentType returns CandiateFilter with provided segment type.
func WithSegmentType(typ commonpb.SegmentState) CandidateFilter {
	return func(candidate candidateWithWorker) bool {
		return candidate.Type() == typ
	}
}

// WithWorkerID returns CandidateFilter with provided worker id.
func WithWorkerID(workerID int64) CandidateFilter {
	return func(candidate candidateWithWorker) bool {
		return candidate.workerID == workerID ||
			workerID == -1 // wildcard for offline node
	}
}

// WithSegmentIDs returns CandidateFilter with provided segment ids.
func WithSegmentIDs(segmentIDs ...int64) CandidateFilter {
	set := typeutil.NewSet[int64]()
	set.Insert(segmentIDs...)
	return func(candidate candidateWithWorker) bool {
		return set.Contain(candidate.ID())
	}
}

// WithPartitionID returns CandidateFilter with provided partitionID.
func WithPartitionID(partitionID int64) CandidateFilter {
	return func(candidate candidateWithWorker) bool {
		return candidate.Partition() == partitionID || partitionID == common.AllPartitionsID
	}
}
