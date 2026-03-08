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

// pkoracle package contains pk - segment mapping logic.
package pkoracle

import (
	"fmt"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// PkOracle interface for pk oracle.
type PkOracle interface {
	// GetCandidates returns segment candidates of which pk might belongs to.
	Get(pk storage.PrimaryKey, filters ...CandidateFilter) ([]int64, error)
	BatchGet(pks []storage.PrimaryKey, filters ...CandidateFilter) map[int64][]bool
	// RegisterCandidate adds candidate into pkOracle.
	Register(candidate Candidate, workerID int64) error
	// RemoveCandidate removes candidate and returns the removed candidates.
	Remove(filters ...CandidateFilter) []Candidate
	// CheckCandidate checks whether candidate with provided key exists.
	Exists(candidate Candidate, workerID int64) bool
	// Range iterates over all candidates without removing them.
	Range(fn func(candidate Candidate) bool)
	// RefundRemoved refunds resources for BloomFilterSet candidates.
	RefundRemoved(candidates []Candidate)
	// RemoveAndRefundAll removes all candidates and refunds resources for BloomFilterSet candidates.
	// Used during shutdown to clean up and refund resources.
	RemoveAndRefundAll()
}

var _ PkOracle = (*pkOracle)(nil)

// pkOracle implementation.
type pkOracle struct {
	candidates *typeutil.ConcurrentMap[string, candidateWithWorker]
}

// Get implements PkOracle.
func (pko *pkOracle) Get(pk storage.PrimaryKey, filters ...CandidateFilter) ([]int64, error) {
	var result []int64
	lc := storage.NewLocationsCache(pk)
	pko.candidates.Range(func(key string, candidate candidateWithWorker) bool {
		for _, filter := range filters {
			if !filter(candidate) {
				return true
			}
		}

		if candidate.MayPkExist(lc) {
			result = append(result, candidate.ID())
		}
		return true
	})

	return result, nil
}

func (pko *pkOracle) BatchGet(pks []storage.PrimaryKey, filters ...CandidateFilter) map[int64][]bool {
	result := make(map[int64][]bool)

	lc := storage.NewBatchLocationsCache(pks)
	pko.candidates.Range(func(key string, candidate candidateWithWorker) bool {
		for _, filter := range filters {
			if !filter(candidate) {
				return true
			}
		}

		hits := candidate.BatchPkExist(lc)
		result[candidate.ID()] = hits
		return true
	})

	return result
}

func (pko *pkOracle) candidateKey(candidate Candidate, workerID int64) string {
	return fmt.Sprintf("%s-%d-%d", candidate.Type().String(), workerID, candidate.ID())
}

// Register register candidate
func (pko *pkOracle) Register(candidate Candidate, workerID int64) error {
	pko.candidates.Insert(pko.candidateKey(candidate, workerID), candidateWithWorker{
		Candidate: candidate,
		workerID:  workerID,
	})

	return nil
}

// Remove removes candidate from pko and returns the removed candidates.
func (pko *pkOracle) Remove(filters ...CandidateFilter) []Candidate {
	var removed []Candidate
	pko.candidates.Range(func(key string, candidate candidateWithWorker) bool {
		for _, filter := range filters {
			if !filter(candidate) {
				return true
			}
		}
		if _, ok := pko.candidates.GetAndRemove(pko.candidateKey(candidate, candidate.workerID)); ok {
			removed = append(removed, candidate.Candidate)
		}
		return true
	})

	return removed
}

func (pko *pkOracle) Exists(candidate Candidate, workerID int64) bool {
	_, ok := pko.candidates.Get(pko.candidateKey(candidate, workerID))
	return ok
}

// Range iterates over all candidates without removing them.
func (pko *pkOracle) Range(fn func(candidate Candidate) bool) {
	pko.candidates.Range(func(key string, candidate candidateWithWorker) bool {
		return fn(candidate.Candidate)
	})
}

// RefundRemoved refunds resources for BloomFilterSet candidates.
// For candidates that are not BloomFilterSet (e.g., LocalSegment), this is a no-op.
func (pko *pkOracle) RefundRemoved(candidates []Candidate) {
	for _, candidate := range candidates {
		if bfs, ok := candidate.(*BloomFilterSet); ok {
			bfs.Refund()
		}
	}
}

// RemoveAndRefundAll removes all candidates and refunds resources for BloomFilterSet candidates.
// Used during shutdown to clean up and refund resources.
func (pko *pkOracle) RemoveAndRefundAll() {
	removed := pko.Remove()
	for _, candidate := range removed {
		if bfs, ok := candidate.(*BloomFilterSet); ok {
			bfs.Refund()
		}
	}
}

// NewPkOracle returns pkOracle as PkOracle interface.
func NewPkOracle() PkOracle {
	return &pkOracle{
		candidates: typeutil.NewConcurrentMap[string, candidateWithWorker](),
	}
}
