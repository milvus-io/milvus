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

package meta

import (
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// RetrieveSegment selects a complete, non-overlapping recovery frontier,
// retaining ready ancestors while compaction outputs are not yet loadable.
func RetrieveSegment(validSegmentInfos map[int64]*SegmentInfo,
	flushedIDs, droppedIDs typeutil.UniqueSet,
	segmentIndexed, fallbackParentReady func(segID UniqueID) bool,
) (typeutil.UniqueSet, typeutil.UniqueSet) {
	// A recovered view may contain both compactTo segments and still-live
	// compactFrom segments when an ordered compaction metadata update is
	// interrupted. CompactTo segments are published before any compactFrom
	// segment is retired, so normalize the immutable input frontier as follows:
	//   - all direct parents present: retirement has not started and the output
	//     set may be incomplete; keep the parents and remove the child;
	//   - only some direct parents present: retirement has started, which proves
	//     the output set was fully published; keep the child and remove the
	//     remaining parents.
	// Each child can be evaluated independently. M:N outputs share the same
	// CompactionFrom set, so they make the same decision against this snapshot.
	initialFlushedIDs := typeutil.NewUniqueSet(flushedIDs.Collect()...)
	removeFromView := make(typeutil.UniqueSet)
	for id := range initialFlushedIDs {
		segment := validSegmentInfos[id]
		if segment == nil || len(segment.GetCompactionFrom()) == 0 {
			continue
		}

		presentParents := make(typeutil.UniqueSet)
		for _, parentID := range segment.GetCompactionFrom() {
			if initialFlushedIDs.Contain(parentID) {
				presentParents.Insert(parentID)
			}
		}

		switch len(presentParents) {
		case len(segment.GetCompactionFrom()):
			removeFromView.Insert(id)
		case 0:
		default:
			removeFromView.Insert(presentParents.Collect()...)
		}
	}
	initialFlushedIDs.Remove(removeFromView.Collect()...)
	flushedIDs = initialFlushedIDs

	allParentsReady := func(ids ...UniqueID) bool {
		for _, id := range ids {
			seg, ok := validSegmentInfos[id]
			if !ok || seg == nil || seg.GetIsInvisible() || !fallbackParentReady(id) {
				return false
			}
		}
		return true
	}

	// Select the initial fallbacks in one pass. Every expansion, including the
	// ones added later to resolve overlap, must use a complete ready parent set.
	expanded := make(typeutil.UniqueSet)
	for id := range flushedIDs {
		segment := validSegmentInfos[id]
		compactionFrom := segment.GetCompactionFrom()
		if len(compactionFrom) == 0 || segmentIndexed(id) || !allParentsReady(compactionFrom...) {
			continue
		}
		expanded.Insert(id)
	}

	buildCandidates := func() (typeutil.UniqueSet, map[UniqueID]typeutil.UniqueSet) {
		candidates := make(typeutil.UniqueSet)
		fallbackOwners := make(map[UniqueID]typeutil.UniqueSet)
		type candidateVisit struct {
			id     UniqueID
			rootID UniqueID
		}
		var visited map[candidateVisit]struct{}
		var emit func(id, rootID UniqueID, fromFallback bool)
		emit = func(id, rootID UniqueID, fromFallback bool) {
			segment, ok := validSegmentInfos[id]
			// DataView admission can certify a complete ancestor frontier behind
			// an unready intermediate. Flatten it without emitting that intermediate.
			// Legacy callers only admit indexed direct parents, so stop there.
			if ok && segment != nil && (expanded.Contain(id) ||
				(fromFallback && !segmentIndexed(id) && len(segment.GetCompactionFrom()) > 0 && allParentsReady(segment.GetCompactionFrom()...))) {
				if visited == nil {
					visited = make(map[candidateVisit]struct{})
				}
				visit := candidateVisit{id: id, rootID: rootID}
				if _, ok := visited[visit]; ok {
					return
				}
				visited[visit] = struct{}{}

				for _, parentID := range segment.GetCompactionFrom() {
					emit(parentID, rootID, true)
				}
				return
			}

			candidates.Insert(id)
			if fromFallback {
				if fallbackOwners[id] == nil {
					fallbackOwners[id] = make(typeutil.UniqueSet)
				}
				fallbackOwners[id].Insert(rootID)
			}
		}
		for id := range flushedIDs {
			emit(id, id, false)
		}
		return candidates, fallbackOwners
	}

	type ancestorCoverage int
	const (
		noAncestorCoverage ancestorCoverage = iota
		partialAncestorCoverage
		completeAncestorCoverage
	)

	type coverageResult struct {
		coverage ancestorCoverage
		owners   typeutil.UniqueSet
	}
	newCoverageChecker := func(candidates typeutil.UniqueSet, fallbackOwners map[UniqueID]typeutil.UniqueSet) func(UniqueID) coverageResult {
		coverageCache := make(map[UniqueID]coverageResult)
		visiting := make(typeutil.UniqueSet)
		trackOwners := fallbackOwners != nil
		var coverage func(UniqueID) coverageResult
		coverage = func(id UniqueID) coverageResult {
			if cached, ok := coverageCache[id]; ok {
				return cached
			}
			if visiting.Contain(id) {
				return coverageResult{coverage: noAncestorCoverage}
			}

			segment, ok := validSegmentInfos[id]
			if !ok || segment == nil || len(segment.GetCompactionFrom()) == 0 {
				return coverageResult{coverage: noAncestorCoverage}
			}
			visiting.Insert(id)
			defer visiting.Remove(id)

			result := coverageResult{}
			if trackOwners {
				result.owners = make(typeutil.UniqueSet)
			}
			anyCovered := false
			allCovered := true
			for _, parentID := range segment.GetCompactionFrom() {
				parentResult := coverageResult{coverage: noAncestorCoverage}
				if candidates.Contain(parentID) {
					parentResult.coverage = completeAncestorCoverage
					if trackOwners {
						parentResult.owners = fallbackOwners[parentID]
					}
				} else {
					parentResult = coverage(parentID)
				}
				anyCovered = anyCovered || parentResult.coverage != noAncestorCoverage
				allCovered = allCovered && parentResult.coverage == completeAncestorCoverage
				if trackOwners {
					for ownerID := range parentResult.owners {
						result.owners.Insert(ownerID)
					}
				}
			}

			switch {
			case allCovered:
				result.coverage = completeAncestorCoverage
			case anyCovered:
				result.coverage = partialAncestorCoverage
			default:
				result.coverage = noAncestorCoverage
			}
			coverageCache[id] = result
			return result
		}
		return coverage
	}

	// Close partial overlaps created by M:N compactions. Expand the descendant
	// when all of its direct parents are ready; otherwise cancel the root
	// fallback that introduced the overlapping ancestors and keep its child.
	var candidates typeutil.UniqueSet
	blockedRoots := make(typeutil.UniqueSet)
	for {
		var fallbackOwners map[UniqueID]typeutil.UniqueSet
		candidates, fallbackOwners = buildCandidates()
		coverage := newCoverageChecker(candidates, fallbackOwners)
		toExpand := make(typeutil.UniqueSet)
		toCancel := make(typeutil.UniqueSet)
		hasPartialCoverage := false
		for id := range candidates {
			result := coverage(id)
			if result.coverage != partialAncestorCoverage {
				continue
			}
			hasPartialCoverage = true
			segment := validSegmentInfos[id]
			if segment != nil && len(segment.GetCompactionFrom()) > 0 && !blockedRoots.Contain(id) && allParentsReady(segment.GetCompactionFrom()...) {
				toExpand.Insert(id)
			} else {
				toCancel.Insert(result.owners.Collect()...)
			}
		}
		if !hasPartialCoverage {
			break
		}

		changed := false
		for id := range toExpand {
			if !toCancel.Contain(id) {
				expanded.Insert(id)
				changed = true
			}
		}
		for id := range toCancel {
			expanded.Remove(id)
			blockedRoots.Insert(id)
			changed = true
		}
		if !changed {
			// The original leaf frontier is the safe fallback for malformed
			// metadata that cannot attribute a partial overlap to an expansion.
			candidates = typeutil.NewUniqueSet(flushedIDs.Collect()...)
			break
		}
	}

	// Deduplicate against the immutable candidate frontier. An ancestor may be
	// selected by another branch of an M:N compaction, so walk the complete
	// ancestry here, but never introduce additional segments during this phase.
	coverage := newCoverageChecker(candidates, nil)
	finalFrontier := make(typeutil.UniqueSet)
	for id := range candidates {
		if coverage(id).coverage != completeAncestorCoverage {
			finalFrontier.Insert(id)
		}
	}
	droppedIDs.Remove(finalFrontier.Collect()...)

	return finalFrontier, droppedIDs
}

// HasCommittedManifest reports whether a Storage V3 manifest references
// committed files. ManifestEarliest is only the placeholder assigned to a new
// Growing segment before its first manifest commit.
func HasCommittedManifest(info *SegmentInfo) (bool, error) {
	if info.GetStorageVersion() != storage.StorageV3 || info.GetManifestPath() == "" {
		return false, nil
	}

	_, version, err := packed.UnmarshalManifestPath(info.GetManifestPath())
	if err != nil {
		return false, merr.WrapErrDataIntegrity(err,
			"invalid manifest path for segment %d", info.GetID())
	}
	return version > packed.ManifestEarliest, nil
}
