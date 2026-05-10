// Package compactor Copyright (c) KIOXIA Corporation. All rights reserved.
// Licensed under the MIT license.
package compactor

import (
	"sort"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v3/log"
)

type CentroidsSplitter struct {
	Centroids [][]float32
	Counts    []int64
}

/*
splitCentroids splits centroids into groups with at most maxCentroidsPerGroup per group.
Allows +-1 centroid per group implicitly via move-based balancing.
*/
func (ci *CentroidsSplitter) splitCentroids(
	maxCentroidsPerGroup int,
	minSegments int,
	useAdditionalCentroidIfImbalance bool,
) ([][]int, map[int]int) {
	n := len(ci.Counts)

	log.Debug("Initial counts", zap.Int64s("counts", ci.Counts))

	bestBalance := int64(^uint64(0) >> 1)
	var bestBalanceRatio float64
	var bestGroups [][]int
	var bestAssignment map[int]int
	maxK := maxCentroidsPerGroup
	if useAdditionalCentroidIfImbalance {
		maxK++
	}
	for k := 2; k <= maxK; k++ {
		numGroups := (n + k - 1) / k
		if numGroups < minSegments {
			break
		}
		if k > maxCentroidsPerGroup && bestBalanceRatio <= 0.1 {
			log.Debug("balance is good enough, dont exceed max centroids", zap.Float64("bestBalanceRatio", bestBalanceRatio))
			break
		}
		log.Debug("Evaluating k",
			zap.Int("k", k),
			zap.Int("numGroups", numGroups),
		)

		groups, assignment, groupCounts := ci.initialAssign(numGroups)
		if groups == nil {
			continue
		}
		ci.balanceGroups(
			groups,
			assignment,
			groupCounts,
			k,
		)

		maxC, minC := maxMin(groupCounts)
		diff := maxC - minC

		log.Debug("Balance factor for k",
			zap.Int("k", k),
			zap.Int64("maxCount", maxC),
			zap.Int64("minCount", minC),
			zap.Int64("diff", diff),
		)

		if diff < bestBalance {
			bestBalance = diff
			bestBalanceRatio = float64(diff) / float64(maxC)
			bestGroups = cloneGroups(groups)
			bestAssignment = cloneAssignment(assignment)
		}
	}

	log.Debug("Best assignment completed",
		zap.Float64("bestBalanceRatio", bestBalanceRatio),
		zap.Any("groups", bestGroups),
		zap.Any("assignment", bestAssignment),
	)

	return bestGroups, bestAssignment
}

/* ---------------- initial assignment ---------------- */
/**
 * give each group one centroid per iteration, always pick the group with the smallest total count so far,
 * while preventing any group from getting two centroids in the same iteration
 */
func (ci *CentroidsSplitter) initialAssign(
	numGroups int,
) ([][]int, map[int]int, []int64) {
	type ic struct {
		i int
		c int64
	}

	n := len(ci.Counts)
	list := make([]ic, n)
	for i := 0; i < n; i++ {
		list[i] = ic{i: i, c: ci.Counts[i]}
	}

	// Sort by descending count (LPT order)
	sort.Slice(list, func(i, j int) bool {
		return list[i].c > list[j].c
	})

	groups := make([][]int, numGroups)
	groupCounts := make([]int64, numGroups)
	assignment := make(map[int]int, n)

	baseSize := n / numGroups
	extra := n % numGroups

	// Precompute capacity per group
	capacity := make([]int, numGroups)
	for g := 0; g < numGroups; g++ {
		capacity[g] = baseSize
		if g < extra {
			capacity[g]++
		}
	}

	for _, v := range list {
		bestGroup := -1
		var bestCount int64

		for g := 0; g < numGroups; g++ {
			if len(groups[g]) >= capacity[g] {
				continue
			}

			if bestGroup == -1 || groupCounts[g] < bestCount {
				bestGroup = g
				bestCount = groupCounts[g]
			}
		}

		// Safety fallback (should never happen if capacity is correct)
		if bestGroup == -1 {
			log.Debug("no available group capacity during LPT assignment")
			return nil, nil, nil
		}

		groups[bestGroup] = append(groups[bestGroup], v.i)
		groupCounts[bestGroup] += v.c
		assignment[v.i] = bestGroup
	}

	return groups, assignment, groupCounts
}

/* ---------------- balancing ---------------- */
func (ci *CentroidsSplitter) balanceGroups(
	groups [][]int,
	assignment map[int]int,
	groupCounts []int64,
	k int,
) {
	const maxIters = 1000

	for iter := 0; iter < maxIters; iter++ {
		changed := false

		// always try swaps first
		if ci.trySwaps(groups, assignment, groupCounts) {
			changed = true
		}

		// only try moves if imbalance > 10%
		maxC, minC := maxMin(groupCounts)
		if float64(maxC-minC)/float64(maxC) > 0.1 {
			if ci.tryMoves(groups, assignment, groupCounts, k) {
				changed = true
			}
		}

		if !changed {
			log.Debug("Balance converged", zap.Int("iters", iter))
			return
		}
	}
}

// tryMoves attempts to move up to maxWidth centroids from one group to another
// respecting the ±1 centroid rule (k-1 <= group size <= k+1)
func (ci *CentroidsSplitter) tryMoves(
	groups [][]int,
	assignment map[int]int,
	groupCounts []int64,
	k int,
) bool {
	nGroups := len(groups)

	for gFrom := 0; gFrom < nGroups; gFrom++ {
		if len(groups[gFrom]) <= k-1 {
			continue
		}
		for gTo := 0; gTo < nGroups; gTo++ {
			if gFrom == gTo {
				continue
			}
			if len(groups[gTo]) >= k+1 {
				continue
			}

			bestIdx := -1
			bestDiff := abs(groupCounts[gFrom] - groupCounts[gTo])

			for _, idx := range groups[gFrom] {
				c := ci.Counts[idx]
				newFrom := groupCounts[gFrom] - c
				newTo := groupCounts[gTo] + c
				newDiff := abs(newFrom - newTo)
				if newDiff < bestDiff {
					bestDiff = newDiff
					bestIdx = idx
				}
			}

			if bestIdx >= 0 {
				removeAll(&groups[gFrom], bestIdx)
				groups[gTo] = append(groups[gTo], bestIdx)
				groupCounts[gFrom] -= ci.Counts[bestIdx]
				groupCounts[gTo] += ci.Counts[bestIdx]
				assignment[bestIdx] = gTo

				return true
			}
		}
	}

	return false
}

func sumCounts(allCounts []int64, idxs []int) int64 {
	var sum int64
	for _, i := range idxs {
		sum += allCounts[i]
	}
	return sum
}

func (ci *CentroidsSplitter) trySwaps(
	groups [][]int,
	assignment map[int]int,
	groupCounts []int64,
) bool {
	for g1 := 0; g1 < len(groups); g1++ {
		for g2 := g1 + 1; g2 < len(groups); g2++ {
			for _, i1 := range groups[g1] {
				c1 := ci.Counts[i1]
				for _, i2 := range groups[g2] {
					c2 := ci.Counts[i2]

					before := abs(groupCounts[g1] - groupCounts[g2])
					after := abs(
						(groupCounts[g1] - c1 + c2) -
							(groupCounts[g2] - c2 + c1),
					)

					if after < before {
						removeAll(&groups[g1], i1)
						removeAll(&groups[g2], i2)

						groups[g1] = append(groups[g1], i2)
						groups[g2] = append(groups[g2], i1)

						groupCounts[g1] = groupCounts[g1] - c1 + c2
						groupCounts[g2] = groupCounts[g2] - c2 + c1

						assignment[i1] = g2
						assignment[i2] = g1

						return true
					}
				}
			}
		}
	}
	return false
}

/* ---------------- helpers ---------------- */

func removeAll(g *[]int, idx int) {
	out := (*g)[:0]
	for _, v := range *g {
		if v != idx {
			out = append(out, v)
		}
	}
	*g = out
}

func maxMin(v []int64) (max, min int64) {
	min = int64(^uint64(0) >> 1)
	for _, x := range v {
		if x > max {
			max = x
		}
		if x < min {
			min = x
		}
	}
	return
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

func cloneGroups(src [][]int) [][]int {
	out := make([][]int, len(src))
	for i := range src {
		out[i] = append([]int{}, src[i]...)
	}
	return out
}

func cloneAssignment(src map[int]int) map[int]int {
	out := make(map[int]int, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}
