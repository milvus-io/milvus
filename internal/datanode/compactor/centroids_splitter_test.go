// Copyright (c) KIOXIA Corporation. All rights reserved.
// Licensed under the MIT license.
package compactor

import (
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
)

type CentroidsSplitterSuite struct {
	suite.Suite
	log *zap.Logger
}

func TestCentroidsSplitterSuite(t *testing.T) {
	suite.Run(t, new(CentroidsSplitterSuite))
}

func (s *CentroidsSplitterSuite) SetupSuite() {
	var err error
	s.log, err = zap.NewDevelopment()
	s.Require().NoError(err)
}

func (s *CentroidsSplitterSuite) TearDownSuite() {
	if s.log != nil {
		_ = s.log.Sync() // Ignore sync errors
	}
}

// ===========================================================================
// Test CentroidsSplitter.splitCentroids()
// ===========================================================================

/*
Tests splitCentroids on 50 centroids with varied counts.
Ensures the algorithm selects the best k, i.e., the number of centroids per group,
that minimizes the max-min difference of group totals.
*/
func (s *CentroidsSplitterSuite) TestSelectsBestBalanceLargeInput() {
	// 50 centroids with varied counts
	counts := []int64{
		5000, 4800, 4900, 4700, 4600,
		3000, 3200, 3100, 2900, 3300,
		6000, 6200, 5900, 6100, 5800,
		2000, 2100, 1900, 2200, 1800,
		7000, 7200, 6800, 7100, 6900,
		4000, 4100, 3900, 4200, 3800,
		5500, 5600, 5400, 5700, 5300,
		3500, 3600, 3400, 3700, 3300,
		4500, 4600, 4400, 4700, 4300,
		2500, 2600, 2400, 2700, 2300,
	}
	cs := CentroidsSplitter{Counts: counts}
	maxK := 12
	minSegs := 3
	groups, _ := cs.splitCentroids(maxK, minSegs, false)
	s.Require().NotNil(groups)
	sums := groupSums(counts, groups)
	maxSum, minSum := maxMin(sums)
	actualDiff := maxSum - minSum
	// verify the balance is reasonable (< 10% imbalance)
	imbalance := float64(actualDiff) / float64(maxSum)
	s.LessOrEqual(imbalance, 0.10,
		"imbalance should be <= 10%%, got %.2f%%", imbalance*100)
	s.log.Info("Large input optimal balance test",
		zap.Int("numCentroids", len(counts)),
		zap.Int("numGroups", len(groups)),
		zap.Int64("actualDiff", actualDiff),
		zap.Float64("imbalance%", imbalance*100))
	s.logGroups(counts, groups)
}

/*
 * calculateBestPossibleDiff finds the absolute optimal balance by exhaustive search.
 * Tries all possible assignments and returns the minimum max-min difference.
 * Due to exponential complexity - limit to use for small inputs (n <= 12).
 */
func (s *CentroidsSplitterSuite) calculateBestPossibleDiff(
	counts []int64,
	maxK int,
	minSegs int,
) (int64, [][]int, map[int]int) {
	n := len(counts)
	// Safety check: brute force only feasible for very small inputs
	if n > 12 {
		s.log.Warn("Input too large for brute force, skipping",
			zap.Int("n", n))
		return -1, nil, nil // Signal to skip validation
	}
	bestDiff := int64(^uint64(0) >> 1) // max int64
	var bestGroups [][]int
	var bestAssignment map[int]int
	// Try every valid k from 2 to maxK
	for k := 2; k <= maxK; k++ {
		numGroups := (n + k - 1) / k
		if numGroups < minSegs {
			break
		}
		diff, groups, assignment := s.bruteForceMinDiff(counts, numGroups, k)
		if diff < bestDiff {
			bestDiff = diff
			bestGroups = groups
			bestAssignment = assignment
			s.log.Debug("Found better k (brute force)",
				zap.Int("k", k),
				zap.Int("numGroups", numGroups),
				zap.Int64("diff", diff),
				zap.Any("groups", groups))
		}
	}
	return bestDiff, bestGroups, bestAssignment
}

/*
bruteForceMinDiff tests every possible way to assign n centroids
into numGroups groups to find the most balanced grouping (smallest max-min diff).

It uses a numeric counter (assignmentNum) to represent each unique assignment:
  - It uses assignmentNum as a number written in base numGroups.
  - Each "digit" in that base-numGroups representation corresponds to one centroid
    and stores the group index it belongs to.
  - Example (n=3, numGroups=2):
    assignmentNum=0 - [0,0,0]
    assignmentNum=1 - [1,0,0]
    assignmentNum=2 - [0,1,0]
    ...
    assignmentNum=7 - [1,1,1]

By incrementing assignmentNum from 0 up to numGroups^n - 1,
the loop automatically enumerates every possible group assignment.
For each one, the function checks if the group sizes respect the k +/- rule
and calculates the balance difference (maxSum - minSum),
keeping the smallest difference found.

Params:
=======
counts - a slice of int64 numbers, each representing a centroid’s weight or size.
numGroups - how many groups we want to divide the centroids into.
k - the target number of centroids per group (used to enforce +/- 1 size constraint).

Returns:
========
- The smallest possible difference between the heaviest and lightest group sums
- The groups assignment that achieves this difference
- The assignment map for that best solution

steps:
======
- Compute number of centroids n
- Compute total combinations = numGroups^n
- Loop through every possible assignment
- Convert counter → assignment array (base-numGroups)
- Compute group sizes and sums
- Skip invalid group sizes (k +/- 1) (k=maximum number of centroids allowed per group)
- Compute imbalance (max - min)
- Keep smallest imbalance found
- Return best (most balanced) diff
*/
func (s *CentroidsSplitterSuite) bruteForceMinDiff(
	counts []int64,
	numGroups int,
	k int,
) (int64, [][]int, map[int]int) {
	n := len(counts)
	minDiff := int64(^uint64(0) >> 1)
	var bestAssignment []int
	// Total number of possible assignments: numGroups^n
	// Each centroid can go into any of the numGroups.
	// That means there are numGroups^n possible ways to assign centroids.
	// Example:
	// n = 3, numGroups = 2 - 2^3 = 8 total assignments
	// n = 4, numGroups = 3 - 3^4 = 81 total assignments
	// So the algorithm will try all assignmentNum values from 0 to totalAssignments-1.
	totalAssignments := 1
	for i := 0; i < n; i++ {
		totalAssignments *= numGroups
	}
	// Try each assignment
	for assignmentNum := 0; assignmentNum < totalAssignments; assignmentNum++ {
		// turns the integer assignmentNum into an array (assignment)
		// that represents which group each centroid belongs to.
		// for example: [0, 1, 0] means:
		// centroid 0 - group 0
		// centroid 1 - group 1
		// centroid 2 - group 0
		assignment := make([]int, n)
		temp := assignmentNum
		for i := 0; i < n; i++ {
			assignment[i] = temp % numGroups
			temp /= numGroups
		}
		// Calculate group sizes and sums for this assignment
		groupSizes := make([]int, numGroups)
		groupSums := make([]int64, numGroups)
		for i := 0; i < n; i++ {
			g := assignment[i]
			groupSizes[g]++
			groupSums[g] += counts[i]
		}
		// Check if this assignment is valid (respects k+/-1 rule)
		valid := true
		for g := 0; g < numGroups; g++ {
			size := groupSizes[g]
			if size < k-1 || size > k+1 {
				valid = false
				break
			}
		}
		if !valid {
			continue
		}
		// Calculate max-min difference
		maxSum := int64(0)
		minSum := int64(^uint64(0) >> 1)
		// Loop over all elements in groupSums, sum will be the value of each
		// element, _ ignores the index
		for _, sum := range groupSums {
			if sum > maxSum {
				maxSum = sum
			}
			if sum < minSum {
				minSum = sum
			}
		}
		diff := maxSum - minSum
		// Track the best
		if diff < minDiff {
			minDiff = diff
			bestAssignment = make([]int, n)
			copy(bestAssignment, assignment)
		}
	}
	// Convert best assignment array to groups and map
	groups := make([][]int, numGroups)
	assignmentMap := make(map[int]int)
	for centroidIdx, groupIdx := range bestAssignment {
		groups[groupIdx] = append(groups[groupIdx], centroidIdx)
		assignmentMap[centroidIdx] = groupIdx
	}
	return minDiff, groups, assignmentMap
}

// printBalanceComparison prints detailed comparison info between the actual (splitCentroids)
// and the optimal (brute-force) results for diagnostic and debugging purposes.
func (s *CentroidsSplitterSuite) printBalanceComparison(
	testName string,
	counts []int64,
	groups [][]int,
	assignment map[int]int,
	sums []int64,
	actualDiff int64,
	bestGroups [][]int,
	bestAssignment map[int]int,
	bestSums []int64,
	bestDiff int64,
	ratio *float64, // optional, can be nil
	tolerance *float64, // optional, can be nil
) {
	fmt.Printf("===========================================\n")
	fmt.Printf("%s\n", testName)
	fmt.Printf("-------------------------------------------\n")
	fmt.Printf("Input counts: %v\n", counts)
	fmt.Printf("-------------------------------------------\n")
	fmt.Printf("ACTUAL (splitCentroids):\n")
	fmt.Printf("  Diff: %d\n", actualDiff)
	fmt.Printf("  Groups: %v\n", groups)
	fmt.Printf("  Assignment: %v\n", assignment)
	fmt.Printf("  Group sums: %v\n", sums)
	fmt.Printf("-------------------------------------------\n")
	fmt.Printf("OPTIMAL (brute force):\n")
	fmt.Printf("  Diff: %d\n", bestDiff)
	fmt.Printf("  Groups: %v\n", bestGroups)
	fmt.Printf("  Assignment: %v\n", bestAssignment)
	fmt.Printf("  Group sums: %v\n", bestSums)
	if ratio != nil && tolerance != nil {
		fmt.Printf("-------------------------------------------\n")
		fmt.Printf("Comparison:\n")
		fmt.Printf("  Ratio (actualDiff / bestDiff): %.3f\n", *ratio)
		fmt.Printf("  Allowed: ≤ %.2f\n", float64(bestDiff)+*tolerance)
	}
	fmt.Printf("===========================================\n\n")
}

/* Tests with exact expected max-min differences */
func (s *CentroidsSplitterSuite) TestExactBalanceSmallInput() {
	// Define the four test inputs
	testInputs := []struct {
		name   string
		counts []int64
	}{
		{"Input1", []int64{100, 90, 80, 70, 60, 50}},
		{"Input2", []int64{60, 40, 70, 30, 80, 20, 90, 10}},
		{"Input3", []int64{100, 100, 100, 50, 50, 50, 25, 25, 25}},
		{"Input4", []int64{100, 80, 60, 40, 20}},
		{"Input5", []int64{100, 200, 300}},
		{"Input6", []int64{1000, 1000, 1000, 1000, 1000, 1000, 1000, 1000}},
	}
	minSegs := 2
	for _, input := range testInputs {
		for _, maxK := range []int{3, 4} {
			testName := fmt.Sprintf("%s, maxK=%d", input.name, maxK)
			s.Run(testName, func() {
				cs := CentroidsSplitter{Counts: input.counts}
				groups, assignment := cs.splitCentroids(maxK, minSegs, false)
				s.Require().NotNil(groups, "splitCentroids should return groups")
				sums := groupSums(input.counts, groups)
				maxSum, minSum := maxMin(sums)
				actualDiff := maxSum - minSum
				// Compute optimal via brute force
				bestDiff, bestGroups, bestAssignment := s.calculateBestPossibleDiff(input.counts, maxK, minSegs)
				s.Require().NotEqual(int64(-1), bestDiff, "brute-force solution must exist")
				bestSums := groupSums(input.counts, bestGroups)
				s.printBalanceComparison(
					testName,
					input.counts, groups, assignment, sums, actualDiff,
					bestGroups, bestAssignment, bestSums, bestDiff,
					nil, nil, // no ratio/tolerance for exact balance inputs
				)
				s.Equal(bestDiff, actualDiff,
					"Expected exact balance for this input, got diff=%d (best=%d)",
					actualDiff, bestDiff,
				)
			})
		}
	}
}

/* TestRandomCentroidsBruteForce verifies that CentroidsSplitter balances centroids
 * reasonably well compared to a brute-force optimal solution.
 *
 * The test generates random counts for a small number of centroids and splits them
 * into groups using splitCentroids. It then compares the imbalance (difference
 * between max and min group sums) to the best possible imbalance calculated via
 * brute-force. Since spliCentroids may not find the global optimum, we allow some
 *  tolerance in the comparison.
 *  The tolerance is set as the larger of:
 *   - 60% above the brute-force best difference (allows splitCentroids to be slightly worse)
 *   - 3% of the total sum of all counts (prevents false failures for large totals)
 *
 * This test runs multiple times for different `maxK` values (maximum centroids per group)
 * to cover different grouping scenarios.
 */
func (s *CentroidsSplitterSuite) TestRandomCentroidsBruteForce() {
	const nCentroids = 10 // small enough to compute brute-force solution
	const numRuns = 3     // number of random test runs
	const minSegs = 2     // minimum number of groups
	// Test for different maximum centroids per group
	for _, maxK := range []int{3, 4, 5} {
		for run := 1; run <= numRuns; run++ {
			// Generate random counts between 1_000 and 10_000 for each centroid
			counts := make([]int64, nCentroids)
			for i := range counts {
				counts[i] = int64(1000 + rand.Intn(9000))
			}
			// Run as a subtest so each run reports individually
			s.Run(fmt.Sprintf("maxK%d_run%d", maxK, run), func() {
				cs := CentroidsSplitter{Counts: counts}
				groups, assignment := cs.splitCentroids(maxK, minSegs, false)
				s.Require().NotNil(groups, "splitCentroids should return groups")
				// Compute actual result
				sums := groupSums(counts, groups)
				maxSum, minSum := maxMin(sums)
				actualDiff := maxSum - minSum
				// Compute optimal via brute force
				bestDiff, bestGroups, bestAssignment := s.calculateBestPossibleDiff(counts, maxK, minSegs)
				s.Require().NotEqual(int64(-1), bestDiff, "brute-force solution must exist")
				// Compute sums for the brute force solution
				bestSums := groupSums(counts, bestGroups)
				total := sumCounts(counts, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
				tolerance := math.Max(
					float64(bestDiff)*0.6, // allow up to 60% worse than optimal
					float64(total)*0.03,   // or 3% of total counts
				)
				ratio := float64(actualDiff) / float64(bestDiff)
				s.printBalanceComparison(
					fmt.Sprintf("Run %d (maxK=%d)", run, maxK),
					counts, groups, assignment, sums, actualDiff,
					bestGroups, bestAssignment, bestSums, bestDiff,
					&ratio, &tolerance,
				)
				s.LessOrEqual(float64(actualDiff), float64(bestDiff)+tolerance,
					"Run %d: balance should be optimal or near-optimal: "+
						"best=%d, actual=%d, allowed≤%.0f",
					run, bestDiff, actualDiff, float64(bestDiff)+tolerance)
			})
		}
	}
}

/*
 * Test that splitCentroids chooses the value of k that produces the most balanced groups
 * in terms of the total counts (weights) of centroids.
 * The balance is measured by the difference between the largest and smallest group sums (max-min difference).
 * The smaller this difference, the better the balance.
 *
 * The test uses 6 centroids with different weights.
 * splitCentroids shall try different k values (2, 3, 4).
 * It will pick the one that gives the best balance between groups.
 * Then the test measures how balanced the result is:
 * The best result is:
 * k = 2 - 3 groups
 * Group sums = [150, 150, 150]
 * Max-min difference = 0 (perfectly balanced)
 * Groups (centroid indices):
 *
 * Group	Centroid indices	Counts sum
 * 0		[5, 0]	[50, 100]		150
 * 1		[4, 1]	[60, 90]		150
 * 2		[2, 3]	[80, 70]		150
 */
func (s *CentroidsSplitterSuite) TestSelectsBestBalance() {
	// Carefully chosen counts where different k produce different balances
	counts := []int64{100, 90, 80, 70, 60, 50}
	cs := CentroidsSplitter{Counts: counts}
	// Run the function under test
	groups, _ := cs.splitCentroids(4, 2, false)
	s.Require().NotNil(groups)
	// Compute sums per group
	sums := groupSums(counts, groups)
	maxSum, minSum := maxMin(sums)
	actualDiff := maxSum - minSum
	// Assert the best balance (diff=0)
	s.Equal(int64(0), actualDiff, "should select k that minimizes max-min difference, got diff=%d", actualDiff)
	// Log info about the groups
	s.logGroups(counts, groups)
}

/*
 * TestComparesAllKValues verifies that splitCentroids evaluates all valid k values
 * and selects the one that produces the most balanced groups in terms of total counts.
 * Input counts: 50, 50, 60, 60, 70, 70, 80, 80, 90, 90
 * The algorithm shall try different k values (2..6) and computes the number of groups for each.
 * Best result for these counts is diff 0, which can be achieved for example with k=2 and sum 140:
 * "centroidIndices": [1, 9], "counts": [50, 90], "sum": 140}
 * "centroidIndices": [2, 6], "counts": [60, 80], "sum": 140}
 * "centroidIndices": [8, 0], "counts": [90, 50], "sum": 140}
 * "centroidIndices": [7, 3], "counts": [80, 60], "sum": 140}
 * "centroidIndices": [4, 5], "counts": [70, 70], "sum": 140}
 */
func (s *CentroidsSplitterSuite) TestComparesAllKValues() {
	// 10 centroids with paired values
	counts := []int64{50, 50, 60, 60, 70, 70, 80, 80, 90, 90}
	cs := CentroidsSplitter{Counts: counts}
	groups, _ := cs.splitCentroids(6, 2, false)
	s.Require().NotNil(groups)
	sums := groupSums(counts, groups)
	maxSum, minSum := maxMin(sums)
	actualDiff := maxSum - minSum
	s.Equal(actualDiff, int64(0), "should evaluate all k values to find best balance, got diff=%d", actualDiff)
	s.logGroups(counts, groups)
}

/**
 * Run splitCentroids and check that it produces valid, balanced groups.
 *
 * splitCentroids - Split the centroids into groups (segments):
 * Input:
 * 	- maxCentroidsPerGroup	- max number of centroids allowed per group
 * 	- minSegments 			- min number of groups (segments) required
 * 	- Counts (from struct)	- A slice of weights, one per centroid.
 * 						  For example: [100, 200, 150, 120].
 * Returns:
 * - groups: list of groups. Each element is a list of centroid indices
 * 	  		belonging to that group. fOr example:
 * 			{0, 1, 4},   * group 0 has centroids 0,1,4
 *			{2, 3, 5},   * group 1 has centroids 2,3,5
 * - assign: map[int]int - centroid to group mapping
 * 		    map - centroid to group. Example:
 * 			0: 0,  - centroid 0 -> group 0
 * 			1: 0,  - centroid 1 -> group 0
 * 			2: 1,  - centroid 2 -> group 1
 *
 * Validations performed by this test:
 * - groups and assignment maps are not nil, and at least minSegments exist
 * - every centroid is assigned exactly once
 * - Group sizes are:  k−1 <= size <= k+1
 * - imbalance (max−min)/max <= 15%
 * - total counts after grouping equal the original total
 * - number of groups <= 2 * the optimal theoretical number
 */
func (s *CentroidsSplitterSuite) runSplitTest(counts []int64, maxCentroidsPerGroup int, minSegments int, name string) {
	cs := CentroidsSplitter{Counts: counts}
	groups, assign := cs.splitCentroids(maxCentroidsPerGroup, minSegments, false)
	s.log.Debug(fmt.Sprintf("==== TEST: %s ====", name),
		zap.Any("groups", groups),
		zap.Any("assign", assign),
	)
	// Validate that the algorithm didn’t return nil and there are at least
	// minSegments groups
	s.Require().NotNil(groups, "%s: groups should not be nil", name)
	s.Require().NotNil(assign, "%s: assignment should not be nil", name)
	s.Require().GreaterOrEqual(len(groups), minSegments,
		"%s: should have at least %d groups, got %d", name, minSegments, len(groups))

	// Validates that:
	// - There is no empty group
	// - every centroid appears exactly once
	// - The assign map correctly matches group indices
	// - No centroid is missing or duplicated
	seen := make(map[int]bool) // which centroids have already been assigned to a group
	// Iterates over all groups, where: gIdx = group index
	// g = slice of centroid indices belonging to that group
	for gIdx, g := range groups {
		s.NotEmpty(g, "%s: group %d is empty", name, gIdx)
		for _, cIdx := range g {
			s.Require().False(seen[cIdx],
				"%s: centroid %d appears in multiple groups", name, cIdx)
			seen[cIdx] = true
			// Checks consistency between the groups list and the assign map
			// assign[cIdx] - centroid cIdx belongs to group X, if in the actual group iteration,
			// the gIdx is different from X - the assign map doesn’t reflect reality
			s.Require().Equal(gIdx, assign[cIdx],
				"%s: assignment[%d]=%d but centroid %d is in group %d",
				name, cIdx, assign[cIdx], cIdx, gIdx)
		}
	}
	s.Require().Equal(len(counts), len(seen),
		"%s: not all centroids assigned - %d/%d", name, len(seen), len(counts))

	// Validate that each group should have roughly the same number of centroids
	// k defines the ideal group size, Validate that for all groups (k-1 <= group size <= k+1)
	avgGroupSize := float64(len(counts)) / float64(len(groups))
	k := int(avgGroupSize + 0.5) // round to nearest
	for gIdx, g := range groups {
		size := len(g)
		s.Require().GreaterOrEqual(size, k-1, "%s: group %d has %d centroids, less than k-1=%d",
			name, gIdx, size, k-1)
		s.Require().LessOrEqual(size, k+1, "%s: group %d has %d centroids, more than k+1=%d",
			name, gIdx, size, k+1)
	}

	// Validate that the final imbalance is <= 15%
	// centroids_splitter tries to move centroids to reduce imbalance only if imbalance > 10%.
	// Validate the difference between the largest and smallest group.
	// Ensure that the smallest group is no more than 15% smaller than the largest group.
	// First, Check how many vectors are in each group, groupSums calculates total weight per group
	// Then, find the largest and smallest group sum and normalize the difference by the largest
	// group. calculates imbalance = (max-min)/max - This gives a number between 0 and 1 (or 0% to 100%):
	// 0 = perfectly balanced (all groups have the same number of vectors).
	// 1 = extremely unbalanced (smallest group has 0 vectors).
	// Fails if imbalance > 15%
	// This enforces the quality of load balancing, no group should be much heavier
	sums := groupSums(counts, groups) // Calculates the total vectors count of each group of centroids
	// Returns a slice sums of total vectors counts per group.
	maxSum, minSum := maxMin(sums)
	for i, sum := range sums {
		s.log.Debug("Group sum", zap.Int("group", i), zap.Int64("sum", sum), zap.Int("size", len(groups[i])))
	}
	totalSum := int64(0)
	for _, sum := range sums {
		totalSum += sum
	}
	avgSum := totalSum / int64(len(groups))
	imbalance := float64(maxSum-minSum) / float64(maxSum)
	s.log.Debug("Balance metrics",
		zap.Int64("maxSum", maxSum), zap.Int64("minSum", minSum), zap.Int64("avgSum", avgSum),
		zap.Int64("diff", maxSum-minSum), zap.Float64("imbalance%", imbalance*100))
	// balance should be <= 15% (Using 15% instead of 10% to allow some edge cases)
	s.Require().LessOrEqual(imbalance, 0.15,
		"%s: imbalance too high: %.2f%% (max=%d, min=%d)", name, imbalance*100, maxSum, minSum)

	// Verifies that the total of all counts equals the sum of all group totals.
	// No centroid’s weight was lost or duplicated.
	originalSum := int64(0)
	for _, c := range counts {
		originalSum += c
	}
	s.Require().Equal(originalSum, totalSum,
		"%s: total count mismatch - original=%d, grouped=%d", name, originalSum, totalSum)

	// Validate that the number of groups is reasonable and not too fragmented.
	// At most 2× the theoretical minimum number of groups.
	// Adding maxCentroidsPerGroup - 1 ensures that any remainder from the division pushes
	// the result up to the next integer.
	optimalGroups := (len(counts) + maxCentroidsPerGroup - 1) / maxCentroidsPerGroup
	s.Require().LessOrEqual(len(groups), optimalGroups*2,
		"%s: created %d groups but optimal would be around %d (inefficient)",
		name, len(groups), optimalGroups)

	s.log.Info("Test passed", zap.String("name", name), zap.Int("numCentroids", len(counts)),
		zap.Int("numGroups", len(groups)), zap.Int("avgGroupSize", k),
		zap.Float64("imbalance%", imbalance*100))
}

/*
  - Creates random input and run the test.
    Simulate centroid sizes are similar but not identical.
*/
func (s *CentroidsSplitterSuite) TestRandomCentroidsCounts() {
	// Create random centroid counts.
	// Each count is between 3,000,000  5,000,000.
	// The global rand package is already seeded in Go 1.20+.
	counts := make([]int64, 120)
	for i := range counts {
		counts[i] = int64(3_000_000 + rand.Intn(2_000_000))
	}
	// maxCentroidsPerGroup=16, minSegments=2
	s.runSplitTest(counts, 16, 2, "Random balanced case")
}

func (s *CentroidsSplitterSuite) TestFixedCentroidsCounts() {
	counts := []int64{
		3473296, 4866376, 4291173, 2683936, 3839945, 6327972,
		5207609, 5006737, 3506260, 3280164, 3610312, 1524297,
		7639688, 3230970, 6914819, 2319632, 2298432, 4925060,
		8018111, 5897668, 2292120, 3238289, 7281027, 2942480,
		5685714, 3913458, 3753165, 3403539, 1986213, 6886230,
		5108165, 6899555, 4365188, 8221242, 3393948, 12753218,
		3967886, 3761110, 5013434, 4572566, 6265133, 6580697,
		2435948, 3676068, 2761141, 5757571, 4160833, 4554245,
		6048565, 5047204, 3137337, 2754281, 1131556, 6492510,
		2604366, 2504598, 3169946, 4438026, 4213988, 3124943,
		1993591, 7393692, 11555602, 4422540, 3877762, 3196419,
		2728727, 8280894, 3970758, 8390059, 2014176, 2334127,
		7424962, 5481976, 6189038, 3797582, 3568091, 2681390,
		1502587, 4225497, 4249895, 2606960, 8260517, 1542166,
		4388597, 11418302, 4664231, 4542939, 3686400, 4298004,
		8467358, 3332138, 5154904, 6915174, 4401403, 4650820,
		3804139, 10088736, 6061097, 4137064, 6299193, 3040331,
		2922572, 5109555, 3576530, 11986363, 8717854, 5634064,
	}
	s.runSplitTest(counts, 16, 2, "Production trace case")
}

/**
* stress-test the splitCentroids() algorithm by generating random input data
* (random centroid counts, random lengths) and verifying:
* - every centroid assigned exactly once
* - assignment map is consistent with groups
* - no group is empty
 */
func (s *CentroidsSplitterSuite) TestStressCentroidsSplitter() {
	// Repeat the test 50 times with different random data
	for trial := 0; trial < 50; trial++ {
		// each test run will use between 5 and 100 centroids
		n := 5 + rand.Intn(96) // 5..100
		counts := make([]int64, n)
		// Each centroid gets a random count between 1 and 1,000,000
		for i := range counts {
			counts[i] = int64(1 + rand.Intn(1_000_000))
		}
		cs := CentroidsSplitter{Counts: counts}
		// maxCentroidsPerGroup = 10, minSegments = 2
		groups, assign := cs.splitCentroids(10, 2, false)
		if groups == nil {
			continue // valid: no k satisfied minSegments
		}
		// Track which centroids were seen
		seen := make(map[int]bool, n)
		for gIdx, g := range groups {
			s.NotEmpty(g, "trial %d: group %d is empty", trial, gIdx)
			for _, idx := range g {
				s.False(seen[idx], "trial %d: centroid %d duplicated", trial, idx)
				seen[idx] = true
				s.Equal(gIdx, assign[idx], "trial %d: assignment mismatch for centroid %d", trial, idx)
			}
		}
		s.Equal(n, len(seen), "trial %d: not all centroids assigned", trial)
	}
}

/**
 * Uses small input (10 items) to verify that:
 * - Every centroid appears exactly once in all the groups.
 * - The assign map (centroid - group index) correctly matches what’s inside groups.
 */
func (s *CentroidsSplitterSuite) TestAssignmentConsistency() {
	// Creates 10 centroids with arbitrary weights (test indexing only - values don’t matter here)
	counts := []int64{100, 200, 300, 400, 500, 600, 700, 800, 900, 1000}
	cs := CentroidsSplitter{Counts: counts}
	groups, assign := cs.splitCentroids(4, 2, false) // maxCentroidsPerGroup = 4, minSegments = 2
	seen := make(map[int]bool)
	for gIdx, g := range groups {
		for _, cIdx := range g {
			s.False(seen[cIdx], "centroid %d appears more than once", cIdx)
			seen[cIdx] = true
			s.Equal(gIdx, assign[cIdx], "assignment map different from groups for centroid %d", cIdx)
		}
	}
	s.Equal(len(counts), len(seen), "not all centroids assigned")
}

/**
* Verifies that with identical counts every group sum should be exactly equal
 */
func (s *CentroidsSplitterSuite) TestUniformCounts() {
	n := 24
	counts := make([]int64, n)
	for i := range counts {
		counts[i] = 1000
	}
	cs := CentroidsSplitter{Counts: counts}
	groups, _ := cs.splitCentroids(6, 2, false)
	sums := groupSums(counts, groups) // sums of total counts per group
	for i := 1; i < len(sums); i++ {
		s.Equal(sums[0], sums[i], "uniform counts should yield equal group sums")
	}
}

/**
 * Test the split of only two centroids (500 and 1500 vectors),
 * the splitter must return one group containing both,
 * must not lose or duplicate any centroid,
 * and the total count of all grouped centroids must match the input total (2000).
 */
func (s *CentroidsSplitterSuite) TestTwoCentroids() {
	counts := []int64{500, 1500}
	cs := CentroidsSplitter{Counts: counts}
	groups, assign := cs.splitCentroids(4, 1, false)
	total := int64(0)
	// Loops over all groups and sums the counts of each centroid
	// Each inner slice ([]int) in groups represents one group containing
	// the list of centroid indices belonging to that group
	for _, g := range groups {
		for _, idx := range g {
			total += counts[idx]
		}
	}
	// Ensures no centroid was lost or duplicated
	s.Equal(int64(2000), total)
	// assign is a map[int]int, It maps centroid_index - group_index
	// key = index of a centroid (position in Counts)
	// value = which group that centroid was assigned to
	s.Equal(len(counts), len(assign))
}

// Single centroid edge-case: k starts at 2 - numGroups = 1.
// minSegments=1 should allow it; minSegments=2 should yield nil (no valid k).
/**
 * This test ensures that:
 * When there’s only one centroid, the splitter can create one valid group
 * if only one is required,
 * and refuses to run if more groups are requested than possible.
 */
func (s *CentroidsSplitterSuite) TestSingleCentroid() {
	counts := []int64{42}
	cs := CentroidsSplitter{Counts: counts}
	// When there’s only one centroid and we require only one segment,
	// verifies that there is one assignment in the map, since we had exactly 1 centroid,
	// this ensures: it was assigned to a group, no centroid was skipped
	groups, assign := cs.splitCentroids(4, 1, false)
	s.NotNil(groups)
	s.Equal(1, len(assign))
	// checks the opposite situation:
	// when the function cannot satisfy the required number of groups.
	groups2, assign2 := cs.splitCentroids(4, 2, false)
	s.Nil(groups2)
	s.Nil(assign2)
}

/**
* Test that empty input doesn’t crash (returns nil)
 */
func (s *CentroidsSplitterSuite) TestEmptyCentroids() {
	cs := CentroidsSplitter{Counts: []int64{}}
	groups, assign := cs.splitCentroids(4, 1, false)
	s.Nil(groups)
	s.Nil(assign)
}

/*
 * Tests extreme imbalance (one large centroid among many small ones.
 * Ensures function still returns valid groups.
 */
func (s *CentroidsSplitterSuite) TestSkewedCounts() {
	counts := make([]int64, 50)
	for i := range counts {
		counts[i] = 100
	}
	counts[0] = 1_000_000 // one huge outlier
	cs := CentroidsSplitter{Counts: counts}
	groups, assign := cs.splitCentroids(10, 2, false)
	s.NotNil(groups)
	// Each centroid should have an assignment in assign
	s.Equal(len(counts), len(assign))
	s.assertNoEmptyGroups(groups)
}

/**
 * Verifies that CentroidsSplitter correctly handles the case
 * when maxCentroidsPerGroup equals the number of centroids. The function should:
 * 1. Try different k (number of groups) values to balance centroid counts.
 * 2. Produce a valid assignment of centroids to groups.
 * 3. Ensure all centroids are included in the output.
 * The test does not assert the exact number of groups, only that total counts match.
 */
func (s *CentroidsSplitterSuite) TestMaxKEqualsN() {
	counts := []int64{10, 20, 30, 40, 50} // size of each centroid
	cs := CentroidsSplitter{Counts: counts}
	// k iterates 2..5; at k=5 numGroups=1; minSegments=1 allows it
	groups, _ := cs.splitCentroids(5, 1, false)
	// maxCentroidsPerGroup = 5 - a single group can hold up to 5 centroids.
	//	minSegments = 1 - at least one group must be created.
	s.NotNil(groups)
	total := int64(0)
	for _, g := range groups {
		for _, idx := range g {
			total += counts[idx]
		}
	}
	s.Equal(int64(150), total)
}

// ===========================================================================
// Test CentroidsSplitter.initialAssign()
// ===========================================================================
/**
 * This function tests the CentroidsSplitter.initialAssign(). It checks that the initial grouping
 * (before balancing) works correctly and consistently.
 * initialAssign(numGroups int) - spreads centroids evenly across all groups, while starting from the largest counts first,
 * i.e. round-robin on descending counts: largest count goes to group 0, next to group 1, etc.
 * It creates an initial assignment of centroids into groups before any balancing happens.
 * Each group gets a mix of large and small centroids.
 * With 6 items and numGroups=3 we get:
 * groups = [
 * [1, 2],   // G0 - 60 + 30 = 90
 * [3, 4],   // G1 - 50 + 20 = 70
 * [5, 0],   // G2 - 40 + 10 = 50
 * ]
 * groupCounts = [90, 70, 50]
 * assignment = {1:0, 2:0, 3:1, 4:1, 5:2, 0:2}
 */
func (s *CentroidsSplitterSuite) TestInitialAssignRoundRobin() {
	counts := []int64{10, 60, 30, 50, 20, 40} // sorted desc: 60 50 40 30 20 10
	cs := CentroidsSplitter{Counts: counts}
	groups, assign, gCounts := cs.initialAssign(3)
	// Validate that there are 3 groups, 2 items each
	s.Equal(3, len(groups))
	for _, g := range groups {
		s.Equal(2, len(g), "each group should have exactly 2 members")
	}
	// Makes sure the assign map correctly matches what’s in groups.
	// For example, if centroid index 3 is in groups[1], then assign[3] must equal 1.
	for gIdx, g := range groups {
		for _, cIdx := range g {
			s.Equal(gIdx, assign[cIdx])
		}
	}
	// Verifies that groupCounts (computed inside initialAssign) are correct.
	for gIdx, g := range groups {
		sum := int64(0)
		for _, idx := range g {
			sum += counts[idx]
		}
		s.Equal(sum, gCounts[gIdx])
	}
}

// ===========================================================================
// Test CentroidsSplitterSuite.trySwaps()
// ===========================================================================
/**
 * The test intentionally constructs a fake unbalanced scenario to check that trySwaps() can detect and correct it.
 * This test checks whether the trySwaps() function in CentroidsSplitter really improves the balance
 * between groups when an obvious better swap exists.
 * trySwaps checks every possible pair of centroids between two groups.
 * For each possible swap (i1 from group1, i2 from group2), it:
 * Calculates the current imbalance between the two groups.
 * Simulates swapping them. Calculates the new imbalance.
 * If the new imbalance is smaller - it performs the swap and returns true.
 * Starting setup:
 * Group0 = [0(100), 1(100)] - total = 200
 * Group1 = [2(1), 3(1)]     - total = 2
 * The function shall hit the first improving swap (centroids 1 <-> 2) and execute it.
 * After swap:
 * Group0 = [0(100), 2(1)] - total = 101
 * Group1 = [1(100), 3(1)] - total = 101
 */
func (s *CentroidsSplitterSuite) TestTrySwapsImprovesBalance() {
	counts := []int64{100, 100, 1, 1}
	cs := CentroidsSplitter{Counts: counts}
	groups := [][]int{{0, 1}, {2, 3}}
	assign := map[int]int{0: 0, 1: 0, 2: 1, 3: 1}
	gCounts := []int64{200, 2}
	swapped := cs.trySwaps(groups, assign, gCounts)
	s.True(swapped, "an improving swap must exist")
	// Verify the swap actually happened correctly
	// Check group counts were updated
	diff := abs(gCounts[0] - gCounts[1])
	s.True(diff < 198, "balance should have improved, got diff=%d", diff)
	s.Equal(int64(101), gCounts[0], "group0 count should be 101 after swap")
	s.Equal(int64(101), gCounts[1], "group1 count should be 101 after swap")
	// All result in the same balance (101, 101), so we just verify:
	// - Each group has exactly 2 centroids
	// - Group sums are correct
	s.Equal(2, len(groups[0]), "group0 should still have 2 centroids")
	s.Equal(2, len(groups[1]), "group1 should still have 2 centroids")
	// Verify assignment map is consistent with groups
	for gIdx, g := range groups {
		for _, cIdx := range g {
			s.Equal(gIdx, assign[cIdx],
				"assignment[%d] should be %d after swap", cIdx, gIdx)
		}
	}
	// Verify actual sums match what we expect
	actualSum0 := int64(0)
	actualSum1 := int64(0)
	for _, idx := range groups[0] {
		actualSum0 += counts[idx]
	}
	for _, idx := range groups[1] {
		actualSum1 += counts[idx]
	}
	s.Equal(int64(101), actualSum0, "group0 actual sum should be 101")
	s.Equal(int64(101), actualSum1, "group1 actual sum should be 101")
	// Verify one centroid from each count type ended up in each group
	// (one 100-count and one 1-count per group)
	has100G0 := false
	has1G0 := false
	for _, idx := range groups[0] {
		if counts[idx] == 100 {
			has100G0 = true
		}
		if counts[idx] == 1 {
			has1G0 = true
		}
	}
	s.True(has100G0, "group0 should have one centroid with count=100")
	s.True(has1G0, "group0 should have one centroid with count=1")
}

// trySwaps returns false when groups are already perfectly balanced.
func (s *CentroidsSplitterSuite) TestTrySwapsNoChangeWhenBalanced() {
	counts := []int64{50, 50, 50, 50}
	cs := CentroidsSplitter{Counts: counts}
	groups := [][]int{{0, 1}, {2, 3}}
	assign := map[int]int{0: 0, 1: 0, 2: 1, 3: 1}
	gCounts := []int64{100, 100}
	swapped := cs.trySwaps(groups, assign, gCounts)
	s.False(swapped, "no swap should fire when already balanced")
}

// ===========================================================================
// Test CentroidsSplitter.tryMoves()
// ===========================================================================
/*
* tryMoves should move a centroid when it reduces imbalance and group-size
*  constraints (k-1 .. k+1) allow it.
* Initial State:
* Group0: [0, 1, 2, 3] - counts = [100, 100, 100, 1] - sum = 301, size = 4
* Group1: [4, 5]       - counts = [50, 50]           - sum = 100, size = 2
* Current diff: 301 - 100 = 201
* tryMoves tries moving 1-5 centroids from Group0 - Group1 (since Group0 is heavier).
* Constraints:
* Group0 size must stay >= k-1 = 2 (currently 4, so can remove up to 2)
* Group1 size must stay <= k+1 = 4 (currently 2, so can add up to 2)
* So it can move at most 2 centroids.
* Trying different widths:
* Width 1: Move first 1 centroid from Group0 - Group1
* Move centroid 0 (count=100)
* New Group0: sum = 301 - 100 = 201
* New Group1: sum = 100 + 100 = 200
* New diff = |201 - 200| = 1 - (huge improvement!)
* Width 2: Move first 2 centroids
* Move centroids 0, 1 (counts=100, 100)
* New Group0: sum = 301 - 200 = 101
* New Group1: sum = 100 + 200 = 300
* New diff = |101 - 300| = 199 (worse than width=1)
* Best move: width=1 (moving centroid 0)
* Expected Result:
* goGroup0: [1, 2, 3]    - counts = [100, 100, 1]  - sum = 201, size = 3
* Group1: [4, 5, 0]    - counts = [50, 50, 100]  - sum = 200, size = 3
* New diff: |201 - 200| = 1
 */
func (s *CentroidsSplitterSuite) TestTryMovesReducesImbalance() {
	// k=3 - allowed size [2,4]
	// group0 has 4 items (at limit), group1 has 2 (at limit)
	// counts chosen so moving idx=3 (val 1) from g0-g1 reduces diff
	counts := []int64{100, 100, 100, 1, 50, 50}
	cs := CentroidsSplitter{Counts: counts}
	groups := [][]int{{0, 1, 2, 3}, {4, 5}} // sizes 4, 2  both within [2,4]
	assign := map[int]int{0: 0, 1: 0, 2: 0, 3: 0, 4: 1, 5: 1}
	gCounts := []int64{301, 100}
	// Initial imbalance: 301 - 100 = 201
	moved := cs.tryMoves(groups, assign, gCounts, 3) // k=3, maxWidth=5
	s.True(moved, "a move should fire to reduce imbalance")
	// Verify imbalance decreased
	newDiff := abs(gCounts[0] - gCounts[1])
	s.True(newDiff < 201, "imbalance should have decreased, got diff=%d", newDiff)
	// Verify the expected move happened: centroid 0 moved from Group0 - Group1
	s.Equal([]int{1, 2, 3}, groups[0], "group0 should be [1,2,3] after move")
	s.Equal([]int{4, 5, 0}, groups[1], "group1 should be [4,5,0] after move")
	// Verify group counts updated correctly
	s.Equal(int64(201), gCounts[0], "group0 count should be 201")
	s.Equal(int64(200), gCounts[1], "group1 count should be 200")
	// Verify new imbalance is minimal (1)
	s.Equal(int64(1), newDiff, "diff should be exactly 1 after optimal move")
	// Verify group sizes respect k +/- 1 constraint
	s.Equal(3, len(groups[0]), "group0 size should be 3 (within [2,4])")
	s.Equal(3, len(groups[1]), "group1 size should be 3 (within [2,4])")
	// Verify assignment map updated
	s.Equal(1, assign[0], "centroid 0 should now be assigned to group1")
	s.Equal(0, assign[1], "centroid 1 should still be in group0")
	s.Equal(0, assign[2], "centroid 2 should still be in group0")
	s.Equal(0, assign[3], "centroid 3 should still be in group0")
	s.Equal(1, assign[4], "centroid 4 should still be in group1")
	s.Equal(1, assign[5], "centroid 5 should still be in group1")
	// Verify actual sums
	actualSum0 := int64(0)
	actualSum1 := int64(0)
	for _, idx := range groups[0] {
		actualSum0 += counts[idx]
	}
	for _, idx := range groups[1] {
		actualSum1 += counts[idx]
	}
	s.Equal(int64(201), actualSum0, "group0 actual sum should be 201")
	s.Equal(int64(200), actualSum1, "group1 actual sum should be 200")
}

/**
*  tryMoves must NOT move when the source group is already at minimum size (k-1).
* k = 3 - allowed group sizes: [2, 4]
* group0 size = 2 - exactly k-1
* group1 size = 2 - exactly k-1
* Total vectors in groups:
* group0 = 1000 + 1 = 1001
* group1 = 5 + 5 = 10
* There is huge imbalance, but groups are at minimum size, so we cannot remove centroids.
 */
func (s *CentroidsSplitterSuite) TestTryMovesRespectsMinGroupSize() {
	// k=3 - min size = 2; group0 already has 2 items - cannot remove
	counts := []int64{1000, 1, 5, 5}
	cs := CentroidsSplitter{Counts: counts}
	groups := [][]int{{0, 1}, {2, 3}} // both size 2 == k-1
	assign := map[int]int{0: 0, 1: 0, 2: 1, 3: 1}
	gCounts := []int64{1001, 10}
	moved := cs.tryMoves(groups, assign, gCounts, 3)
	s.False(moved, "cannot move from a group already at minimum size k-1")
}

// ===========================================================================
// Test  CentroidsSplitter.balanceGroups()
// ===========================================================================
/*
* This test verifies that after balanceGroups() finishes:
* The total vector counts of all groups are reasonably balanced
* within about 10% difference between the most loaded and least loaded group.
* balanceGroups() is the full balancing algorithm. It repeatedly:
* Calls trySwaps() to swap centroids between groups when that reduces imbalance.
* Calls tryMoves() to move centroids when imbalance > 10%.
* Stops when: No swaps or moves help anymore, or It reaches maxIters = 1000.
* after several iterations, all groups should have similar total vector counts.
 */
func (s *CentroidsSplitterSuite) TestBalanceGroupsConverges() {
	// Create random centroids with random weight
	n := 40
	counts := make([]int64, n)
	for i := range counts {
		counts[i] = int64(1 + rand.Intn(10000))
	}
	cs := CentroidsSplitter{Counts: counts}
	k := 5
	numGroups := (n + k - 1) / k
	groups, assign, gCounts := cs.initialAssign(numGroups)
	cs.balanceGroups(groups, assign, gCounts, k)
	// maxC = total vectors in the heaviest group
	// minC = total vectors in the lightest group
	// imbalance = how much heavier the max group is compared to the max value (as a ratio)
	// Example: If one group has 110,000 and another has 100,000 vectors -
	// imbalance = (110000 - 100000) / 110000 ≈ 0.09 = 9%
	maxC, minC := maxMin(gCounts)
	// After balancing the imbalance should be ≤ 10 % of max
	// (that's the threshold in balanceGroups for triggering moves)
	imbalance := float64(maxC-minC) / float64(maxC)
	// The expected threshold shall be <= 10% imbalance (plus 1% tolerance)
	s.True(imbalance <= 0.10+0.01,
		"imbalance %.2f%% should be ≤ ~10%% after balancing", imbalance*100)
}

// ===========================================================================
// 4.  Helper functions
// ===========================================================================

/*
Test that maxMin correctly returns the maximum (5) and minimum (1)
values from a slice.
*/
func (s *CentroidsSplitterSuite) TestMaxMin() {
	max, min := maxMin([]int64{3, 5, 1, 4})
	s.Equal(int64(5), max)
	s.Equal(int64(1), min)
}

func (s *CentroidsSplitterSuite) TestAbs() {
	s.Equal(int64(5), abs(-5))
	s.Equal(int64(5), abs(5))
	s.Equal(int64(0), abs(0))
}

/*
test removeAll
It shall remove all occurrences of a given integer from a slice of integers
*/
func (s *CentroidsSplitterSuite) TestRemoveAll() {
	g := []int{1, 2, 3, 2, 4}
	removeAll(&g, 2)
	s.Equal([]int{1, 3, 4}, g)
}

func (s *CentroidsSplitterSuite) TestRemoveAllNonExistent() {
	g := []int{1, 2, 3}
	removeAll(&g, 99)
	s.Equal([]int{1, 2, 3}, g)
}

/*
Test cloneGroups which performs a deep clone of a 2D slice ([][]int), not just a shallow copy.
*/
func (s *CentroidsSplitterSuite) TestCloneGroups() {
	orig := [][]int{{1, 2}, {3, 4}}
	clone := cloneGroups(orig)
	// Modify the clone
	clone[0][0] = 999
	// Check that the original was not changed
	s.Equal([][]int{{1, 2}, {3, 4}}, orig, "original must not be modified")
	s.NotEqual(orig, clone, "clone should differ from original after modification")
}

/*
*
This test ensures that cloneAssignment really creates a new, independent map, not just another reference to the same one.
*/
func (s *CentroidsSplitterSuite) TestCloneAssignment() {
	orig := map[int]int{0: 1, 1: 2}
	clone := cloneAssignment(orig)
	clone[0] = 999
	s.Equal(1, orig[0], "original must not be mutated")
}

func (s *CentroidsSplitterSuite) TestSumCounts() {
	counts := []int64{10, 20, 30, 40, 50}
	s.Equal(int64(90), sumCounts(counts, []int{0, 2, 4})) // indices 0,2,4 - 10+30+50 = 90
	s.Equal(int64(20), sumCounts(counts, []int{1}))       // index 1 - 20
	s.Equal(int64(0), sumCounts(counts, []int{}))         // empty indices - 0
}

func (s *CentroidsSplitterSuite) assertNoEmptyGroups(groups [][]int) {
	for i, g := range groups {
		s.NotEmpty(g, "group %d must not be empty", i)
	}
}

/*
Calculates the total count of each group of centroids.
Parameters:
  - counts []int64  a slice where counts[i] is the weight or count for centroid i.
    Example: counts = [100, 200, 300, 400]
  - groups [][]int  each element is a group, containing indices into the counts array.
    Example: groups = [[0, 1], [2, 3]]
    means: group 0 has centroids 0 and 1, group 1 has centroids 2 and 3

Returns:
  - A slice sums of total counts per group.
    Each sums[i] = total count (sum of counts) of all centroids in groups[i].
*/
func groupSums(counts []int64, groups [][]int) []int64 {
	sums := make([]int64, len(groups))
	// loop over each group g, where i is its index (group number)
	for i, g := range groups {
		// loop over all centroid indices in that group
		for _, idx := range g {
			sums[i] += counts[idx]
		}
	}
	return sums
}

// Helper to log groups and their counts/sums
func (s *CentroidsSplitterSuite) logGroups(counts []int64, groups [][]int) {
	sums := groupSums(counts, groups)
	for i, g := range groups {
		groupCounts := make([]int64, len(g))
		for j, idx := range g {
			groupCounts[j] = counts[idx]
		}
		s.log.Info("Group details",
			zap.Int("group", i),
			zap.Ints("centroidIndices", g),
			zap.Int64s("counts", groupCounts),
			zap.Int64("sum", sums[i]),
		)
	}
	maxSum, minSum := maxMin(sums)
	s.log.Info("Groups summary",
		zap.Int("numGroups", len(groups)),
		zap.Int64("maxSum", maxSum),
		zap.Int64("minSum", minSum),
		zap.Int64("diff", maxSum-minSum),
	)
}
