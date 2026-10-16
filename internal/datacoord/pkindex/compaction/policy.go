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

package compaction

// rule is the trigger rule that picked a compaction.
type rule uint8

const (
	ruleNone     rule = iota
	ruleL0            // the oldest flushed tables become a run
	ruleMerge         // consecutive runs of similar size merge into one run
	ruleSpaceAmp      // every table merges into one run
)

func (r rule) String() string {
	switch r {
	case ruleL0:
		return "l0"
	case ruleMerge:
		return "merge"
	case ruleSpaceAmp:
		return "spaceamp"
	}
	return "none"
}

// choice is what a rule picked on one group list: groups[first:last+1] are
// the inputs.
type choice struct {
	rule           rule
	score          float64
	first, last    int
	dropTombstones bool
}

// evaluate returns the choice with the highest score, if any rule applies. On
// equal scores the flushed-table rule wins, then the merge rule, then the
// space-amplification rule. It expects a normalized config.
func evaluate(gs []group, cfg Config) (choice, bool) {
	var best choice
	consider := func(c choice, ok bool) {
		if ok && (best.rule == ruleNone || c.score > best.score) {
			best = c
		}
	}
	consider(pickL0(gs, cfg))
	consider(pickMerge(gs, cfg))
	consider(pickSpaceAmp(gs, cfg))
	return best, best.rule != ruleNone
}

// pickL0 applies when at least L0Trigger tables are flushed tables. The
// inputs are the oldest of them: at least two, at most MaxInputTables, and
// no more than MaxInputBytes once two are in. Tombstones drop only when the
// inputs reach the end of the list.
func pickL0(gs []group, cfg Config) (choice, bool) {
	l0 := l0Prefix(gs)
	if l0 < cfg.L0Trigger {
		return choice{}, false
	}
	first := l0 - 1
	bytes := gs[first].bytes
	for first > 0 && l0-first < cfg.MaxInputTables && (l0-first < 2 || bytes+gs[first-1].bytes <= cfg.MaxInputBytes) {
		first--
		bytes += gs[first].bytes
	}
	return choice{
		rule:           ruleL0,
		score:          float64(l0) / float64(cfg.L0Trigger),
		first:          first,
		last:           l0 - 1,
		dropTombstones: l0 == len(gs),
	}, true
}

// pickMerge applies when at least MinMergeRuns consecutive runs have sizes
// within SizeRatio of each other and no more than MaxInputBytes in all. Of
// the windows that qualify, it takes the one with the most runs, then the
// fewest bytes. Tombstones drop only when the window ends at the last run.
func pickMerge(gs []group, cfg Config) (choice, bool) {
	l0 := l0Prefix(gs)
	runs := gs[l0:]
	var best choice
	var bestN int
	var bestBytes int64
	for i := 0; i+cfg.MinMergeRuns <= len(runs); i++ {
		minB, maxB, total := runs[i].bytes, runs[i].bytes, int64(0)
		for j := i; j < len(runs); j++ {
			b := runs[j].bytes
			minB, maxB, total = min(minB, b), max(maxB, b), total+b
			if float64(maxB) > cfg.SizeRatio*float64(minB) || total > cfg.MaxInputBytes {
				break
			}
			n := j - i + 1
			if n < cfg.MinMergeRuns {
				continue
			}
			if bestN == 0 || n > bestN || (n == bestN && total < bestBytes) {
				best = choice{rule: ruleMerge, score: float64(n) / float64(cfg.MinMergeRuns), first: l0 + i, last: l0 + j, dropTombstones: j == len(runs)-1}
				bestN, bestBytes = n, total
			}
		}
	}
	return best, bestN > 0
}

// pickSpaceAmp applies when at least two runs exist and the bytes outside the
// oldest run reach SpaceAmpRatio times the oldest run. Every table is an
// input and tombstones drop. This rule has no input cap because its purpose
// is to rewrite everything. MaxInputTables applies to the flushed-table rule
// only, and MaxInputBytes to the flushed-table and merge rules.
func pickSpaceAmp(gs []group, cfg Config) (choice, bool) {
	l0 := l0Prefix(gs)
	if len(gs)-l0 < 2 {
		return choice{}, false
	}
	last := gs[len(gs)-1].bytes
	var total int64
	for _, g := range gs {
		total += g.bytes
	}
	if last <= 0 || float64(total-last) < cfg.SpaceAmpRatio*float64(last) {
		return choice{}, false
	}
	return choice{
		rule:           ruleSpaceAmp,
		score:          float64(total-last) / (float64(last) * cfg.SpaceAmpRatio),
		first:          0,
		last:           len(gs) - 1,
		dropTombstones: true,
	}, true
}
