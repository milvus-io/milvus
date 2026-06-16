/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package chain

import "github.com/milvus-io/milvus/pkg/v3/util/merr"

// ColumnSet is a small set helper for column names.
type ColumnSet map[string]struct{}

// NewColumnSet creates a ColumnSet from column names.
func NewColumnSet(cols ...string) ColumnSet {
	set := make(ColumnSet, len(cols))
	for _, col := range cols {
		set.Add(col)
	}
	return set
}

// Add adds a column name to the set.
func (s ColumnSet) Add(col string) {
	if col == "" {
		return
	}
	s[col] = struct{}{}
}

// Remove removes a column name from the set.
func (s ColumnSet) Remove(col string) {
	delete(s, col)
}

// Contains reports whether the set contains a column name.
func (s ColumnSet) Contains(col string) bool {
	_, ok := s[col]
	return ok
}

// Clone returns a copy of the set.
func (s ColumnSet) Clone() ColumnSet {
	clone := make(ColumnSet, len(s))
	for col := range s {
		clone[col] = struct{}{}
	}
	return clone
}

// RemoveSystemColumns removes function-chain system columns from the set.
func (s ColumnSet) RemoveSystemColumns() {
	for col := range s {
		if IsFunctionChainSystemName(col) {
			delete(s, col)
		}
	}
}

// DownstreamSpec describes non-system columns consumed after FuncChain execution.
// It is the liveness root for optimization, not the final API response projection.
type DownstreamSpec struct {
	RequiredColumns []string
}

// SystemColumnPolicy controls how pruning treats function-chain system columns.
type SystemColumnPolicy struct {
	KeepAllSystemColumns bool
}

func normalizeSystemColumnPolicy(policy SystemColumnPolicy) SystemColumnPolicy {
	// The current conservative default is to retain all existing system columns.
	if !policy.KeepAllSystemColumns {
		policy.KeepAllSystemColumns = true
	}
	return policy
}

// ExecuteOptions controls optional FuncChain execution optimizations.
type ExecuteOptions struct {
	EnableColumnPruning bool
	EnableParallel      bool // reserved for future schedule support

	Downstream         DownstreamSpec
	SystemColumnPolicy SystemColumnPolicy
}

// LivenessInfo contains non-system column liveness for each operator.
type LivenessInfo struct {
	LiveBefore []ColumnSet
	LiveAfter  []ColumnSet
}

// OptimizationPlan is non-executable metadata used by FuncChain.ExecuteWithOptions.
type OptimizationPlan struct {
	Liveness    *LivenessInfo
	PruneBefore []ColumnSet
	PruneAfter  []ColumnSet

	SystemColumnPolicy SystemColumnPolicy
}

type functionChainPlanner struct {
	operators []Operator
	opts      ExecuteOptions
}

func (fc *FuncChain) buildOptimizationPlan(opts ExecuteOptions) (*OptimizationPlan, error) {
	planner := functionChainPlanner{
		operators: fc.operators,
		opts:      opts,
	}
	return planner.Plan()
}

func (p functionChainPlanner) Plan() (*OptimizationPlan, error) {
	if !p.opts.EnableColumnPruning && !p.opts.EnableParallel {
		return nil, nil
	}
	if p.opts.EnableParallel {
		return nil, merr.WrapErrServiceInternal("function chain parallel execution is not implemented")
	}
	if err := p.validateOperatorMetadata(); err != nil {
		return nil, err
	}

	liveness := p.analyzeLiveness()
	return &OptimizationPlan{
		Liveness:           liveness,
		PruneBefore:        liveness.LiveBefore,
		PruneAfter:         liveness.LiveAfter,
		SystemColumnPolicy: normalizeSystemColumnPolicy(p.opts.SystemColumnPolicy),
	}, nil
}

func (p functionChainPlanner) validateOperatorMetadata() error {
	for i, op := range p.operators {
		if op == nil {
			return merr.WrapErrServiceInternalMsg("operator[%d] is nil", i)
		}
		for _, input := range op.Inputs() {
			if input == "" {
				return merr.WrapErrServiceInternalMsg("operator[%d] %s has empty input column", i, op.Name())
			}
		}
		seenOutputs := make(map[string]struct{}, len(op.Outputs()))
		for _, output := range op.Outputs() {
			if output == "" {
				return merr.WrapErrServiceInternalMsg("operator[%d] %s has empty output column", i, op.Name())
			}
			if _, ok := seenOutputs[output]; ok {
				return merr.WrapErrServiceInternalMsg("operator[%d] %s has duplicate output column %q", i, op.Name(), output)
			}
			seenOutputs[output] = struct{}{}
		}
	}
	for _, col := range p.opts.Downstream.RequiredColumns {
		if col == "" {
			return merr.WrapErrServiceInternal("downstream required column is empty")
		}
	}
	return nil
}

func (p functionChainPlanner) analyzeLiveness() *LivenessInfo {
	n := len(p.operators)
	liveBefore := make([]ColumnSet, n)
	liveAfter := make([]ColumnSet, n)

	live := NewColumnSet(p.opts.Downstream.RequiredColumns...)
	live.RemoveSystemColumns()

	for i := n - 1; i >= 0; i-- {
		op := p.operators[i]

		liveAfter[i] = live.Clone()
		next := live.Clone()

		for _, output := range op.Outputs() {
			if IsFunctionChainSystemName(output) {
				continue
			}
			next.Remove(output)
		}

		for _, input := range op.Inputs() {
			if IsFunctionChainSystemName(input) {
				continue
			}
			next.Add(input)
		}

		liveBefore[i] = next.Clone()
		live = next
	}

	return &LivenessInfo{
		LiveBefore: liveBefore,
		LiveAfter:  liveAfter,
	}
}
