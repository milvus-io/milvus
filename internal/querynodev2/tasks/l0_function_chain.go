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

package tasks

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/arrow/memory/mallocator"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	chaintypes "github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type l0RerankChainBuilder func(context.Context, int, *chain.DataFrame) (*chain.FuncChain, error)

var newL0RerankAllocator = func() memory.Allocator {
	return mallocator.NewMallocator()
}

func appendL0RerankReduceContract(fc *chain.FuncChain) *chain.FuncChain {
	fc.Sort(chaintypes.ScoreFieldName, true, chaintypes.IDFieldName)
	return fc
}

func executeL0RerankChains(ctx context.Context, segDFs []*chain.DataFrame, buildChain l0RerankChainBuilder, errPrefix string) error {
	rerankedDFs := make([]*chain.DataFrame, len(segDFs))
	executeOneSegment := func(ctx context.Context, i int) error {
		df := segDFs[i]
		if df == nil {
			return merr.WrapErrServiceInternal(fmt.Sprintf("%s: DataFrame %d is nil", errPrefix, i))
		}

		fc, err := buildChain(ctx, i, df)
		if err != nil {
			return err
		}
		if fc == nil {
			return merr.WrapErrServiceInternal(fmt.Sprintf("%s: function chain %d is nil", errPrefix, i))
		}

		reranked, err := fc.ExecuteWithOptions(ctx, chain.ExecuteOptions{
			EnableColumnPruning: true,
		}, df)
		if err != nil {
			return err
		}
		rerankedDFs[i] = reranked
		return nil
	}

	if len(segDFs) == 1 {
		if err := executeOneSegment(ctx, 0); err != nil {
			return err
		}
	} else {
		errGroup, groupCtx := errgroup.WithContext(ctx)
		for i := range segDFs {
			idx := i
			errGroup.Go(func() error {
				return executeOneSegment(groupCtx, idx)
			})
		}
		if err := errGroup.Wait(); err != nil {
			for _, reranked := range rerankedDFs {
				if reranked != nil {
					reranked.Release()
				}
			}
			return err
		}
	}

	for i, reranked := range rerankedDFs {
		segDFs[i].Release()
		segDFs[i] = reranked
	}
	return nil
}

func (t *SearchTask) applyL0Rerank(segDFs []*chain.DataFrame, prepared *preparedL0Rerank, searchedSegments []segments.Segment, searchReq *segcore.SearchRequest) (retErr error) {
	if prepared == nil {
		return nil
	}

	start := time.Now()
	defer func() {
		status := metrics.SuccessLabel
		if retErr != nil {
			status = metrics.FailLabel
		}
		metrics.QueryNodeFunctionChainLatency.WithLabelValues(
			fmt.Sprint(t.GetNodeID()),
			metrics.FunctionChainLevelL0,
			status,
		).Observe(float64(time.Since(start).Microseconds()) / 1000.0)
	}()

	switch {
	case prepared.chain != nil && prepared.boostScore != nil:
		return merr.WrapErrServiceInternalMsg("l0_rerank: public chain and boost score are both prepared")
	case prepared.chain != nil:
		return t.applyPublicL0Rerank(segDFs, prepared)
	case prepared.boostScore != nil:
		return t.applyPreparedBoostScores(segDFs, prepared.boostScore, searchedSegments, searchReq)
	default:
		return merr.WrapErrServiceInternalMsg("l0_rerank: prepared L0 rerank has no implementation")
	}
}

func (t *SearchTask) applyPublicL0Rerank(segDFs []*chain.DataFrame, prepared *preparedL0Rerank) error {
	if prepared == nil || prepared.chain == nil {
		return merr.WrapErrServiceInternalMsg("l0_rerank: prepared L0 function chain is nil")
	}
	if len(segDFs) == 0 {
		return nil
	}
	if segDFs[0] == nil {
		return merr.WrapErrServiceInternal("l0_rerank: DataFrame 0 is nil")
	}

	repr := prepared.chain
	// Public L0 avoids reparsing proto by reusing the prepared ChainRepr, but builds
	// a fresh FuncChain for each segment so operator/function execution state is not
	// shared across concurrent per-segment execution.
	return executeL0RerankChains(t.ctx, segDFs, func(context.Context, int, *chain.DataFrame) (*chain.FuncChain, error) {
		fc, err := chain.FuncChainFromReprWithContext(repr, newL0RerankAllocator(), chaintypes.FunctionBuildContext{})
		if err != nil {
			return nil, err
		}
		return appendL0RerankReduceContract(fc), nil
	}, "l0_rerank")
}

func validateL0FunctionChainOps(repr *chain.ChainRepr) error {
	if repr == nil {
		return merr.WrapErrParameterInvalidMsg("function chain repr is nil")
	}
	for opIdx, op := range repr.Operators {
		if op.Type != chaintypes.OpTypeMap {
			return merr.WrapErrParameterInvalidMsg("op[%d] type %q is not supported by L0 rerank function chain", opIdx, op.Type)
		}
	}
	return nil
}

func validateL0FunctionChainSystemOutputs(repr *chain.ChainRepr) error {
	if repr == nil {
		return merr.WrapErrParameterInvalidMsg("function chain repr is nil")
	}
	for opIdx, op := range repr.Info.Ops {
		for _, output := range op.WriteNames {
			if !chain.IsFunctionChainSystemName(output) {
				continue
			}
			if output != chaintypes.ScoreFieldName {
				return merr.WrapErrParameterInvalidMsg("op[%d] system output %q is not writable by L0 rerank function chain", opIdx, output)
			}
		}
	}
	return nil
}
