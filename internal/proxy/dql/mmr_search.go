// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dql

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	chaintypes "github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// mmrPage is the paging contract of an ordinary search whose rerank chain
// contains MMR. QueryNode returns the fixed candidate window from rank 0, the
// reduce keeps the whole window, and the rerank chain slices the user page out
// of the MMR order. The window depends only on limit, so every page of a
// paging session diversifies the same candidates.
type mmrPage struct {
	window int64
	limit  int64
	offset int64
}

// expectedRows is the page size MMR can return when QueryNode fills the
// window: short past the window end and empty beyond it.
func (p *mmrPage) expectedRows() int64 {
	return max(0, min(p.limit, p.window-p.offset))
}

// mmrWindow returns the MMR candidate window of a rerank configuration for
// the given page size, or false when it has no MMR step.
func mmrWindow(meta rerankMeta, limit int64) (int64, bool, error) {
	switch m := meta.(type) {
	case *funcScoreRerankMeta:
		mmrFunc := chain.FindMMRFunction(m.funcScore)
		if mmrFunc == nil {
			return 0, false, nil
		}
		window, err := chain.MMRCandidateSize(mmrFunc, limit)
		return window, true, err
	case *functionChainRerankMeta:
		for _, op := range m.repr.Operators {
			if op.Type != chaintypes.OpTypeMMR {
				continue
			}
			reader := chaintypes.NewParamReader("mmr", op.Params)
			window, err := reader.Int64(chaintypes.MMRParamCandidateSize, false, chaintypes.MMRMinDefaultCandidateSize)
			if err != nil {
				return 0, true, err
			}
			return window, true, chain.ValidateMMRWindow(window, limit)
		}
	}
	return 0, false, nil
}

// fillDefaultMMRInputField makes the ANN field the MMR vector input when the
// request leaves it out.
func fillDefaultMMRInputField(funcScore *schemapb.FunctionScore, annsField string) {
	mmrFunc := chain.FindMMRFunction(funcScore)
	if mmrFunc == nil || len(mmrFunc.GetInputFieldNames()) > 0 || annsField == "" {
		return
	}
	mmrFunc.InputFieldNames = []string{annsField}
}

// annsFieldName returns the name of the searched vector field.
func annsFieldName(schema *schemapb.CollectionSchema, fieldID int64) string {
	return typeutil.GetFieldByID(schema, fieldID).GetName()
}

// initMMRPage switches an ordinary search with MMR to window retrieval: the
// QueryNode top-k becomes the window and reduce no longer applies offset. It
// must run before the plan is serialized.
func (t *SearchTask) initMMRPage(queryInfo *planpb.QueryInfo, offset int64) error {
	limit := queryInfo.GetTopk() - offset
	window, ok, err := mmrWindow(t.rerankMeta, limit)
	if err != nil || !ok {
		return err
	}
	if t.aggCtx != nil {
		return merr.WrapErrParameterInvalidMsg("mmr is not supported with search_aggregation")
	}
	if queryInfo.GetGroupByFieldId() > 0 || len(queryInfo.GetGroupByFieldIds()) > 0 {
		return merr.WrapErrParameterInvalidMsg("mmr: cannot be combined with group_by")
	}
	if err := validateLimit(window, t.largeTopKEnabled); err != nil {
		return merr.WrapErrParameterInvalidMsg("mmr: candidate_size [%d] is invalid, %v", window, err)
	}
	t.mmrPage = &mmrPage{window: window, limit: limit, offset: offset}
	queryInfo.Topk = window
	t.Offset = 0
	return nil
}
