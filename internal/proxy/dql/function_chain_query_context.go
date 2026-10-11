// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dql

import (
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	chaintypes "github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func chainsNeedQueryContext(plans []*schemapb.FunctionChain) bool {
	for _, plan := range plans {
		for _, op := range plan.GetOps() {
			if strings.EqualFold(op.GetExpr().GetName(), "py_udf") {
				return true
			}
		}
	}
	return false
}

func (t *SearchTask) needsQueryContext() bool {
	if chainsNeedQueryContext(t.request.GetFunctionChains()) {
		return true
	}
	for _, sub := range t.request.GetSubReqs() {
		if chainsNeedQueryContext(sub.GetFunctionChains()) {
			return true
		}
	}
	return false
}

// snapshotQueryInputs decodes owned request snapshots before native embedding
// replaces text with vectors. It retains every HybridSearch ANN input.
func (t *SearchTask) snapshotQueryInputs() ([]*commonpb.PlaceholderValue, error) {
	inputs := [][]byte{t.request.GetPlaceholderGroup()}
	if t.GetIsAdvanced() {
		inputs = make([][]byte, len(t.request.GetSubReqs()))
		for index, sub := range t.request.GetSubReqs() {
			inputs[index] = sub.GetPlaceholderGroup()
		}
	}
	result := make([]*commonpb.PlaceholderValue, len(inputs))
	for index, raw := range inputs {
		group := &commonpb.PlaceholderGroup{}
		if err := proto.Unmarshal(raw, group); err != nil || len(group.GetPlaceholders()) != 1 {
			return nil, merr.WrapErrParameterInvalidMsg("query context input %d requires one valid query placeholder", index)
		}
		input := group.Placeholders[0]
		if _, known := commonpb.PlaceholderType_name[int32(input.GetType())]; !known || input.GetType() == commonpb.PlaceholderType_None {
			return nil, merr.WrapErrParameterInvalidMsg("query context input %d has an unsupported placeholder type", index)
		}
		if int64(len(input.GetValues())) != t.GetNq() {
			return nil, merr.WrapErrParameterInvalidMsg("query context input %d does not match NQ %d", index, t.GetNq())
		}
		if input.GetType() == commonpb.PlaceholderType_VarChar {
			for _, value := range input.GetValues() {
				if !utf8.Valid(value) {
					return nil, merr.WrapErrParameterInvalidMsg("query context input %d is not valid UTF-8", index)
				}
			}
		}
		result[index] = input
	}
	return result, nil
}

func (t *SearchTask) prepareQueryContext() error {
	if !t.needsQueryContext() {
		return nil
	}
	queries, err := t.snapshotQueryInputs()
	if err != nil {
		return err
	}
	t.queryContext = &chaintypes.QueryContext{OriginalQueries: queries, Queries: queries}
	return nil
}
