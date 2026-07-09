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
package proxy

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	chaintypes "github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type functionChainRerankMeta struct {
	inputFieldNames []string
	inputFieldIDs   []int64
	chainPB         *schemapb.FunctionChain
	repr            *chain.ChainRepr
}

func (m *functionChainRerankMeta) GetInputFieldNames() []string { return m.inputFieldNames }
func (m *functionChainRerankMeta) GetInputFieldIDs() []int64    { return m.inputFieldIDs }

func hasFunctionRerank(request *milvuspb.SearchRequest) bool {
	return request.GetFunctionScore() != nil || len(request.GetFunctionChains()) > 0
}

func validateFunctionChainSearchRequest(request *milvuspb.SearchRequest, isAdvanced bool) error {
	if request.GetFunctionScore() != nil && len(request.GetFunctionChains()) > 0 {
		return merr.WrapErrParameterInvalidMsg("function_score and function_chains cannot be used together")
	}
	if isAdvanced && len(request.GetFunctionChains()) > 0 {
		return merr.WrapErrParameterInvalidMsg("function_chains is not supported for hybrid search yet")
	}
	return nil
}

func splitFunctionChainsByStage(chains []*schemapb.FunctionChain) ([]*schemapb.FunctionChain, []*schemapb.FunctionChain, error) {
	l2Chains := make([]*schemapb.FunctionChain, 0)
	querynodeChains := make([]*schemapb.FunctionChain, 0)
	seenStages := make(map[schemapb.FunctionChainStage]struct{}, len(chains))

	for i, chainPB := range chains {
		if chainPB == nil {
			return nil, nil, merr.WrapErrParameterInvalidMsg("function chain[%d] is nil", i)
		}
		stage := chainPB.GetStage()
		if _, ok := seenStages[stage]; ok {
			return nil, nil, merr.WrapErrParameterInvalidMsg("function chain stage %s appears more than once", stage.String())
		}
		seenStages[stage] = struct{}{}

		switch stage {
		case schemapb.FunctionChainStage_FunctionChainStageL2Rerank:
			l2Chains = append(l2Chains, chainPB)
		case schemapb.FunctionChainStage_FunctionChainStageL0Rerank:
			querynodeChains = append(querynodeChains, chainPB)
		case schemapb.FunctionChainStage_FunctionChainStageL1Rerank:
			return nil, nil, merr.WrapErrParameterInvalidMsg("function chain[%d] stage %s is not supported yet", i, stage.String())
		default:
			return nil, nil, merr.WrapErrParameterInvalidMsg("function chain[%d] stage %s is not supported in search request", i, stage.String())
		}
	}

	return l2Chains, querynodeChains, nil
}

func newFunctionChainRerankMeta(chains []*schemapb.FunctionChain, schema *schemaInfo) (*functionChainRerankMeta, error) {
	if len(chains) == 0 {
		return nil, nil
	}

	seenStages := make(map[schemapb.FunctionChainStage]struct{}, len(chains))
	var chainPB *schemapb.FunctionChain
	var repr *chain.ChainRepr

	for i, pb := range chains {
		if pb == nil {
			return nil, merr.WrapErrParameterInvalidMsg("function chain[%d] is nil", i)
		}
		stage := pb.GetStage()
		if _, ok := seenStages[stage]; ok {
			return nil, merr.WrapErrParameterInvalidMsg("function chain stage %s appears more than once", stage.String())
		}
		seenStages[stage] = struct{}{}

		if stage != schemapb.FunctionChainStage_FunctionChainStageL2Rerank {
			return nil, merr.WrapErrParameterInvalidMsg("function chain[%d] stage %s is not supported in search request", i, stage.String())
		}
		if len(pb.GetOps()) == 0 {
			return nil, merr.WrapErrParameterInvalidMsg("function chain[%d] must contain at least one op", i)
		}

		r, err := chain.ProtoChainToRepr(pb)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("function chain[%d]: %v", i, err)
		}
		if err := validateL2RerankSystemNames(r); err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("function chain[%d]: %v", i, err)
		}
		chainPB = pb
		repr = r
	}

	inputFieldNames, inputFieldIDs, err := planL2FunctionChainInputs(repr, schema)
	if err != nil {
		return nil, err
	}

	return &functionChainRerankMeta{
		inputFieldNames: inputFieldNames,
		inputFieldIDs:   inputFieldIDs,
		chainPB:         chainPB,
		repr:            repr,
	}, nil
}

func planL2FunctionChainInputs(repr *chain.ChainRepr, schema *schemaInfo) ([]string, []int64, error) {
	if repr == nil {
		return nil, nil, merr.WrapErrParameterInvalidMsg("function chain repr is nil")
	}

	inputFieldNames := make([]string, 0)
	inputFieldIDs := make([]int64, 0)
	seenInputFields := make(map[string]struct{})

	for _, input := range repr.Info.RequiredInputs {
		if chain.IsFunctionChainSystemName(input) {
			if err := validateL2RerankSystemInput(input); err != nil {
				return nil, nil, err
			}
			continue
		}

		_, fieldID, err := getFunctionChainInputField(schema, input)
		if err != nil {
			return nil, nil, err
		}

		if _, ok := seenInputFields[input]; ok {
			continue
		}
		seenInputFields[input] = struct{}{}
		inputFieldNames = append(inputFieldNames, input)
		inputFieldIDs = append(inputFieldIDs, fieldID)
	}

	return inputFieldNames, inputFieldIDs, nil
}

func validateL2RerankSystemNames(repr *chain.ChainRepr) error {
	if repr == nil {
		return merr.WrapErrParameterInvalidMsg("function chain repr is nil")
	}
	for opIdx, op := range repr.Info.Ops {
		for _, input := range op.ReadNames {
			if !chain.IsFunctionChainSystemName(input) {
				continue
			}
			if err := validateL2RerankSystemInput(input); err != nil {
				return merr.WrapErrParameterInvalidMsg("op[%d] input %q: %v", opIdx, input, err)
			}
		}
		for _, output := range op.WriteNames {
			if !chain.IsFunctionChainSystemName(output) {
				continue
			}
			if err := validateL2RerankSystemOutput(output); err != nil {
				return merr.WrapErrParameterInvalidMsg("op[%d] output %q: %v", opIdx, output, err)
			}
		}
	}
	return nil
}

func validateL2RerankSystemInput(name string) error {
	switch name {
	case chaintypes.IDFieldName, chaintypes.ScoreFieldName:
		return nil
	default:
		return merr.WrapErrParameterInvalidMsg("system input %q is not supported by L2 rerank function chain", name)
	}
}

func validateL2RerankSystemOutput(name string) error {
	switch name {
	case chaintypes.ScoreFieldName:
		return nil
	default:
		return merr.WrapErrParameterInvalidMsg("system output %q is not writable by L2 rerank function chain", name)
	}
}

func getFunctionChainInputField(schema *schemaInfo, name string) (*schemapb.FieldSchema, int64, error) {
	if schema == nil || schema.schemaHelper == nil {
		return nil, 0, merr.WrapErrParameterInvalidMsg("function chain input %q is neither a previous output nor a collection field", name)
	}

	field, err := schema.schemaHelper.GetFieldFromName(name)
	if err != nil {
		return nil, 0, merr.WrapErrParameterInvalidMsg("function chain input %q is neither a previous output nor a collection field", name)
	}

	return validateFunctionChainInputField(name, field, field.GetFieldID())
}

func validateFunctionChainInputField(name string, field *schemapb.FieldSchema, fieldID int64) (*schemapb.FieldSchema, int64, error) {
	if _, err := chain.ToArrowType(field.GetDataType()); err != nil {
		return nil, 0, merr.WrapErrParameterInvalidMsg("function chain input %q has unsupported field type %s", name, field.GetDataType().String())
	}
	return field, fieldID, nil
}
