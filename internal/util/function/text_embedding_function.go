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

package function

import (
	"fmt"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
)

const (
	Provider string = "provider"
)

const (
	openAIProvider       string = "openai"
	azureOpenAIProvider  string = "azure_openai"
	aliDashScopeProvider string = "dashscope"
	bedrockProvider      string = "bedrock"
	vertexAIProvider     string = "vertexai"
	voyageAIProvider     string = "voyageai"
)

// Text embedding for retrieval task
type textEmbeddingProvider interface {
	MaxBatch() int
	CallEmbedding(texts []string, mode TextEmbeddingMode) ([][]float32, error)
	FieldDim() int64
}

func getProvider(functionSchema *schemapb.FunctionSchema) (string, error) {
	for _, param := range functionSchema.Params {
		switch strings.ToLower(param.Key) {
		case Provider:
			return strings.ToLower(param.Value), nil
		default:
		}
	}
	return "", fmt.Errorf("The text embedding service provider parameter:[%s] was not found", Provider)
}

type TextEmbeddingFunction struct {
	FunctionBase

	embProvider textEmbeddingProvider
}

func NewTextEmbeddingFunction(coll *schemapb.CollectionSchema, functionSchema *schemapb.FunctionSchema) (*TextEmbeddingFunction, error) {
	if len(functionSchema.GetOutputFieldNames()) != 1 {
		return nil, fmt.Errorf("Text function should only have one output field, but now is %d", len(functionSchema.GetOutputFieldNames()))
	}

	base, err := NewFunctionBase(coll, functionSchema)
	if err != nil {
		return nil, err
	}

	if base.outputFields[0].DataType != schemapb.DataType_FloatVector {
		return nil, fmt.Errorf("Text embedding function's output field not match, needs [%s], got [%s]",
			schemapb.DataType_name[int32(schemapb.DataType_FloatVector)],
			schemapb.DataType_name[int32(base.outputFields[0].DataType)])
	}

	provider, err := getProvider(functionSchema)
	if err != nil {
		return nil, err
	}
	switch provider {
	case openAIProvider:
		embP, err := NewOpenAIEmbeddingProvider(base.outputFields[0], functionSchema)
		if err != nil {
			return nil, err
		}
		return &TextEmbeddingFunction{
			FunctionBase: *base,
			embProvider:  embP,
		}, nil
	case azureOpenAIProvider:
		embP, err := NewAzureOpenAIEmbeddingProvider(base.outputFields[0], functionSchema)
		if err != nil {
			return nil, err
		}
		return &TextEmbeddingFunction{
			FunctionBase: *base,
			embProvider:  embP,
		}, nil
	case bedrockProvider:
		embP, err := NewBedrockEmbeddingProvider(base.outputFields[0], functionSchema, nil)
		if err != nil {
			return nil, err
		}
		return &TextEmbeddingFunction{
			FunctionBase: *base,
			embProvider:  embP,
		}, nil
	case aliDashScopeProvider:
		embP, err := NewAliDashScopeEmbeddingProvider(base.outputFields[0], functionSchema)
		if err != nil {
			return nil, err
		}
		return &TextEmbeddingFunction{
			FunctionBase: *base,
			embProvider:  embP,
		}, nil
	case vertexAIProvider:
		embP, err := NewVertexAIEmbeddingProvider(base.outputFields[0], functionSchema, nil)
		if err != nil {
			return nil, err
		}
		return &TextEmbeddingFunction{
			FunctionBase: *base,
			embProvider:  embP,
		}, nil
	case voyageAIProvider:
		embP, err := NewVoyageAIEmbeddingProvider(base.outputFields[0], functionSchema)
		if err != nil {
			return nil, err
		}
		return &TextEmbeddingFunction{
			FunctionBase: *base,
			embProvider:  embP,
		}, nil
	default:
		return nil, fmt.Errorf("Unsupported text embedding service provider: [%s] , list of supported [%s, %s, %s, %s, %s, %s]", provider, openAIProvider, azureOpenAIProvider, aliDashScopeProvider, bedrockProvider, vertexAIProvider, voyageAIProvider)
	}
}

func (runner *TextEmbeddingFunction) MaxBatch() int {
	return runner.embProvider.MaxBatch()
}

func (runner *TextEmbeddingFunction) ProcessInsert(inputs []*schemapb.FieldData) ([]*schemapb.FieldData, error) {
	if len(inputs) != 1 {
		return nil, fmt.Errorf("Text embedding function only receives one input field, but got [%d]", len(inputs))
	}

	if inputs[0].Type != schemapb.DataType_VarChar {
		return nil, fmt.Errorf("Text embedding only supports varchar field as input field, but got %s", schemapb.DataType_name[int32(inputs[0].Type)])
	}

	texts := inputs[0].GetScalars().GetStringData().GetData()
	if texts == nil {
		return nil, fmt.Errorf("Input texts is empty")
	}
	numRows := len(texts)
	if numRows > runner.MaxBatch() {
		return nil, fmt.Errorf("Embedding supports up to [%d] pieces of data at a time, got [%d]", runner.MaxBatch(), numRows)
	}
	embds, err := runner.embProvider.CallEmbedding(texts, InsertMode)
	if err != nil {
		return nil, err
	}
	data := make([]float32, 0, len(texts)*int(runner.embProvider.FieldDim()))
	for _, emb := range embds {
		data = append(data, emb...)
	}

	var outputField schemapb.FieldData
	outputField.FieldId = runner.outputFields[0].FieldID
	outputField.FieldName = runner.outputFields[0].Name
	outputField.Type = runner.outputFields[0].DataType
	outputField.IsDynamic = runner.outputFields[0].IsDynamic
	outputField.Field = &schemapb.FieldData_Vectors{
		Vectors: &schemapb.VectorField{
			Data: &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{
					Data: data,
				},
			},
			Dim: runner.embProvider.FieldDim(),
		},
	}
	return []*schemapb.FieldData{&outputField}, nil
}

func (runner *TextEmbeddingFunction) ProcessSearch(placeholderGroup *commonpb.PlaceholderGroup) (*commonpb.PlaceholderGroup, error) {
	texts := funcutil.GetVarCharFromPlaceholder(placeholderGroup.Placeholders[0]) // Already checked externally
	numRows := len(texts)
	if numRows > runner.MaxBatch() {
		return nil, fmt.Errorf("Embedding supports up to [%d] pieces of data at a time, got [%d]", runner.MaxBatch(), numRows)
	}
	embds, err := runner.embProvider.CallEmbedding(texts, SearchMode)
	if err != nil {
		return nil, err
	}
	return funcutil.Float32VectorsToPlaceholderGroup(embds), nil
}

func (runner *TextEmbeddingFunction) ProcessBulkInsert(inputs []storage.FieldData) (map[storage.FieldID]storage.FieldData, error) {
	if len(inputs) != 1 {
		return nil, fmt.Errorf("TextEmbedding function only receives one input, bug got [%d]", len(inputs))
	}

	if inputs[0].GetDataType() != schemapb.DataType_VarChar {
		return nil, fmt.Errorf(" only supports varchar field, the input is not varchar")
	}

	texts, ok := inputs[0].GetDataRows().([]string)
	if !ok {
		return nil, fmt.Errorf("Input texts is empty")
	}

	embds, err := runner.embProvider.CallEmbedding(texts, InsertMode)
	if err != nil {
		return nil, err
	}
	data := make([]float32, 0, len(texts)*int(runner.embProvider.FieldDim()))
	for _, emb := range embds {
		data = append(data, emb...)
	}

	field := &storage.FloatVectorFieldData{
		Data: data,
		Dim:  int(runner.embProvider.FieldDim()),
	}
	return map[storage.FieldID]storage.FieldData{
		runner.outputFields[0].FieldID: field,
	}, nil
}
