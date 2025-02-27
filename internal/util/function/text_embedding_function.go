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
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
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
	cohereProvider       string = "cohere"
	siliconflowProvider  string = "siliconflow"
	teiProvider          string = "tei"
)

// Text embedding for retrieval task
type textEmbeddingProvider interface {
	MaxBatch() int
	CallEmbedding(texts []string, mode TextEmbeddingMode) (any, error)
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

func isValidInputDataType(dataType schemapb.DataType) bool {
	return dataType == schemapb.DataType_VarChar || dataType == schemapb.DataType_Text
}

func NewTextEmbeddingFunction(coll *schemapb.CollectionSchema, functionSchema *schemapb.FunctionSchema) (*TextEmbeddingFunction, error) {
	if len(functionSchema.GetOutputFieldNames()) != 1 {
		return nil, fmt.Errorf("Text function should only have one output field, but now is %d", len(functionSchema.GetOutputFieldNames()))
	}

	base, err := NewFunctionBase(coll, functionSchema)
	if err != nil {
		return nil, err
	}

	if base.outputFields[0].DataType != schemapb.DataType_FloatVector && base.outputFields[0].DataType != schemapb.DataType_Int8Vector {
		return nil, fmt.Errorf("Text embedding function's output field not match, needs [%s, %s], got [%s]",
			schemapb.DataType_name[int32(schemapb.DataType_FloatVector)],
			schemapb.DataType_name[int32(schemapb.DataType_Int8Vector)],
			schemapb.DataType_name[int32(base.outputFields[0].DataType)])
	}

	provider, err := getProvider(functionSchema)
	if err != nil {
		return nil, err
	}
	var embP textEmbeddingProvider
	var newProviderErr error
	switch provider {
	case openAIProvider:
		embP, newProviderErr = NewOpenAIEmbeddingProvider(base.outputFields[0], functionSchema)
	case azureOpenAIProvider:
		embP, newProviderErr = NewAzureOpenAIEmbeddingProvider(base.outputFields[0], functionSchema)
	case bedrockProvider:
		embP, newProviderErr = NewBedrockEmbeddingProvider(base.outputFields[0], functionSchema, nil)
	case aliDashScopeProvider:
		embP, newProviderErr = NewAliDashScopeEmbeddingProvider(base.outputFields[0], functionSchema)
	case vertexAIProvider:
		embP, newProviderErr = NewVertexAIEmbeddingProvider(base.outputFields[0], functionSchema, nil)
	case voyageAIProvider:
		embP, newProviderErr = NewVoyageAIEmbeddingProvider(base.outputFields[0], functionSchema)
	case cohereProvider:
		embP, newProviderErr = NewCohereEmbeddingProvider(base.outputFields[0], functionSchema)
	case siliconflowProvider:
		embP, newProviderErr = NewSiliconflowEmbeddingProvider(base.outputFields[0], functionSchema)
	case teiProvider:
		embP, newProviderErr = NewTEIEmbeddingProvider(base.outputFields[0], functionSchema)
	default:
		return nil, fmt.Errorf("Unsupported text embedding service provider: [%s] , list of supported [%s, %s, %s, %s, %s, %s, %s, %s, %s]", provider, openAIProvider, azureOpenAIProvider, aliDashScopeProvider, bedrockProvider, vertexAIProvider, voyageAIProvider, cohereProvider, siliconflowProvider, teiProvider)
	}

	if newProviderErr != nil {
		return nil, newProviderErr
	}
	return &TextEmbeddingFunction{
		FunctionBase: *base,
		embProvider:  embP,
	}, nil
}

func (runner *TextEmbeddingFunction) MaxBatch() int {
	return runner.embProvider.MaxBatch()
}

func (runner *TextEmbeddingFunction) packToFieldData(embds any) ([]*schemapb.FieldData, error) {
	var outputField schemapb.FieldData
	outputField.FieldId = runner.GetOutputFields()[0].FieldID
	outputField.FieldName = runner.GetOutputFields()[0].Name
	outputField.Type = runner.GetOutputFields()[0].DataType
	outputField.IsDynamic = runner.GetOutputFields()[0].IsDynamic
	switch embds := embds.(type) {
	case [][]float32:
		data := make([]float32, 0, len(embds)*int(runner.embProvider.FieldDim()))
		for _, emb := range embds {
			data = append(data, emb...)
		}

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
	case [][]int8:
		data := make([]byte, 0, len(embds)*int(runner.embProvider.FieldDim()))
		for _, emb := range embds {
			for _, v := range emb {
				data = append(data, byte(v))
			}
		}

		outputField.Field = &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Data: &schemapb.VectorField_Int8Vector{
					Int8Vector: data,
				},
				Dim: runner.embProvider.FieldDim(),
			},
		}
	}
	return []*schemapb.FieldData{&outputField}, nil
}

func (runner *TextEmbeddingFunction) ProcessInsert(inputs []*schemapb.FieldData) ([]*schemapb.FieldData, error) {
	if len(inputs) != 1 {
		return nil, fmt.Errorf("Text embedding function only receives one input field, but got [%d]", len(inputs))
	}

	if !isValidInputDataType(inputs[0].Type) {
		return nil, fmt.Errorf("Text embedding only supports varchar or text field as input field, but got %s", schemapb.DataType_name[int32(inputs[0].Type)])
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

	return runner.packToFieldData(embds)
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
	if runner.GetOutputFields()[0].DataType == schemapb.DataType_FloatVector {
		return funcutil.Float32VectorsToPlaceholderGroup(embds.([][]float32)), nil
	} else if runner.GetOutputFields()[0].DataType == schemapb.DataType_Int8Vector {
		return funcutil.Int8VectorsToPlaceholderGroup(embds.([][]int8)), nil
	}
	return nil, fmt.Errorf("Text embedding function doesn't support % vector", schemapb.DataType_name[int32(runner.GetOutputFields()[0].DataType)])
}

func (runner *TextEmbeddingFunction) ProcessBulkInsert(inputs []storage.FieldData) (map[storage.FieldID]storage.FieldData, error) {
	if len(inputs) != 1 {
		return nil, fmt.Errorf("TextEmbedding function only receives one input, bug got [%d]", len(inputs))
	}

	if !isValidInputDataType(inputs[0].GetDataType()) {
		return nil, fmt.Errorf("TextEmbedding function only supports varchar or text field as input field, but got %s", schemapb.DataType_name[int32(inputs[0].GetDataType())])
	}

	texts, ok := inputs[0].GetDataRows().([]string)
	if !ok {
		return nil, fmt.Errorf("Input texts is empty")
	}

	embds, err := runner.embProvider.CallEmbedding(texts, InsertMode)
	if err != nil {
		return nil, err
	}

	switch embds := embds.(type) {
	case [][]float32:
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
	case [][]int8:
		data := make([]int8, 0, len(texts)*int(runner.embProvider.FieldDim()))
		for _, emb := range embds {
			data = append(data, emb...)
		}

		field := &storage.Int8VectorFieldData{
			Data: data,
			Dim:  int(runner.embProvider.FieldDim()),
		}
		return map[storage.FieldID]storage.FieldData{
			runner.outputFields[0].FieldID: field,
		}, nil
	}
	return nil, fmt.Errorf("Unknow embedding type")
}
