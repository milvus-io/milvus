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
	"context"
	"fmt"
	"reflect"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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

func hasEmptyString(texts []string) bool {
	for _, text := range texts {
		if text == "" {
			return true
		}
	}
	return false
}

func TextEmbeddingOutputsCheck(fields []*schemapb.FieldSchema) error {
	if len(fields) != 1 || (fields[0].DataType != schemapb.DataType_FloatVector && fields[0].DataType != schemapb.DataType_Int8Vector) {
		return fmt.Errorf("TextEmbedding function output field must be a FloatVector or Int8Vector field")
	}
	return nil
}

// Text embedding for retrieval task
type textEmbeddingProvider interface {
	MaxBatch() int
	CallEmbedding(texts []string, mode TextEmbeddingMode) (any, error)
	FieldDim() int64
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

	if err := TextEmbeddingOutputsCheck(base.outputFields); err != nil {
		return nil, err
	}

	var embP textEmbeddingProvider
	var newProviderErr error
	conf := paramtable.Get().FunctionCfg.GetTextEmbeddingProviderConfig(base.provider)
	switch base.provider {
	case openAIProvider:
		embP, newProviderErr = NewOpenAIEmbeddingProvider(base.outputFields[0], functionSchema, conf)
	case azureOpenAIProvider:
		embP, newProviderErr = NewAzureOpenAIEmbeddingProvider(base.outputFields[0], functionSchema, conf)
	case bedrockProvider:
		embP, newProviderErr = NewBedrockEmbeddingProvider(base.outputFields[0], functionSchema, nil, conf)
	case aliDashScopeProvider:
		embP, newProviderErr = NewAliDashScopeEmbeddingProvider(base.outputFields[0], functionSchema, conf)
	case vertexAIProvider:
		embP, newProviderErr = NewVertexAIEmbeddingProvider(base.outputFields[0], functionSchema, nil, conf)
	case voyageAIProvider:
		embP, newProviderErr = NewVoyageAIEmbeddingProvider(base.outputFields[0], functionSchema, conf)
	case cohereProvider:
		embP, newProviderErr = NewCohereEmbeddingProvider(base.outputFields[0], functionSchema, conf)
	case siliconflowProvider:
		embP, newProviderErr = NewSiliconflowEmbeddingProvider(base.outputFields[0], functionSchema, conf)
	case teiProvider:
		embP, newProviderErr = NewTEIEmbeddingProvider(base.outputFields[0], functionSchema, conf)
	default:
		return nil, fmt.Errorf("Unsupported text embedding service provider: [%s] , list of supported [%s, %s, %s, %s, %s, %s, %s, %s, %s]", base.provider, openAIProvider, azureOpenAIProvider, aliDashScopeProvider, bedrockProvider, vertexAIProvider, voyageAIProvider, cohereProvider, siliconflowProvider, teiProvider)
	}

	if newProviderErr != nil {
		return nil, newProviderErr
	}
	return &TextEmbeddingFunction{
		FunctionBase: *base,
		embProvider:  embP,
	}, nil
}

func (runner *TextEmbeddingFunction) Check() error {
	embds, err := runner.embProvider.CallEmbedding([]string{"check"}, InsertMode)
	if err != nil {
		return err
	}
	dim := 0
	switch embds := embds.(type) {
	case [][]float32:
		dim = len(embds[0])
	case [][]int8:
		dim = len(embds[0])
	default:
		return fmt.Errorf("Unsupport embedding type: %s", reflect.TypeOf(embds).String())
	}
	if dim != int(runner.embProvider.FieldDim()) {
		return fmt.Errorf("The dim set in the schema is inconsistent with the dim of the model, dim in schema is %d, dim of model is %d", runner.embProvider.FieldDim(), dim)
	}
	return nil
}

func (runner *TextEmbeddingFunction) MaxBatch() int {
	return runner.embProvider.MaxBatch()
}

func (runner *TextEmbeddingFunction) GetCollectionName() string {
	return runner.collectionName
}

func (runner *TextEmbeddingFunction) GetFunctionProvider() string {
	return runner.provider
}

func (runner *TextEmbeddingFunction) GetFunctionTypeName() string {
	return runner.functionTypeName
}

func (runner *TextEmbeddingFunction) GetFunctionName() string {
	return runner.functionName
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

func (runner *TextEmbeddingFunction) ProcessInsert(ctx context.Context, inputs []*schemapb.FieldData) ([]*schemapb.FieldData, error) {
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

	// make sure all texts are not empty
	if hasEmptyString(texts) {
		return nil, fmt.Errorf("There is an empty string in the input data, TextEmbedding function does not support empty text")
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

func (runner *TextEmbeddingFunction) ProcessSearch(ctx context.Context, placeholderGroup *commonpb.PlaceholderGroup) (*commonpb.PlaceholderGroup, error) {
	texts := funcutil.GetVarCharFromPlaceholder(placeholderGroup.Placeholders[0]) // Already checked externally
	numRows := len(texts)
	if numRows > runner.MaxBatch() {
		return nil, fmt.Errorf("Embedding supports up to [%d] pieces of data at a time, got [%d]", runner.MaxBatch(), numRows)
	}
	// make sure all texts are not empty
	if hasEmptyString(texts) {
		return nil, fmt.Errorf("There is an empty string in the queries, TextEmbedding function does not support empty text")
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

	// make sure all texts are not empty
	// In storage.FieldData, null is also stored as an empty string
	if hasEmptyString(texts) {
		return nil, fmt.Errorf("There is an empty string in the input data, TextEmbedding function does not support empty text")
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
