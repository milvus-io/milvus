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
	"os"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/models"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	TextEmbeddingAda002 string = "text-embedding-ada-002"
	TextEmbedding3Small string = "text-embedding-3-small"
	TextEmbedding3Large string = "text-embedding-3-large"
)

const (
	maxBatch   = 128
	timeoutSec = 30
)

const (
	ModelNameParamKey          string = "model_name"
	DimParamKey                string = "dim"
	UserParamKey               string = "user"
	OpenaiEmbeddingUrlParamKey string = "embedding_url"
	OpenaiApiKeyParamKey       string = "api_key"
)

type OpenAIEmbeddingFunction struct {
	FunctionBase
	fieldDim int64

	client        *models.OpenAIEmbeddingClient
	modelName     string
	embedDimParam int64
	user          string
}

func createOpenAIEmbeddingClient(apiKey string, url string) (*models.OpenAIEmbeddingClient, error) {
	if apiKey == "" {
		apiKey = os.Getenv("OPENAI_API_KEY")
	}
	if apiKey == "" {
		return nil, fmt.Errorf("The apiKey configuration was not found in the environment variables")
	}

	if url == "" {
		url = os.Getenv("OPENAI_EMBEDDING_URL")
	}
	if url == "" {
		url = "https://api.openai.com/v1/embeddings"
	}
	c := models.NewOpenAIEmbeddingClient(apiKey, url)
	return &c, nil
}

func NewOpenAIEmbeddingFunction(coll *schemapb.CollectionSchema, schema *schemapb.FunctionSchema) (*OpenAIEmbeddingFunction, error) {
	if len(schema.GetOutputFieldIds()) != 1 {
		return nil, fmt.Errorf("OpenAIEmbedding function should only have one output field, but now is %d", len(schema.GetOutputFieldIds()))
	}

	base, err := NewBase(coll, schema)
	if err != nil {
		return nil, err
	}

	if base.outputFields[0].DataType != schemapb.DataType_FloatVector {
		return nil, fmt.Errorf("Output field not match, openai embedding needs [%s], got [%s]",
			schemapb.DataType_name[int32(schemapb.DataType_FloatVector)],
			schemapb.DataType_name[int32(base.outputFields[0].DataType)])
	}

	fieldDim, err := typeutil.GetDim(base.outputFields[0])
	if err != nil {
		return nil, err
	}
	var apiKey, url, modelName, user string
	var dim int64

	for _, param := range schema.Params {
		switch strings.ToLower(param.Key) {
		case ModelNameParamKey:
			modelName = param.Value
		case DimParamKey:
			dim, err = strconv.ParseInt(param.Value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("dim [%s] is not int", param.Value)
			}

			if dim != 0 && dim != fieldDim {
				return nil, fmt.Errorf("Field %s's dim is [%d], but embeding's dim is [%d]", schema.Name, fieldDim, dim)
			}
		case UserParamKey:
			user = param.Value
		case OpenaiApiKeyParamKey:
			apiKey = param.Value
		case OpenaiEmbeddingUrlParamKey:
			url = param.Value
		default:
		}
	}

	c, err := createOpenAIEmbeddingClient(apiKey, url)
	if err != nil {
		return nil, err
	}

	runner := OpenAIEmbeddingFunction{
		FunctionBase:  *base,
		client:        c,
		fieldDim:      fieldDim,
		modelName:     modelName,
		user:          user,
		embedDimParam: dim,
	}

	if runner.modelName != TextEmbeddingAda002 && runner.modelName != TextEmbedding3Small && runner.modelName != TextEmbedding3Large {
		return nil, fmt.Errorf("Unsupported model: %s, only support [%s, %s, %s]",
			runner.modelName, TextEmbeddingAda002, TextEmbedding3Small, TextEmbedding3Large)
	}
	return &runner, nil
}

func (runner *OpenAIEmbeddingFunction) MaxBatch() int {
	return 5 * maxBatch
}

func (runner *OpenAIEmbeddingFunction) callEmbedding(texts []string) ([][]float32, error) {
	numRows := len(texts)
	if numRows > runner.MaxBatch() {
		return nil, fmt.Errorf("OpenAI embedding supports up to [%d] pieces of data at a time, got [%d]", runner.MaxBatch(), numRows)
	}

	data := make([][]float32, 0, numRows)
	for i := 0; i < numRows; i += maxBatch {
		end := i + maxBatch
		if end > numRows {
			end = numRows
		}
		resp, err := runner.client.Embedding(runner.modelName, texts[i:end], int(runner.embedDimParam), runner.user, timeoutSec)
		if err != nil {
			return nil, err
		}
		if end-i != len(resp.Data) {
			return nil, fmt.Errorf("The texts number is [%d], but got embedding number [%d]", end-i, len(resp.Data))
		}
		for _, item := range resp.Data {
			if len(item.Embedding) != int(runner.fieldDim) {
				return nil, fmt.Errorf("The required embedding dim for field [%s] is [%d], but the embedding obtained from the model is [%d]",
					runner.outputFields[0].Name, runner.fieldDim, len(item.Embedding))
			}
			data = append(data, item.Embedding)
		}
	}
	return data, nil
}

func (runner *OpenAIEmbeddingFunction) ProcessInsert(inputs []*schemapb.FieldData) ([]*schemapb.FieldData, error) {
	if len(inputs) != 1 {
		return nil, fmt.Errorf("OpenAIEmbedding function only receives one input, bug got [%d]", len(inputs))
	}

	if inputs[0].Type != schemapb.DataType_VarChar {
		return nil, fmt.Errorf("OpenAIEmbedding only supports varchar field, the input is not varchar")
	}

	texts := inputs[0].GetScalars().GetStringData().GetData()
	if texts == nil {
		return nil, fmt.Errorf("Input texts is empty")
	}

	embds, err := runner.callEmbedding(texts)
	if err != nil {
		return nil, err
	}
	data := make([]float32, 0, len(texts)*int(runner.fieldDim))
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
			Dim: runner.fieldDim,
		},
	}
	return []*schemapb.FieldData{&outputField}, nil
}

func (runner *OpenAIEmbeddingFunction) ProcessSearch(placeholderGroup *commonpb.PlaceholderGroup) (*commonpb.PlaceholderGroup, error) {
	texts := funcutil.GetVarCharFromPlaceholder(placeholderGroup.Placeholders[0]) // Already checked externally
	embds, err := runner.callEmbedding(texts)
	if err != nil {
		return nil, err
	}
	return funcutil.Float32VectorsToPlaceholderGroup(embds), nil
}
