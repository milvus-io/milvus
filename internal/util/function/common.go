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
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type TextEmbeddingMode int

const (
	InsertMode TextEmbeddingMode = iota
	SearchMode
)

type embeddingType int

const (
	unsupportEmbd embeddingType = iota
	float32Embd
	int8Embd
)

// common params
const (
	modelNameParamKey    string = "model_name"
	dimParamKey          string = "dim"
	embeddingURLParamKey string = "url"
	apiKeyParamKey       string = "api_key"
	truncateParamKey     string = "truncate"
)

// ali text embedding
const (
	TextEmbeddingV1 string = "text-embedding-v1"
	TextEmbeddingV2 string = "text-embedding-v2"
	TextEmbeddingV3 string = "text-embedding-v3"

	dashscopeAKEnvStr string = "MILVUSAI_DASHSCOPE_API_KEY"
)

// openai/azure text embedding

const (
	TextEmbeddingAda002 string = "text-embedding-ada-002"
	TextEmbedding3Small string = "text-embedding-3-small"
	TextEmbedding3Large string = "text-embedding-3-large"

	openaiAKEnvStr string = "MILVUSAI_OPENAI_API_KEY"

	azureOpenaiAKEnvStr     string = "MILVUSAI_AZURE_OPENAI_API_KEY"
	azureOpenaiResourceName string = "MILVUSAI_AZURE_OPENAI_RESOURCE_NAME"

	userParamKey string = "user"
)

// bedrock emebdding

const (
	BedRockTitanTextEmbeddingsV2 string = "amazon.titan-embed-text-v2:0"
	awsAKIdParamKey              string = "aws_access_key_id"
	awsSAKParamKey               string = "aws_secret_access_key"
	regionParamKey               string = "regin"
	normalizeParamKey            string = "normalize"

	bedrockAccessKeyId string = "MILVUSAI_BEDROCK_ACCESS_KEY_ID"
	bedrockSAKEnvStr   string = "MILVUSAI_BEDROCK_SECRET_ACCESS_KEY"
)

// vertexAI

const (
	locationParamKey  string = "location"
	projectIDParamKey string = "projectid"
	taskTypeParamKey  string = "task"

	textEmbedding005             string = "text-embedding-005"
	textMultilingualEmbedding002 string = "text-multilingual-embedding-002"

	vertexServiceAccountJSONEnv string = "MILVUSAI_GOOGLE_APPLICATION_CREDENTIALS"
)

// voyageAI
const (
	voyage3Large   string = "voyage-3-large"
	voyage3        string = "voyage-3"
	voyage3Lite    string = "voyage-3-lite"
	voyageCode3    string = "voyage-code-3"
	voyageFinance2 string = "voyage-finance-2"
	voyageLaw2     string = "voyage-law-2"
	voyageCode2    string = "voyage-code-2"

	voyageAIAKEnvStr string = "MILVUSAI_VOYAGEAI_API_KEY"
)

// cohere

const (
	embedEnglishV30           string = "embed-english-v3.0"
	embedMultilingualV30      string = "embed-multilingual-v3.0"
	embedEnglishLightV30      string = "embed-english-light-v3.0"
	embedMultilingualLightV30 string = "embed-multilingual-light-v3.0"
	embedEnglishV20           string = "embed-english-v2.0"
	embedEnglishLightV20      string = "embed-english-light-v2.0"
	embedMultilingualV20      string = "embed-multilingual-v2.0"

	cohereAIAKEnvStr string = "MILVUSAI_COHERE_API_KEY"
)

// siliconflow

const (
	bAAIBgeLargeZhV15               string = "BAAI/bge-large-zh-v1.5"
	bAAIBgeLargeEhV15               string = "BAAI/bge-large-eh-v1.5"
	neteaseYoudaoBceEmbeddingBasev1 string = "netease-youdao/bce-embedding-base_v1"
	bAAIBgeM3                       string = "BAAI/bge-m3"
	proBAAIBgeM3                    string = "Pro/BAAI/bge-m3 "

	siliconflowAKEnvStr string = "MILVUSAI_SILICONFLOW_API_KEY"
)

// TEI

const (
	ingestionPromptParamKey     string = "ingestion_prompt"
	searchPromptParamKey        string = "search_prompt"
	maxClientBatchSizeParamKey  string = "max_client_batch_size"
	truncationDirectionParamKey string = "truncation_direction"
	endpointParamKey            string = "endpoint"

	enableTeiEnvStr string = "MILVUSAI_ENABLE_TEI"
)

func parseAndCheckFieldDim(dimStr string, fieldDim int64, fieldName string) (int64, error) {
	dim, err := strconv.ParseInt(dimStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("dimension [%s] provided in Function params is not a valid int", dimStr)
	}

	if dim != 0 && dim != fieldDim {
		return 0, fmt.Errorf("Function output field:[%s]'s dimension [%d] does not match the dimension [%d] provided in Function params.", fieldName, fieldDim, dim)
	}
	return dim, nil
}

func getEmbdType(dtype schemapb.DataType) embeddingType {
	switch dtype {
	case schemapb.DataType_FloatVector:
		return float32Embd
	case schemapb.DataType_Int8Vector:
		return int8Embd
	default:
		return unsupportEmbd
	}
}

type EmbdResult struct {
	floatEmbds [][]float32
	int8Embds  [][]int8
	eType      embeddingType
}

func newEmbdResult(size int, eType embeddingType) *EmbdResult {
	embR := EmbdResult{}
	embR.eType = eType
	if eType == float32Embd {
		embR.floatEmbds = make([][]float32, 0, size)
	} else {
		embR.int8Embds = make([][]int8, 0, size)
	}
	return &embR
}

func (embR *EmbdResult) append(newEmbd any) {
	switch newEmbd := newEmbd.(type) {
	case [][]float32:
		embR.floatEmbds = append(embR.floatEmbds, newEmbd...)
	case []float32:
		embR.floatEmbds = append(embR.floatEmbds, newEmbd)
	case [][]int8:
		embR.int8Embds = append(embR.int8Embds, newEmbd...)
	case []int8:
		embR.int8Embds = append(embR.int8Embds, newEmbd)
	}
}
