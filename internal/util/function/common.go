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
	"github.com/milvus-io/milvus/internal/util/credentials"
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
	credentialParamKey   string = "credential"
	truncateParamKey     string = "truncate"
)

// ali text embedding
const (
	dashscopeAKEnvStr string = "MILVUSAI_DASHSCOPE_API_KEY"
)

// openai/azure text embedding

const (
	openaiAKEnvStr string = "MILVUSAI_OPENAI_API_KEY"

	azureOpenaiAKEnvStr     string = "MILVUSAI_AZURE_OPENAI_API_KEY"
	azureOpenaiResourceName string = "MILVUSAI_AZURE_OPENAI_RESOURCE_NAME"

	userParamKey string = "user"
)

// bedrock emebdding

const (
	// awsAKIdParamKey   string = "aws_access_key_id"
	// awsSAKParamKey    string = "aws_secret_access_key"
	regionParamKey    string = "region"
	normalizeParamKey string = "normalize"

	bedrockAccessKeyId string = "MILVUSAI_BEDROCK_ACCESS_KEY_ID"
	bedrockSAKEnvStr   string = "MILVUSAI_BEDROCK_SECRET_ACCESS_KEY"
)

// vertexAI

const (
	locationParamKey  string = "location"
	projectIDParamKey string = "projectid"
	taskTypeParamKey  string = "task"

	vertexServiceAccountJSONEnv string = "MILVUSAI_GOOGLE_APPLICATION_CREDENTIALS"
)

// voyageAI
const (
	truncationParamKey string = "truncation"
	voyageAIAKEnvStr   string = "MILVUSAI_VOYAGEAI_API_KEY"
)

// cohere

const (
	cohereAIAKEnvStr string = "MILVUSAI_COHERE_API_KEY"
)

// siliconflow

const (
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

func parseAKAndURL(credentials *credentials.Credentials, params []*commonpb.KeyValuePair, confParams map[string]string, apiKeyEnv string) (string, string, error) {
	// function param > yaml > env
	var err error
	var apiKey, url string

	for _, param := range params {
		switch strings.ToLower(param.Key) {
		case credentialParamKey:
			credentialName := param.Value
			if apiKey, err = credentials.GetAPIKeyCredential(credentialName); err != nil {
				return "", "", err
			}
		}
	}

	// from milvus.yaml
	if apiKey == "" {
		credentialName := confParams[credentialParamKey]
		if credentialName != "" {
			if apiKey, err = credentials.GetAPIKeyCredential(credentialName); err != nil {
				return "", "", err
			}
		}
	}

	if url == "" {
		url = confParams[embeddingURLParamKey]
	}

	// from env, url doesn't support configuration in in env
	if apiKey == "" {
		apiKey = os.Getenv(apiKeyEnv)
	}
	return apiKey, url, nil
}

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
