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
)

type TextEmbeddingMode int

const (
	InsertMode TextEmbeddingMode = iota
	SearchMode
)

// common params
const (
	modelNameParamKey    string = "model_name"
	dimParamKey          string = "dim"
	embeddingURLParamKey string = "url"
	apiKeyParamKey       string = "api_key"
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
