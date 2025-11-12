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
package models

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/credentials"
)

type TextEmbeddingMode int

const (
	InsertMode TextEmbeddingMode = iota
	SearchMode
)

type EmbeddingType int

const (
	UnsupportEmbd EmbeddingType = iota
	Float32Embd
	Int8Embd
)

const EnableConf string = "enable"

// common params
const (
	ModelNameParamKey          string = "model_name"
	DimParamKey                string = "dim"
	URLParamKey                string = "url"
	CredentialParamKey         string = "credential"
	TruncateParamKey           string = "truncate"
	MaxClientBatchSizeParamKey string = "max_client_batch_size"
	IntegrationIDKey           string = "integration_id"
)

// ali text embedding
const (
	DashscopeAKEnvStr string = "MILVUS_DASHSCOPE_API_KEY"
)

// openai/azure text embedding

const (
	OpenaiAKEnvStr string = "MILVUS_OPENAI_API_KEY"

	AzureOpenaiAKEnvStr     string = "MILVUS_AZURE_OPENAI_API_KEY"
	AzureOpenaiResourceName string = "MILVUS_AZURE_OPENAI_RESOURCE_NAME"

	UserParamKey string = "user"
)

// bedrock emebdding

const (
	// awsAKIdParamKey   string = "aws_access_key_id"
	// awsSAKParamKey    string = "aws_secret_access_key"
	RegionParamKey    string = "region"
	NormalizeParamKey string = "normalize"

	BedrockAccessKeyId string = "MILVUS_BEDROCK_ACCESS_KEY_ID"
	BedrockSAKEnvStr   string = "MILVUS_BEDROCK_SECRET_ACCESS_KEY"
)

// vertexAI

const (
	LocationParamKey  string = "location"
	ProjectIDParamKey string = "projectid"
	TaskTypeParamKey  string = "task"

	VertexServiceAccountJSONEnv string = "MILVUS_GOOGLE_APPLICATION_CREDENTIALS"
)

// voyageAI
const (
	TruncationParamKey string = "truncation"
	VoyageAIAKEnvStr   string = "MILVUS_VOYAGEAI_API_KEY"
)

// cohere

const (
	MaxTKsPerDocParamKey string = "max_tokens_per_doc"
	CohereAIAKEnvStr     string = "MILVUS_COHERE_API_KEY"
)

// siliconflow

const (
	MaxChunksPerDocParamKey string = "max_chunks_per_doc"
	OverlapTokensParamKey   string = "overlap_tokens"

	SiliconflowAKEnvStr string = "MILVUS_SILICONFLOW_API_KEY"
)

// TEI and vllm

const (
	IngestionPromptParamKey     string = "ingestion_prompt"
	SearchPromptParamKey        string = "search_prompt"
	TruncationDirectionParamKey string = "truncation_direction"
	EndpointParamKey            string = "endpoint"

	VllmTruncateParamName string = "truncate_prompt_tokens"

	TeiTruncateParamName string = "truncate"
)

// zilliz

const (
	ModelDeploymentIDKey string = "model_deployment_id"
	TruncationKey        string = "truncation"
)

type ModelExtraInfo struct {
	ClusterID string
	DBName    string
}

func ParseAKAndURL(credentials *credentials.Credentials, params []*commonpb.KeyValuePair, confParams map[string]string, apiKeyEnv string, extraInfo *ModelExtraInfo) (string, string, error) {
	// function param > yaml > env
	var err error
	var apiKey, url string

	for _, param := range params {
		switch strings.ToLower(param.Key) {
		case CredentialParamKey:
			credentialName := param.Value
			if apiKey, err = credentials.GetAPIKeyCredential(credentialName); err != nil {
				return "", "", err
			}
		case IntegrationIDKey:
			apiKey = param.Value + "|" + extraInfo.ClusterID + "|" + extraInfo.DBName
		}
	}

	// from milvus.yaml
	if apiKey == "" {
		credentialName := confParams[CredentialParamKey]
		if credentialName != "" {
			if apiKey, err = credentials.GetAPIKeyCredential(credentialName); err != nil {
				return "", "", err
			}
		}
	}

	if url == "" {
		url = confParams[URLParamKey]
	}

	// from env, url doesn't support configuration in in env
	if apiKey == "" {
		apiKey = os.Getenv(apiKeyEnv)
	}

	// DEPRECATED: MILVUSAI_* env variables will be removed in Milvus 3.0.
	// Use NEW_ENV_* instead.
	if apiKey == "" {
		newEnvStr := "MILVUSAI_" + strings.TrimPrefix(apiKeyEnv, "MILVUS_")
		apiKey = os.Getenv(newEnvStr)
	}

	return apiKey, url, nil
}

func ParseAndCheckFieldDim(dimStr string, fieldDim int64, fieldName string) (int64, error) {
	dim, err := strconv.ParseInt(dimStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("dimension [%s] provided in Function params is not a valid int", dimStr)
	}

	if dim != 0 && dim != fieldDim {
		return 0, fmt.Errorf("Function output field:[%s]'s dimension [%d] does not match the dimension [%d] provided in Function params.", fieldName, fieldDim, dim)
	}
	return dim, nil
}

func GetEmbdType(dtype schemapb.DataType) EmbeddingType {
	switch dtype {
	case schemapb.DataType_FloatVector:
		return Float32Embd
	case schemapb.DataType_Int8Vector:
		return Int8Embd
	default:
		return UnsupportEmbd
	}
}

type EmbdResult struct {
	FloatEmbds [][]float32
	Int8Embds  [][]int8
	EmbdType   EmbeddingType
}

func NewEmbdResult(size int, eType EmbeddingType) *EmbdResult {
	embR := EmbdResult{}
	embR.EmbdType = eType
	if eType == Float32Embd {
		embR.FloatEmbds = make([][]float32, 0, size)
	} else {
		embR.Int8Embds = make([][]int8, 0, size)
	}
	return &embR
}

func (embR *EmbdResult) Append(newEmbd any) {
	switch newEmbd := newEmbd.(type) {
	case [][]float32:
		embR.FloatEmbds = append(embR.FloatEmbds, newEmbd...)
	case []float32:
		embR.FloatEmbds = append(embR.FloatEmbds, newEmbd)
	case [][]int8:
		embR.Int8Embds = append(embR.Int8Embds, newEmbd...)
	case []int8:
		embR.Int8Embds = append(embR.Int8Embds, newEmbd)
	}
}

func NewBaseURL(endpoint string) (*url.URL, error) {
	base, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return nil, fmt.Errorf("endpoint: [%s] is not a valid http/https link", endpoint)
	}
	if base.Host == "" {
		return nil, fmt.Errorf("endpoint: [%s] is not a valid http/https link", endpoint)
	}
	return base, nil
}

func IsEnable(conf map[string]string) bool {
	// milvus.yaml
	enableConf := conf[EnableConf]
	return strings.ToLower(enableConf) != "false"
}

type Response any

func PostRequest[T Response](req any, url string, headers map[string]string, timeoutSec int64) (*T, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	if timeoutSec <= 0 {
		timeoutSec = 30
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
	defer cancel()

	body, err := retrySend(ctx, data, http.MethodPost, url, headers, 3)
	if err != nil {
		return nil, err
	}
	var res T
	err = json.Unmarshal(body, &res)
	if err != nil {
		return nil, fmt.Errorf("Call service failed, unmarshal response failed, errs:[%v]", err)
	}
	return &res, err
}

func send(req *http.Request) ([]byte, error) {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Call service failed, errs:[%v]", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Call service failed, read response failed, errs:[%v]", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Call service failed, errs:[%s, %s]", resp.Status, body)
	}
	return body, nil
}

func retrySend(ctx context.Context, data []byte, httpMethod string, url string, headers map[string]string, maxRetries int) ([]byte, error) {
	var err error
	var body []byte
	for i := 0; i < maxRetries; i++ {
		// Check if context is canceled/timed out before each retry
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		req, reqErr := http.NewRequestWithContext(ctx, httpMethod, url, bytes.NewBuffer(data))
		if reqErr != nil {
			return nil, reqErr
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		body, err = send(req)
		if err == nil {
			return body, nil
		}

		// Don't sleep after the last retry attempt
		if i == maxRetries-1 {
			break
		}

		backoffDelay := 1 << uint(i) * time.Second
		jitter := time.Duration(rand.Int63n(int64(backoffDelay / 4)))

		// Use context-aware sleep to respect timeout
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoffDelay + jitter):
			// Continue to next retry
		}
	}
	return nil, err
}
