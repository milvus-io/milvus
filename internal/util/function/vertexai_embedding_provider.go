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
	"strings"
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models/vertexai"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type vertexAIJsonKey struct {
	mu       sync.Mutex
	filePath string
	jsonKey  []byte
}

var vtxKey vertexAIJsonKey

func getVertexAIJsonKey() ([]byte, error) {
	vtxKey.mu.Lock()
	defer vtxKey.mu.Unlock()

	jsonKeyPath := os.Getenv(vertexServiceAccountJSONEnv)
	if jsonKeyPath == "" {
		return nil, errors.New("VetexAI credentials file path is empty")
	}
	if vtxKey.filePath == jsonKeyPath {
		return vtxKey.jsonKey, nil
	}

	jsonKey, err := os.ReadFile(jsonKeyPath)
	if err != nil {
		return nil, fmt.Errorf("Vertexai: read credentials file failed, %v", err)
	}

	vtxKey.jsonKey = jsonKey
	vtxKey.filePath = jsonKeyPath

	return vtxKey.jsonKey, nil
}

const (
	vertexAIDocRetrival  string = "DOC_RETRIEVAL"
	vertexAICodeRetrival string = "CODE_RETRIEVAL"
	vertexAISTS          string = "STS"
)

type VertexAIEmbeddingProvider struct {
	fieldDim int64

	client        *vertexai.VertexAIEmbedding
	modelName     string
	embedDimParam int64
	task          string

	maxBatch   int
	timeoutSec int64
}

func createVertexAIEmbeddingClient(url string, credentialsJSON []byte) (*vertexai.VertexAIEmbedding, error) {
	c := vertexai.NewVertexAIEmbedding(url, credentialsJSON, "https://www.googleapis.com/auth/cloud-platform", "")
	return c, nil
}

func parseGcpCredentialInfo(credentials *credentials.CredentialsManager, params []*commonpb.KeyValuePair, confParams map[string]string) ([]byte, error) {
	// function param > yaml > env
	var credentialsJSON []byte
	var err error

	for _, param := range params {
		switch strings.ToLower(param.Key) {
		case credentialParamKey:
			credentialName := param.Value
			if credentialsJSON, err = credentials.GetGcpCredential(credentialName); err != nil {
				return nil, err
			}
		}
	}

	// from milvus.yaml
	if credentialsJSON == nil {
		credentialName := confParams[credentialParamKey]
		if credentialName != "" {
			if credentialsJSON, err = credentials.GetGcpCredential(credentialName); err != nil {
				return nil, err
			}
		}
	}

	// from env
	if credentialsJSON == nil {
		credentialsJSON, err = getVertexAIJsonKey()
		if err != nil {
			return nil, err
		}
	}
	return credentialsJSON, nil
}

func NewVertexAIEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema, c *vertexai.VertexAIEmbedding, params map[string]string, credentials *credentials.CredentialsManager) (*VertexAIEmbeddingProvider, error) {
	fieldDim, err := typeutil.GetDim(fieldSchema)
	if err != nil {
		return nil, err
	}
	var location, projectID, task, modelName string
	var dim int64

	for _, param := range functionSchema.Params {
		switch strings.ToLower(param.Key) {
		case modelNameParamKey:
			modelName = param.Value
		case dimParamKey:
			dim, err = parseAndCheckFieldDim(param.Value, fieldDim, fieldSchema.Name)
			if err != nil {
				return nil, err
			}
		case locationParamKey:
			location = param.Value
		case projectIDParamKey:
			projectID = param.Value
		case taskTypeParamKey:
			task = param.Value
		default:
		}
	}

	if task == "" {
		task = vertexAIDocRetrival
	}

	if location == "" {
		location = "us-central1"
	}

	url := params["url"]
	if url == "" {
		url = fmt.Sprintf("https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:predict", location, projectID, location, modelName)
	}
	var client *vertexai.VertexAIEmbedding
	if c == nil {
		jsonKey, err := parseGcpCredentialInfo(credentials, functionSchema.Params, params)
		if err != nil {
			return nil, err
		}
		client, err = createVertexAIEmbeddingClient(url, jsonKey)
		if err != nil {
			return nil, err
		}
	} else {
		client = c
	}

	provider := VertexAIEmbeddingProvider{
		fieldDim:      fieldDim,
		client:        client,
		modelName:     modelName,
		embedDimParam: dim,
		task:          task,
		maxBatch:      128,
		timeoutSec:    30,
	}
	return &provider, nil
}

func (provider *VertexAIEmbeddingProvider) MaxBatch() int {
	return 5 * provider.maxBatch
}

func (provider *VertexAIEmbeddingProvider) FieldDim() int64 {
	return provider.fieldDim
}

func (provider *VertexAIEmbeddingProvider) getTaskType(mode TextEmbeddingMode) string {
	if mode == SearchMode {
		switch provider.task {
		case vertexAIDocRetrival:
			return "RETRIEVAL_QUERY"
		case vertexAICodeRetrival:
			return "CODE_RETRIEVAL_QUERY"
		case vertexAISTS:
			return "SEMANTIC_SIMILARITY"
		}
	} else {
		switch provider.task {
		case vertexAIDocRetrival:
			return "RETRIEVAL_DOCUMENT"
		case vertexAICodeRetrival: // When inserting, the model does not distinguish between doc and code
			return "RETRIEVAL_DOCUMENT"
		case vertexAISTS:
			return "SEMANTIC_SIMILARITY"
		}
	}
	return ""
}

func (provider *VertexAIEmbeddingProvider) CallEmbedding(texts []string, mode TextEmbeddingMode) (any, error) {
	numRows := len(texts)
	taskType := provider.getTaskType(mode)
	data := make([][]float32, 0, numRows)
	for i := 0; i < numRows; i += provider.maxBatch {
		end := i + provider.maxBatch
		if end > numRows {
			end = numRows
		}
		resp, err := provider.client.Embedding(provider.modelName, texts[i:end], provider.embedDimParam, taskType, provider.timeoutSec)
		if err != nil {
			return nil, err
		}
		if end-i != len(resp.Predictions) {
			return nil, fmt.Errorf("Get embedding failed. The number of texts and embeddings does not match text:[%d], embedding:[%d]", end-i, len(resp.Predictions))
		}
		for _, item := range resp.Predictions {
			if len(item.Embeddings.Values) != int(provider.fieldDim) {
				return nil, fmt.Errorf("The required embedding dim is [%d], but the embedding obtained from the model is [%d]",
					provider.fieldDim, len(item.Embeddings.Values))
			}
			data = append(data, item.Embeddings.Values)
		}
	}
	return data, nil
}
