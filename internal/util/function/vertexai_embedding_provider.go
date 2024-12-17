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
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/models/vertexai"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type vertexAIJsonKey struct {
	jsonKey []byte
	once    sync.Once
	initErr error
}

var vtxKey vertexAIJsonKey

func getVertexAIJsonKey() ([]byte, error) {
	vtxKey.once.Do(func() {
		jsonKeyPath := os.Getenv(vertexServiceAccountJSONEnv)
		jsonKey, err := os.ReadFile(jsonKeyPath)
		if err != nil {
			vtxKey.initErr = fmt.Errorf("Read service account json file failed, %v", err)
			return
		}
		vtxKey.jsonKey = jsonKey
	})
	return vtxKey.jsonKey, vtxKey.initErr
}

const (
	vertexAIDocRetrival  string = "DOC_RETRIEVAL"
	vertexAICodeRetrival string = "CODE_RETRIEVAL"
	vertexAISTS          string = "STS"
)

func checkTask(modelName string, task string) error {
	if task != vertexAIDocRetrival && task != vertexAICodeRetrival && task != vertexAISTS {
		return fmt.Errorf("Unsupport task %s, the supported list: [%s, %s, %s]", task, vertexAIDocRetrival, vertexAICodeRetrival, vertexAISTS)
	}
	if modelName == textMultilingualEmbedding002 && task == vertexAICodeRetrival {
		return fmt.Errorf("Model %s doesn't support %s task", textMultilingualEmbedding002, vertexAICodeRetrival)
	}
	return nil
}

type VertextAIEmbeddingProvider struct {
	fieldDim int64

	client        *vertexai.VertexAIEmbedding
	modelName     string
	embedDimParam int64
	task          string

	maxBatch   int
	timeoutSec int
}

func createVertextAIEmbeddingClient(url string) (*vertexai.VertexAIEmbedding, error) {
	jsonKey, err := getVertexAIJsonKey()
	if err != nil {
		return nil, err
	}
	c := vertexai.NewVertexAIEmbedding(url, jsonKey, "https://www.googleapis.com/auth/cloud-platform", "")
	return c, nil
}

func NewVertextAIEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema, c *vertexai.VertexAIEmbedding) (*VertextAIEmbeddingProvider, error) {
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
			dim, err = strconv.ParseInt(param.Value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("dim [%s] is not int", param.Value)
			}

			if dim != 0 && dim != fieldDim {
				return nil, fmt.Errorf("Field %s's dim is [%d], but embeding's dim is [%d]", functionSchema.Name, fieldDim, dim)
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
	if err := checkTask(modelName, task); err != nil {
		return nil, err
	}

	if location == "" {
		location = "us-central1"
	}

	if modelName != textEmbedding005 && modelName != textMultilingualEmbedding002 {
		return nil, fmt.Errorf("Unsupported model: %s, only support [%s, %s]",
			modelName, textEmbedding005, textMultilingualEmbedding002)
	}
	url := fmt.Sprintf("https://%s-aiplatform.googleapis.com/v1/projects/%s/locations/%s/publishers/google/models/%s:predict", location, projectID, location, modelName)
	var client *vertexai.VertexAIEmbedding
	if c == nil {
		client, err = createVertextAIEmbeddingClient(url)
		if err != nil {
			return nil, err
		}
	} else {
		client = c
	}

	provider := VertextAIEmbeddingProvider{
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

func (provider *VertextAIEmbeddingProvider) MaxBatch() int {
	return 5 * provider.maxBatch
}

func (provider *VertextAIEmbeddingProvider) FieldDim() int64 {
	return provider.fieldDim
}

func (provider *VertextAIEmbeddingProvider) getTaskType(mode string) string {
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
		case vertexAICodeRetrival:
			return "RETRIEVAL_DOCUMENT"
		case vertexAISTS:
			return "SEMANTIC_SIMILARITY"
		}
	}
	return ""
}

func (provider *VertextAIEmbeddingProvider) CallEmbedding(texts []string, batchLimit bool, mode string) ([][]float32, error) {
	numRows := len(texts)
	if batchLimit && numRows > provider.MaxBatch() {
		return nil, fmt.Errorf("VertextAI text embedding supports up to [%d] pieces of data at a time, got [%d]", provider.MaxBatch(), numRows)
	}

	taskType := provider.getTaskType(mode)
	data := make([][]float32, 0, numRows)
	for i := 0; i < numRows; i += provider.maxBatch {
		end := i + provider.maxBatch
		if end > numRows {
			end = numRows
		}
		resp, err := provider.client.Embedding(provider.modelName, texts[i:end], provider.embedDimParam, taskType, time.Duration(provider.timeoutSec))
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
