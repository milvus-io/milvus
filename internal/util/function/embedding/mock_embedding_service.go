//go:build test
// +build test

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

package embedding

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"

	"github.com/aws/aws-sdk-go-v2/service/bedrockruntime"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/models/ali"
	"github.com/milvus-io/milvus/internal/util/function/models/cohere"
	"github.com/milvus-io/milvus/internal/util/function/models/openai"
	"github.com/milvus-io/milvus/internal/util/function/models/siliconflow"
	"github.com/milvus-io/milvus/internal/util/function/models/tei"
	"github.com/milvus-io/milvus/internal/util/function/models/vertexai"
	"github.com/milvus-io/milvus/internal/util/function/models/voyageai"
	"github.com/milvus-io/milvus/pkg/v2/util/testutils"
)

const TestModel string = "TestModel"

func mockEmbedding[T int8 | float32](texts []string, dim int) [][]T {
	embeddings := make([][]T, 0)
	for i := 0; i < len(texts); i++ {
		emb := make([]T, 0)
		for j := 0; j < dim; j++ {
			emb = append(emb, T(i+j))
		}
		embeddings = append(embeddings, emb)
	}
	return embeddings
}

func CreateErrorEmbeddingServer() *httptest.Server {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	return ts
}

func CreateOpenAIEmbeddingServer() *httptest.Server {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req openai.EmbeddingRequest
		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()
		json.Unmarshal(body, &req)
		embs := mockEmbedding[float32](req.Input, req.Dimensions)
		var res openai.EmbeddingResponse
		res.Object = "list"
		res.Model = "text-embedding-3-small"
		for i := 0; i < len(req.Input); i++ {
			res.Data = append(res.Data, openai.EmbeddingData{
				Object:    "embedding",
				Embedding: embs[i],
				Index:     i,
			})
		}

		res.Usage = openai.Usage{
			PromptTokens: 1,
			TotalTokens:  100,
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))
	return ts
}

func CreateAliEmbeddingServer() *httptest.Server {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req ali.EmbeddingRequest
		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()
		json.Unmarshal(body, &req)
		embs := mockEmbedding[float32](req.Input.Texts, req.Parameters.Dimension)
		var res ali.EmbeddingResponse
		for i := 0; i < len(req.Input.Texts); i++ {
			res.Output.Embeddings = append(res.Output.Embeddings, ali.Embeddings{
				Embedding: embs[i],
				TextIndex: i,
			})
		}

		res.Usage = ali.Usage{
			TotalTokens: 100,
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))
	return ts
}

func CreateVoyageAIEmbeddingServer[T int8 | float32]() *httptest.Server {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req voyageai.EmbeddingRequest
		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()
		json.Unmarshal(body, &req)
		embs := mockEmbedding[T](req.Input, int(req.OutputDimension))
		var res voyageai.EmbeddingResponse[T]
		for i := 0; i < len(req.Input); i++ {
			res.Data = append(res.Data, voyageai.EmbeddingData[T]{
				Object:    "list",
				Embedding: embs[i],
				Index:     i,
			})
		}

		res.Usage = voyageai.Usage{
			TotalTokens: 100,
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))
	return ts
}

func CreateSiliconflowEmbeddingServer(dim int) *httptest.Server {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req siliconflow.EmbeddingRequest
		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()
		json.Unmarshal(body, &req)
		embs := mockEmbedding[float32](req.Input, dim)
		var res siliconflow.EmbeddingResponse
		for i := 0; i < len(req.Input); i++ {
			res.Data = append(res.Data, siliconflow.EmbeddingData{
				Object:    "list",
				Embedding: embs[i],
				Index:     i,
			})
		}

		res.Usage = siliconflow.Usage{
			TotalTokens: 100,
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))
	return ts
}

func CreateVertexAIEmbeddingServer() *httptest.Server {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req vertexai.EmbeddingRequest
		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()
		json.Unmarshal(body, &req)
		var texts []string
		for _, item := range req.Instances {
			texts = append(texts, item.Content)
		}
		embs := mockEmbedding[float32](texts, int(req.Parameters.OutputDimensionality))
		var res vertexai.EmbeddingResponse
		for i := 0; i < len(req.Instances); i++ {
			res.Predictions = append(res.Predictions, vertexai.Prediction{
				Embeddings: vertexai.Embeddings{
					Statistics: vertexai.Statistics{
						Truncated:  false,
						TokenCount: 10,
					},
					Values: embs[i],
				},
			})
		}

		res.Metadata = vertexai.Metadata{
			BillableCharacterCount: 100,
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))
	return ts
}

func CreateCohereEmbeddingServer[T int8 | float32]() *httptest.Server {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req cohere.EmbeddingRequest
		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()
		json.Unmarshal(body, &req)
		embs := mockEmbedding[T](req.Texts, 4)
		var res cohere.EmbeddingResponse
		switch any(embs).(type) {
		case [][]float32:
			res.Embeddings.Float = any(embs).([][]float32)
		case [][]int8:
			res.Embeddings.Int8 = any(embs).([][]int8)
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))
	return ts
}

func CreateTEIEmbeddingServer(dim int) *httptest.Server {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req tei.EmbeddingRequest
		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()
		json.Unmarshal(body, &req)
		embs := mockEmbedding[float32](req.Inputs, dim)
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(embs)
		w.Write(data)
	}))
	return ts
}

type MockBedrockClient struct {
	dim int
}

func (c *MockBedrockClient) InvokeModel(ctx context.Context, params *bedrockruntime.InvokeModelInput, optFns ...func(*bedrockruntime.Options)) (*bedrockruntime.InvokeModelOutput, error) {
	var req BedRockRequest
	json.Unmarshal(params.Body, &req)
	embs := mockEmbedding[float32]([]string{req.InputText}, c.dim)

	var resp BedRockResponse
	resp.Embedding = embs[0]
	resp.InputTextTokenCount = 2
	body, _ := json.Marshal(resp)
	return &bedrockruntime.InvokeModelOutput{Body: body}, nil
}

func GenSearchResultData(nq int64, topk int64, dType schemapb.DataType, fieldName string, fieldId int64) *schemapb.SearchResultData {
	tops := make([]int64, nq)
	for i := 0; i < int(nq); i++ {
		tops[i] = topk
	}
	fieldsData := []*schemapb.FieldData{}
	if fieldName != "" {
		fieldsData = []*schemapb.FieldData{testutils.GenerateScalarFieldData(dType, fieldName, int(nq*topk))}
		fieldsData[0].FieldId = fieldId
	}

	data := &schemapb.SearchResultData{
		NumQueries: nq,
		TopK:       topk,
		Scores:     testutils.GenerateFloat32Array(int(nq * topk)),
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: testutils.GenerateInt64Array(int(nq * topk)),
				},
			},
		},
		Topks:      tops,
		FieldsData: fieldsData,
	}
	return data
}

func GenSearchResultDataWithGrouping(nq int64, topk int64, dType schemapb.DataType, fieldName string, fieldId int64, groupingName string, groupingId int64, groupSize int64) *schemapb.SearchResultData {
	data := GenSearchResultData(nq, topk*groupSize, dType, fieldName, fieldId)
	values := make([]int64, 0)
	for i := int64(0); i < nq*topk*groupSize; i += groupSize {
		for j := int64(0); j < groupSize; j++ {
			values = append(values, i)
		}
	}
	groupingField := testutils.GenerateScalarFieldDataWithValue(schemapb.DataType_Int64, groupingName, groupingId, values)
	data.GroupByFieldValue = groupingField
	return data
}

func FloatsAlmostEqual(a, b []float32, epsilon float32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if float32(math.Abs(float64(a[i]-b[i]))) > epsilon {
			return false
		}
	}
	return true
}
