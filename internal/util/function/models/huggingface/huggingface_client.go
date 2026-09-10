// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package huggingface

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	DefaultBaseURL    = "https://router.huggingface.co"
	DefaultHFProvider = "hf-inference"

	FeatureExtractionTask  = "feature-extraction"
	SentenceSimilarityTask = "sentence-similarity"
)

type Client struct {
	apiKey  string
	baseURL string
}

func NewClient(apiKey string, baseURL string) (*Client, error) {
	if apiKey == "" {
		return nil, merr.WrapErrParameterInvalidMsg("missing credentials config or configure the MILVUS_HUGGINGFACE_API_KEY environment variable in the Milvus service")
	}
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	if _, err := models.NewBaseURL(baseURL); err != nil {
		return nil, err
	}
	return &Client{
		apiKey:  apiKey,
		baseURL: baseURL,
	}, nil
}

func (c *Client) headers() map[string]string {
	return map[string]string{
		"Content-Type":  "application/json",
		"Authorization": fmt.Sprintf("Bearer %s", c.apiKey),
	}
}

type FeatureExtractionRequest map[string]any

type FeatureExtractionResponse = json.RawMessage

type SentenceSimilarityInputs struct {
	SourceSentence string   `json:"source_sentence"`
	Sentences      []string `json:"sentences"`
}

type SentenceSimilarityRequest struct {
	Inputs SentenceSimilarityInputs `json:"inputs"`
}

type SentenceSimilarityResponse []float32

func (c *Client) FeatureExtraction(hfProvider string, modelName string, texts []string, params map[string]any, timeoutMs int64) (*FeatureExtractionResponse, error) {
	url, err := c.buildPipelineURL(hfProvider, modelName, FeatureExtractionTask)
	if err != nil {
		return nil, err
	}
	req := FeatureExtractionRequest{
		"inputs": texts,
	}
	for k, v := range params {
		req[k] = v
	}
	return models.PostRequest[FeatureExtractionResponse](req, url, c.headers(), timeoutMs)
}

func (c *Client) SentenceSimilarity(hfProvider string, modelName string, query string, texts []string, timeoutMs int64) (*SentenceSimilarityResponse, error) {
	url, err := c.buildPipelineURL(hfProvider, modelName, SentenceSimilarityTask)
	if err != nil {
		return nil, err
	}
	req := SentenceSimilarityRequest{
		Inputs: SentenceSimilarityInputs{
			SourceSentence: query,
			Sentences:      texts,
		},
	}
	return models.PostRequest[SentenceSimilarityResponse](req, url, c.headers(), timeoutMs)
}

func (c *Client) buildPipelineURL(hfProvider string, modelName string, task string) (string, error) {
	return buildPipelineURL(c.baseURL, hfProvider, modelName, task)
}

func buildPipelineURL(baseURL string, hfProvider string, modelName string, task string) (string, error) {
	if hfProvider == "" {
		hfProvider = DefaultHFProvider
	}
	if hfProvider != DefaultHFProvider {
		return "", merr.WrapErrParameterInvalidMsg("Hugging Face hf_provider only supports %s", DefaultHFProvider)
	}
	if err := validateModelName(modelName); err != nil {
		return "", err
	}
	if task == "" {
		return "", merr.WrapErrParameterInvalidMsg("Hugging Face pipeline task is required")
	}

	base, err := models.NewBaseURL(baseURL)
	if err != nil {
		return "", err
	}

	prefix := strings.TrimRight(base.Path, "/")
	base.Path = fmt.Sprintf("%s/%s/models/%s/pipeline/%s",
		prefix,
		url.PathEscape(hfProvider),
		modelName,
		task,
	)
	return base.String(), nil
}

func validateModelName(modelName string) error {
	if modelName == "" {
		return merr.WrapErrParameterInvalidMsg("Hugging Face model name is required")
	}
	parts := strings.Split(modelName, "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return merr.WrapErrParameterInvalidMsg("Hugging Face model name contains invalid path segment")
		}
	}
	return nil
}
