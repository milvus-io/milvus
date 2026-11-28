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

package vertexai

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type Instance struct {
	TaskType string `json:"task_type,omitempty"`
	Content  string `json:"content"`
}

type Parameters struct {
	OutputDimensionality int64 `json:"outputDimensionality,omitempty"`
}

type EmbeddingRequest struct {
	Instances  []Instance `json:"instances"`
	Parameters Parameters `json:"parameters,omitempty"`
}

type Statistics struct {
	Truncated  bool `json:"truncated"`
	TokenCount int  `json:"token_count"`
}

type Embeddings struct {
	Statistics Statistics `json:"statistics"`
	Values     []float32  `json:"values"`
}

type Prediction struct {
	Embeddings Embeddings `json:"embeddings"`
}

type Metadata struct {
	BillableCharacterCount int `json:"billableCharacterCount"`
}

type EmbeddingResponse struct {
	Predictions []Prediction `json:"predictions"`
	Metadata    Metadata     `json:"metadata"`
}

type ErrorInfo struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

// Gemini-specific types for :embedContent endpoint

type GeminiPart struct {
	Text string `json:"text"`
}

type GeminiContent struct {
	Parts []GeminiPart `json:"parts"`
}

type GeminiEmbedContentRequest struct {
	Content              GeminiContent `json:"content"`
	TaskType             string        `json:"taskType,omitempty"`
	OutputDimensionality int64         `json:"outputDimensionality,omitempty"`
}

type GeminiEmbeddingValues struct {
	Values []float32 `json:"values"`
}

type GeminiEmbedContentResponse struct {
	Embedding GeminiEmbeddingValues `json:"embedding"`
}

type VertexAIEmbedding struct {
	url     string
	jsonKey []byte
	scopes  string
	token   string
}

func NewVertexAIEmbedding(url string, jsonKey []byte, scopes string, token string) *VertexAIEmbedding {
	return &VertexAIEmbedding{
		url:     url,
		jsonKey: jsonKey,
		scopes:  scopes,
		token:   token,
	}
}

func (c *VertexAIEmbedding) Check() error {
	if c.url == "" {
		return merr.WrapErrParameterInvalidMsg("VertexAI embedding url is empty")
	}
	if len(c.jsonKey) == 0 {
		return merr.WrapErrParameterInvalidMsg("jsonKey is empty")
	}
	if c.scopes == "" {
		return merr.WrapErrParameterInvalidMsg("Scopes param is empty")
	}
	return nil
}

func (c *VertexAIEmbedding) getAccessToken(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	// The service-account token source uses PostForm without attaching ctx.
	// Wrap a copy of its HTTP client so token exchange also honors the caller.
	client := *oauth2.NewClient(ctx, nil)
	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	client.Transport = &oauthContextTransport{ctx: ctx, base: transport}
	authCtx := context.WithValue(ctx, oauth2.HTTPClient, &client)
	creds, err := google.CredentialsFromJSON(authCtx, c.jsonKey, c.scopes)
	if err != nil {
		return "", merr.Wrap(err, "failed to find credentials")
	}
	token, err := creds.TokenSource.Token()
	// OAuth token exchange formats transport errors without preserving their cause.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return "", ctxErr
	}
	if err != nil {
		return "", merr.Wrap(err, "failed to get token")
	}
	return token.AccessToken, nil
}

type oauthContextTransport struct {
	ctx  context.Context
	base http.RoundTripper
}

func (t *oauthContextTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := req.Context().Err(); err != nil {
		return nil, err
	}
	// Keep a deadline installed by http.Client.Timeout as well as the caller's.
	var ctx context.Context
	var cancel context.CancelFunc
	deadline, hasDeadline := req.Context().Deadline()
	if hasDeadline {
		ctx, cancel = context.WithDeadline(t.ctx, deadline)
	} else {
		ctx, cancel = context.WithCancel(t.ctx)
	}
	stop := context.AfterFunc(req.Context(), func() {
		// The copied deadline expires on its own; canceling it here could
		// replace DeadlineExceeded with Canceled when the two timers race.
		if !hasDeadline || req.Context().Err() != context.DeadlineExceeded {
			cancel()
		}
	})
	release := func() {
		stop()
		cancel()
	}
	resp, err := t.base.RoundTrip(req.Clone(ctx))
	if err != nil || resp == nil || resp.Body == nil {
		release()
		return resp, err
	}
	// RoundTrip ends at the response headers; cancellation must cover body reads too.
	resp.Body = &oauthContextResponseBody{ReadCloser: resp.Body, release: release}
	return resp, nil
}

type oauthContextResponseBody struct {
	io.ReadCloser
	release func()
}

func (b *oauthContextResponseBody) Close() error {
	b.release()
	return b.ReadCloser.Close()
}

func (c *VertexAIEmbedding) GeminiEmbedding(ctx context.Context, url string, text string, dim int64, taskType string, timeoutMs int64) (*GeminiEmbedContentResponse, error) {
	req := GeminiEmbedContentRequest{
		Content: GeminiContent{
			Parts: []GeminiPart{{Text: text}},
		},
	}
	if taskType != "" {
		req.TaskType = taskType
	}
	if dim > 0 {
		req.OutputDimensionality = dim
	}

	var token string
	var err error
	if c.token != "" {
		token = c.token
	} else {
		token, err = c.getAccessToken(ctx)
		if err != nil {
			return nil, err
		}
	}

	headers := map[string]string{
		"Content-Type":  "application/json",
		"Authorization": fmt.Sprintf("Bearer %s", token),
	}
	res, err := models.PostRequest[GeminiEmbedContentResponse](ctx, req, url, headers, timeoutMs)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (c *VertexAIEmbedding) Embedding(ctx context.Context, modelName string, texts []string, dim int64, taskType string, timeoutMs int64) (*EmbeddingResponse, error) {
	var r EmbeddingRequest
	for _, text := range texts {
		r.Instances = append(r.Instances, Instance{TaskType: taskType, Content: text})
	}
	if dim != 0 {
		r.Parameters.OutputDimensionality = dim
	}

	var token string
	var err error
	if c.token != "" {
		token = c.token
	} else {
		token, err = c.getAccessToken(ctx)
		if err != nil {
			return nil, err
		}
	}

	headers := map[string]string{
		"Content-Type":  "application/json",
		"Authorization": fmt.Sprintf("Bearer %s", token),
	}
	res, err := models.PostRequest[EmbeddingResponse](ctx, r, c.url, headers, timeoutMs)
	if err != nil {
		return nil, err
	}
	return res, nil
}
