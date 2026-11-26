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

package httpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"testing/iotest"

	"github.com/gin-gonic/gin"
	"github.com/klauspost/compress/gzip"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/proxy/httpserver/httpcompress"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// compressionTestServer serves the v2 routes behind the same chain the proxy
// installs -- h2c, response compression, DQL streaming, gin with
// authentication before request decoding -- over a real listener, so real
// clients exercise every layer a compressed body passes through.
//
// The returned counters are the Insert calls that reached the proxy, and the
// Read calls on a request body the middleware had already decoded.
func compressionTestServer(t *testing.T, enabled bool, rows int, needAuth ...bool) (*httptest.Server, *atomic.Int32, *atomic.Int64) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.QuotaConfig.QuotaAndLimitsEnabled.Key, "false")
	params.Save(params.HTTPCfg.CompressionEnabled.Key, fmt.Sprint(enabled))
	t.Cleanup(func() {
		params.Reset(params.QuotaConfig.QuotaAndLimitsEnabled.Key)
		params.Reset(params.HTTPCfg.CompressionEnabled.Key)
	})

	texts := make([]string, rows)
	for i := range texts {
		texts[i] = fmt.Sprintf("rest query text number %d with some repeated words", i)
	}
	mp := mocks.NewMockProxy(t)
	mp.EXPECT().DescribeCollection(mock.Anything, mock.Anything).Return(&milvuspb.DescribeCollectionResponse{
		CollectionName: DefaultCollectionName,
		Schema:         generateTextCollectionSchema(false),
		ShardsNum:      ShardNumDefault,
		Status:         &StatusSuccess,
	}, nil).Maybe()
	mp.EXPECT().Query(mock.Anything, mock.Anything).Return(&milvuspb.QueryResults{
		Status:       commonSuccessStatus,
		OutputFields: []string{FieldText},
		FieldsData: []*schemapb.FieldData{{
			Type:      schemapb.DataType_Text,
			FieldName: FieldText,
			Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: texts}},
			}},
		}},
	}, nil).Maybe()
	inserts := &atomic.Int32{}
	mp.EXPECT().Insert(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *milvuspb.InsertRequest) (*milvuspb.MutationResult, error) {
			inserts.Add(1)
			text := getFieldDataByName(req.GetFieldsData(), FieldText)
			if text == nil || len(text.GetScalars().GetStringData().GetData()) != 1 ||
				text.GetScalars().GetStringData().GetData()[0] != "rest text value" {
				return nil, merr.WrapErrParameterInvalidMsg("insert did not see the decoded row")
			}
			return &milvuspb.MutationResult{Status: commonSuccessStatus, InsertCnt: 1, IDs: generateIDs(schemapb.DataType_Int64, 1)}, nil
		}).Maybe()

	engine := gin.New()
	engine.Use(IdempotencyKeyHandlerFunc)
	engine.Use(genAuthMiddleWare(len(needAuth) > 0 && needAuth[0]))
	decodedBodyReads := &atomic.Int64{}
	countDecodedReads := func(c *gin.Context) {
		if _, decoded := c.Get(gin.BodyBytesKey); decoded {
			c.Request.Body = &countingBody{ReadCloser: c.Request.Body, reads: decodedBodyReads}
		}
	}
	NewHandlersV2(mp).RegisterRoutesToV2(engine.Group("/v2/vectordb"), httpcompress.RequestBodyMiddleware(AbortHTTPResponse), countDecodedReads)
	handler := h2c.NewHandler(httpcompress.Handler(DQLStreamingHTTPHandler(engine)), &http2.Server{})
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv, inserts, decodedBodyReads
}

// countingBody counts every Read call, including reads that return zero bytes.
type countingBody struct {
	io.ReadCloser
	reads *atomic.Int64
}

func (b *countingBody) Read(p []byte) (int, error) {
	b.reads.Add(1)
	return b.ReadCloser.Read(p)
}

var compressionInsertBody = []byte(`{
	"collectionName": "book",
	"data": [{"book_id": 1, "word_count": 10, "book_intro": [0.1, 0.2], "text_field": "rest text value"}]
}`)

var compressionQueryBody = []byte(`{"collectionName": "book", "filter": "book_id > 0", "outputFields": ["text_field"], "limit": 1000}`)

type restResult struct {
	status          int
	contentEncoding string
	acceptEncoding  string
	transportDecode bool
	wire            int
	body            map[string]any
}

func doREST(t *testing.T, client *http.Client, url string, header http.Header, body []byte) restResult {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	res := restResult{
		status:          resp.StatusCode,
		contentEncoding: resp.Header.Get("Content-Encoding"),
		acceptEncoding:  resp.Header.Get("Accept-Encoding"),
		transportDecode: resp.Uncompressed,
		wire:            len(raw),
	}
	plain := raw
	switch res.contentEncoding {
	case httpcompress.ContentEncodingZstd:
		dec, err := zstd.NewReader(bytes.NewReader(raw))
		require.NoError(t, err)
		plain, err = io.ReadAll(dec)
		dec.Close()
		require.NoError(t, err)
	case httpcompress.ContentEncodingGzip:
		gr, err := gzip.NewReader(bytes.NewReader(raw))
		require.NoError(t, err)
		plain, err = io.ReadAll(gr)
		require.NoError(t, err)
	}
	require.NoError(t, json.Unmarshal(plain, &res.body), string(plain))
	return res
}

func zstdEncode(t *testing.T, p []byte) []byte {
	enc, err := zstd.NewWriter(nil)
	require.NoError(t, err)
	defer enc.Close()
	return enc.EncodeAll(p, nil)
}

func gzipEncode(t *testing.T, p []byte) []byte {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	_, err := w.Write(p)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func TestRESTCompressionEndToEnd(t *testing.T) {
	// More rows than one DQL batch, so the query response takes the batched
	// writer path rather than the buffered recorder.
	rows := dqlJSONBatchRows*2 + 1
	srv, inserts, decodedBodyReads := compressionTestServer(t, true, rows)
	queryURL := srv.URL + versionalV2(EntityCategory, QueryAction)
	insertURL := srv.URL + versionalV2(EntityCategory, InsertAction)
	plainClient := &http.Client{Transport: &http.Transport{DisableCompression: true}}

	t.Run("query, client sends no Accept-Encoding", func(t *testing.T) {
		res := doREST(t, plainClient, queryURL, nil, compressionQueryBody)
		assert.Equal(t, http.StatusOK, res.status)
		assert.Empty(t, res.contentEncoding)
		assert.Equal(t, httpcompress.SupportedRequestEncodings, res.acceptEncoding, "advertised for request compression")
		assert.EqualValues(t, 0, res.body[HTTPReturnCode])
		assert.Len(t, res.body[HTTPReturnData], rows)
	})

	var plainWire int
	for _, accept := range []string{"zstd", "gzip"} {
		t.Run("query, Accept-Encoding "+accept, func(t *testing.T) {
			header := http.Header{}
			header.Set("Accept-Encoding", accept)
			res := doREST(t, plainClient, queryURL, header, compressionQueryBody)
			assert.Equal(t, http.StatusOK, res.status)
			assert.Equal(t, accept, res.contentEncoding)
			assert.EqualValues(t, 0, res.body[HTTPReturnCode])
			assert.Len(t, res.body[HTTPReturnData], rows)
			if plainWire == 0 {
				plainWire = doREST(t, plainClient, queryURL, nil, compressionQueryBody).wire
			}
			assert.Less(t, res.wire*3, plainWire, "%s: %d wire bytes vs %d plain", accept, res.wire, plainWire)
			t.Logf("%s: %d wire bytes vs %d plain", accept, res.wire, plainWire)
		})
	}

	t.Run("query, Go default transport", func(t *testing.T) {
		res := doREST(t, http.DefaultClient, queryURL, nil, compressionQueryBody)
		assert.True(t, res.transportDecode, "the transport asked for gzip and decoded it")
		assert.Len(t, res.body[HTTPReturnData], rows)
	})

	t.Run("query, compressed request admitted", func(t *testing.T) {
		header := http.Header{}
		header.Set("Content-Encoding", "gzip")
		res := doREST(t, plainClient, queryURL, header, gzipEncode(t, compressionQueryBody))
		require.Equal(t, http.StatusOK, res.status)
		require.EqualValues(t, 0, res.body[HTTPReturnCode], res.body[HTTPReturnMessage])
		assert.Len(t, res.body[HTTPReturnData], rows)
	})

	for _, tc := range []struct {
		coding string
		body   []byte
	}{
		{"", compressionInsertBody},
		{"zstd", zstdEncode(t, compressionInsertBody)},
		{"gzip", gzipEncode(t, compressionInsertBody)},
		// A charset sent as Content-Encoding (Apache HttpClient's
		// setContentEncoding("UTF-8")) was always ignored and still is.
		{"UTF-8", compressionInsertBody},
	} {
		t.Run("insert, Content-Encoding "+tc.coding, func(t *testing.T) {
			before := inserts.Load()
			header := http.Header{}
			if tc.coding != "" {
				header.Set("Content-Encoding", tc.coding)
			}
			res := doREST(t, plainClient, insertURL, header, tc.body)
			assert.Equal(t, http.StatusOK, res.status)
			assert.EqualValues(t, 0, res.body[HTTPReturnCode], res.body[HTTPReturnMessage])
			assert.Equal(t, before+1, inserts.Load())
		})
	}
	assert.Zero(t, decodedBodyReads.Load(),
		"insert binds the bytes the middleware decoded; reading them from the request body again would copy the whole body")

	t.Run("insert requires an acceptable response encoding", func(t *testing.T) {
		before := inserts.Load()
		req, err := http.NewRequest(http.MethodPost, insertURL, bytes.NewReader(compressionInsertBody))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept-Encoding", "br, identity;q=0")
		resp, err := plainClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusNotAcceptable, resp.StatusCode)
		require.Equal(t, before, inserts.Load())

		header := http.Header{}
		header.Set("Accept-Encoding", "gzip, identity;q=0")
		res := doREST(t, plainClient, insertURL, header, compressionInsertBody)
		require.Equal(t, http.StatusOK, res.status)
		require.Equal(t, "gzip", res.contentEncoding)
		assert.EqualValues(t, 0, res.body[HTTPReturnCode])
		assert.Equal(t, before+1, inserts.Load())
	})

	t.Run("insert, unsupported Content-Encoding", func(t *testing.T) {
		before := inserts.Load()
		header := http.Header{}
		header.Set("Content-Encoding", "br")
		res := doREST(t, plainClient, insertURL, header, compressionInsertBody)
		assert.Equal(t, http.StatusUnsupportedMediaType, res.status)
		assert.Equal(t, httpcompress.SupportedRequestEncodings, res.acceptEncoding)
		assert.EqualValues(t, merr.Code(merr.ErrParameterInvalid), res.body[HTTPReturnCode])
		assert.Equal(t, before, inserts.Load(), "the handler never ran")
	})
}

func TestRESTCompressionDQLAdmissionBeforeBodyRead(t *testing.T) {
	paramtable.Init()
	engine := gin.New()
	engine.Use(IdempotencyKeyHandlerFunc, genAuthMiddleWare(true))
	NewHandlersV2(&dqlFullProxy{full: true}).RegisterRoutesToV2(engine.Group("/v2/vectordb"), httpcompress.RequestBodyMiddleware(AbortHTTPResponse))
	server := httpcompress.Handler(DQLStreamingHTTPHandler(engine))

	for _, tc := range []struct {
		coding string
		body   []byte
	}{
		{"gzip", gzipEncode(t, compressionQueryBody)},
		{"zstd", zstdEncode(t, compressionQueryBody)},
	} {
		for _, action := range []string{QueryAction, GetAction, SearchAction, AdvancedSearchAction, HybridSearchAction} {
			t.Run(tc.coding+action, func(t *testing.T) {
				req := httptest.NewRequest(http.MethodPost, versionalV2(EntityCategory, action), bytes.NewReader(tc.body))
				req.SetBasicAuth(util.UserRoot, getDefaultRootPassword())
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Content-Encoding", tc.coding)
				var reads atomic.Int64
				req.Body = &countingBody{ReadCloser: req.Body, reads: &reads}
				w := httptest.NewRecorder()

				server.ServeHTTP(w, req)

				require.Equal(t, http.StatusTooManyRequests, w.Code)
				assert.Equal(t, "1", w.Header().Get("Retry-After"))
				assert.Zero(t, reads.Load(), "a full DQL queue must reject the request before reading its compressed body")
			})
		}
	}
}

func TestRESTCompressionCanceledUploadAbortsAfterLogging(t *testing.T) {
	paramtable.Init()
	cfg := &paramtable.Get().HTTPCfg
	paramtable.Get().Save(cfg.CompressionEnabled.Key, "true")
	t.Cleanup(func() { paramtable.Get().Reset(cfg.CompressionEnabled.Key) })

	engine := gin.New()
	var loggedCode int32
	var loggedMessage string
	engine.Use(func(c *gin.Context) {
		c.Next()
		loggedCode = c.GetInt32(HTTPReturnCode)
		loggedMessage = c.GetString(HTTPReturnMessage)
	}, gin.Recovery())
	engine.POST("/", httpcompress.RequestBodyMiddleware(AbortHTTPResponse), func(c *gin.Context) {
		t.Error("route ran after the upload was canceled")
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/", iotest.ErrReader(context.Canceled))
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("Accept-Encoding", "gzip, identity;q=0")
	// Leave Code at zero so any header committed to the transport is observable.
	rec := &httptest.ResponseRecorder{Body: new(bytes.Buffer)}
	handler := httpcompress.Handler(DQLStreamingHTTPHandler(engine))
	require.PanicsWithValue(t, http.ErrAbortHandler, func() { handler.ServeHTTP(rec, req) })
	assert.Equal(t, merr.CanceledCode, loggedCode)
	assert.Equal(t, context.Canceled.Error(), loggedMessage)
	assert.Zero(t, rec.Code)
	assert.Empty(t, rec.Body.Bytes())
}

// With the switch off, clients accepting identity get an uncompressed
// response. The server still accepts compressed request bodies.
func TestRESTCompressionDisabledStillDecodesRequests(t *testing.T) {
	srv, inserts, _ := compressionTestServer(t, false, 10)
	plainClient := &http.Client{Transport: &http.Transport{DisableCompression: true}}

	header := http.Header{}
	header.Set("Accept-Encoding", "zstd, gzip")
	res := doREST(t, plainClient, srv.URL+versionalV2(EntityCategory, QueryAction), header, compressionQueryBody)
	assert.Equal(t, http.StatusOK, res.status)
	assert.Empty(t, res.contentEncoding)
	assert.Equal(t, httpcompress.SupportedRequestEncodings, res.acceptEncoding)
	assert.Len(t, res.body[HTTPReturnData], 10)

	header = http.Header{}
	header.Set("Content-Encoding", "zstd")
	res = doREST(t, plainClient, srv.URL+versionalV2(EntityCategory, InsertAction), header, zstdEncode(t, compressionInsertBody))
	assert.Equal(t, http.StatusOK, res.status)
	assert.EqualValues(t, 0, res.body[HTTPReturnCode], res.body[HTTPReturnMessage])
	assert.EqualValues(t, 1, inserts.Load())
}

// Decoding runs after authentication: a request that fails it never makes the
// proxy expand a body, however far that body would expand.
func TestRESTCompressionDecodesOnlyAfterAuthentication(t *testing.T) {
	srv, inserts, _ := compressionTestServer(t, true, 1, true)
	params := paramtable.Get()
	params.Save(params.ProxyGrpcServerCfg.ServerMaxRecvSize.Key, "1024")
	t.Cleanup(func() { params.Reset(params.ProxyGrpcServerCfg.ServerMaxRecvSize.Key) })
	bomb := zstdEncode(t, make([]byte, 4<<20))
	plainClient := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	url := srv.URL + versionalV2(EntityCategory, InsertAction)

	header := http.Header{}
	header.Set("Content-Encoding", "zstd")
	res := doREST(t, plainClient, url, header, bomb)
	assert.Equal(t, http.StatusUnauthorized, res.status, "rejected by authentication, not by the decoder")

	header.Set("Authorization", "Bearer "+util.UserRoot+":"+getDefaultRootPassword())
	res = doREST(t, plainClient, url, header, bomb)
	assert.Equal(t, http.StatusRequestEntityTooLarge, res.status)
	assert.Zero(t, inserts.Load())
}
