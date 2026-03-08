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
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

var testResult = gin.H{
	"message": "This is a test message",
	"data":    make(map[string]interface{}),
}

func init() {
	const chunkSize = 1024
	rs := randomString(chunkSize)
	var sb strings.Builder
	for sb.Len() < 10*1024*1024 {
		sb.WriteString(rs)
	}
	testResult["data"] = sb.String()
}

func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var result strings.Builder
	for i := 0; i < length; i++ {
		result.WriteByte(charset[rand.Intn(len(charset))])
	}
	return result.String()
}

func BenchmarkHTTPReturn(b *testing.B) {
	// Set Gin to test mode to prevent output to stdout
	gin.SetMode(gin.TestMode)

	b.Run("test HTTPReturn", func(b *testing.B) {
		router := gin.New()
		router.GET("/test1", func(c *gin.Context) {
			HTTPReturn(c, http.StatusOK, testResult)
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			req := httptest.NewRequest("GET", "/test1", nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
		}

		b.ReportAllocs()
	})

	b.Run("test HTTPReturnStream", func(b *testing.B) {
		router := gin.New()
		router.GET("/test2", func(c *gin.Context) {
			HTTPReturnStream(c, http.StatusOK, testResult)
		})

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			req := httptest.NewRequest("GET", "/test2", nil)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)
		}

		b.ReportAllocs()
	})
}

// goos: linux
// goarch: amd64
// pkg: github.com/milvus-io/milvus/internal/distributed/proxy/httpserver
// cpu: Intel(R) Core(TM) i7-8700 CPU @ 3.20GHz
// BenchmarkHTTPReturn
// BenchmarkHTTPReturn/test_HTTPReturn
// BenchmarkHTTPReturn/test_HTTPReturn-12         	      87	  13127452 ns/op	27992718 B/op	      34 allocs/op
// BenchmarkHTTPReturn/test_HTTPReturnStream
// BenchmarkHTTPReturn/test_HTTPReturnStream-12   	      87	  12804875 ns/op	14361636 B/op	      31 allocs/op
// PASS
//
// Process finished with the exit code 0
