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

package http

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/http/healthz"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
)

func TestGetHTTPAddr(t *testing.T) {
	assert.Equal(t, getHTTPAddr(), ":"+DefaultListenPort)
	testPort := "9092"
	t.Setenv(ListenPortEnvKey, testPort)
	assert.Equal(t, getHTTPAddr(), ":"+testPort)
}

type HTTPServerTestSuite struct {
	suite.Suite
	server *httptest.Server
}

func (suite *HTTPServerTestSuite) SetupSuite() {
	suite.server = httptest.NewServer(nil)
	registerDefaults()

}

func (suite *HTTPServerTestSuite) TearDownSuite() {
	defer suite.server.Close()
}

func (suite *HTTPServerTestSuite) TestDefaultLogHandler() {
	log.SetLevel(zap.DebugLevel)
	suite.Equal(zap.DebugLevel, log.GetLevel())

	// replace global logger, log change will not be affected.
	conf := &log.Config{Level: "info", File: log.FileLogConfig{}, DisableTimestamp: true}
	logger, p, _ := log.InitLogger(conf)
	log.ReplaceGlobals(logger, p)
	suite.Equal(zap.InfoLevel, log.GetLevel())

	// change log level through http
	payload, err := json.Marshal(map[string]any{"level": "error"})
	if err != nil {
		log.Fatal(err.Error())
	}

	url := suite.server.URL + "/log/level"
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	if err != nil {
		log.Fatal(err.Error())
	}

	client := suite.server.Client()
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err.Error())
	}
	suite.Equal("{\"level\":\"error\"}\n", string(body))
	suite.Equal(zap.ErrorLevel, log.GetLevel())
}

func (suite *HTTPServerTestSuite) TestHealthzHandler() {
	url := suite.server.URL + "/healthz"
	client := suite.server.Client()

	healthz.Register(&MockIndicator{"m1", commonpb.StateCode_Healthy})

	req, _ := http.NewRequest(http.MethodGet, url, nil)
	resp, err := client.Do(req)
	suite.Nil(err)
	defer resp.Body.Close()
	body, _ := ioutil.ReadAll(resp.Body)
	suite.Equal("OK", string(body))

	req, _ = http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	suite.Nil(err)
	defer resp.Body.Close()
	body, _ = ioutil.ReadAll(resp.Body)
	suite.Equal("{\"state\":\"OK\",\"detail\":[{\"name\":\"m1\",\"code\":1}]}", string(body))

	healthz.Register(&MockIndicator{"m2", commonpb.StateCode_Abnormal})
	req, _ = http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("Content-Type", "application/json")
	resp, err = client.Do(req)
	suite.Nil(err)
	defer resp.Body.Close()
	body, _ = ioutil.ReadAll(resp.Body)
	suite.Equal("{\"state\":\"component m2 state is Abnormal\",\"detail\":[{\"name\":\"m1\",\"code\":1},{\"name\":\"m2\",\"code\":2}]}", string(body))
}

func TestHTTPServerSuite(t *testing.T) {
	suite.Run(t, new(HTTPServerTestSuite))
}

type MockIndicator struct {
	name string
	code commonpb.StateCode
}

func (m *MockIndicator) Health(ctx context.Context) commonpb.StateCode {
	return m.code
}

func (m *MockIndicator) GetName() string {
	return m.name
}
