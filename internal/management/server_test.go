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

package management

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestGetHTTPAddr(t *testing.T) {
	assert.Equal(t, getHTTPAddr(), ":"+DefaultListenPort)
	testPort := "9092"
	t.Setenv(ListenPortEnvKey, testPort)
	assert.Equal(t, getHTTPAddr(), ":"+testPort)
}

func TestDefaultLogHandler(t *testing.T) {
	httpServer := httptest.NewServer(nil)
	defer httpServer.Close()
	registerDefaults()

	log.SetLevel(zap.DebugLevel)
	assert.Equal(t, zap.DebugLevel, log.GetLevel())

	// replace global logger, log change will not be affected.
	conf := &log.Config{Level: "info", File: log.FileLogConfig{}, DisableTimestamp: true}
	logger, p, _ := log.InitLogger(conf)
	log.ReplaceGlobals(logger, p)
	assert.Equal(t, zap.InfoLevel, log.GetLevel())

	// change log level through http
	payload, err := json.Marshal(map[string]interface{}{"level": "error"})
	if err != nil {
		log.Fatal(err.Error())
	}

	url := httpServer.URL + "/log/level"
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(payload))
	req.Header.Set("Content-Type", "application/json")
	if err != nil {
		log.Fatal(err.Error())
	}

	client := httpServer.Client()
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err.Error())
	}
	assert.Equal(t, "{\"level\":\"error\"}\n", string(body))
	assert.Equal(t, zap.ErrorLevel, log.GetLevel())
}
