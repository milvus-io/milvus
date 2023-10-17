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

package eventlog

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/suite"
)

type HandlerSuite struct {
	suite.Suite
}

func (s *HandlerSuite) SetupTest() {
	s.cleanGrpcLog()
}

func (s *HandlerSuite) TearDownTest() {
	s.cleanGrpcLog()
}

func (s *HandlerSuite) cleanGrpcLog() {
	l := grpcLog.Load()
	if l != nil {
		l.Close()
	}
}

func (s *HandlerSuite) TestServerHTTP() {
	req := httptest.NewRequest(http.MethodGet, "/eventlog", nil)
	w := httptest.NewRecorder()

	handler := Handler()

	handler.ServeHTTP(w, req)

	res := w.Result()
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	s.Require().NoError(err)

	resp := eventLogResponse{}

	err = json.Unmarshal(data, &resp)
	s.Require().NoError(err)

	s.Equal(http.StatusOK, resp.Status)

	l := grpcLog.Load()
	s.NotNil(l)
	s.Equal(l.port, resp.Port)
}

func TestHandler(t *testing.T) {
	suite.Run(t, new(HandlerSuite))
}
