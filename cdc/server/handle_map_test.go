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

package server

import (
	"testing"

	"github.com/milvus-io/milvus/cdc/server/model/request"
	"github.com/stretchr/testify/assert"
)

func TestRequestHandle(t *testing.T) {
	assertion := assert.New(t)
	assertion.Len(requestHandlers, 6)

	baseApi := NewBaseCDC()

	handler, ok := requestHandlers[request.Create]
	assertion.True(ok)
	assertion.IsType(&request.CreateRequest{}, handler.generateModel())
	_, err := handler.handle(baseApi, &request.CreateRequest{})
	assertion.NoError(err)
	_, err = handler.handle(baseApi, &request.DeleteRequest{})
	assertion.Error(err)

	handler, ok = requestHandlers[request.Delete]
	assertion.True(ok)
	assertion.IsType(&request.DeleteRequest{}, handler.generateModel())
	_, err = handler.handle(baseApi, &request.DeleteRequest{})
	assertion.NoError(err)
	_, err = handler.handle(baseApi, &request.CreateRequest{})
	assertion.Error(err)

	handler, ok = requestHandlers[request.Pause]
	assertion.True(ok)
	assertion.IsType(&request.PauseRequest{}, handler.generateModel())
	_, err = handler.handle(baseApi, &request.PauseRequest{})
	assertion.NoError(err)
	_, err = handler.handle(baseApi, &request.ResumeRequest{})
	assertion.Error(err)

	handler, ok = requestHandlers[request.Resume]
	assertion.True(ok)
	assertion.IsType(&request.ResumeRequest{}, handler.generateModel())
	_, err = handler.handle(baseApi, &request.ResumeRequest{})
	assertion.NoError(err)
	_, err = handler.handle(baseApi, &request.PauseRequest{})
	assertion.Error(err)

	handler, ok = requestHandlers[request.Get]
	assertion.True(ok)
	assertion.IsType(&request.GetRequest{}, handler.generateModel())
	_, err = handler.handle(baseApi, &request.GetRequest{})
	assertion.NoError(err)
	_, err = handler.handle(baseApi, &request.ListRequest{})
	assertion.Error(err)

	handler, ok = requestHandlers[request.List]
	assertion.True(ok)
	assertion.IsType(&request.ListRequest{}, handler.generateModel())
	_, err = handler.handle(baseApi, &request.ListRequest{})
	assertion.NoError(err)
	_, err = handler.handle(baseApi, &request.GetRequest{})
	assertion.Error(err)
}
