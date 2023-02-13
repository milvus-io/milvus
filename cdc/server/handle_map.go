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
	"errors"

	modelrequest "github.com/milvus-io/milvus/cdc/server/model/request"
)

var (
	requestHandlers map[string]*requestHandler
)

type requestHandler struct {
	generateModel func() any
	handle        func(api CDCApi, request any) (any, error)
}

func init() {
	requestHandlers = map[string]*requestHandler{
		modelrequest.RequestTypeCreate: {
			generateModel: func() any {
				return &modelrequest.CreateRequest{}
			},
			handle: func(api CDCApi, request any) (any, error) {
				createRequest, ok := request.(*modelrequest.CreateRequest)
				if !ok {
					return nil, errors.New("fail to cast the request to the create model")
				}
				return api.Create(createRequest)
			},
		},
		modelrequest.RequestTypeDelete: {
			generateModel: func() any {
				return &modelrequest.DeleteRequest{}
			},
			handle: func(api CDCApi, request any) (any, error) {
				deleteRequest, ok := request.(*modelrequest.DeleteRequest)
				if !ok {
					return nil, errors.New("fail to cast the request to the delete model")
				}
				return api.Delete(deleteRequest)
			},
		},
		modelrequest.RequestTypePause: {
			generateModel: func() any {
				return &modelrequest.PauseRequest{}
			},
			handle: func(api CDCApi, request any) (any, error) {
				pauseRequest, ok := request.(*modelrequest.PauseRequest)
				if !ok {
					return nil, errors.New("fail to cast the request to the pause model")
				}
				return api.Pause(pauseRequest)
			},
		},
		modelrequest.RequestTypeResume: {
			generateModel: func() any {
				return &modelrequest.ResumeRequest{}
			},
			handle: func(api CDCApi, request any) (any, error) {
				resumeRequest, ok := request.(*modelrequest.ResumeRequest)
				if !ok {
					return nil, errors.New("fail to cast the request to the resume model")
				}
				return api.Resume(resumeRequest)
			},
		},
		modelrequest.RequestTypeGet: {
			generateModel: func() any {
				return &modelrequest.GetRequest{}
			},
			handle: func(api CDCApi, request any) (any, error) {
				getRequest, ok := request.(*modelrequest.GetRequest)
				if !ok {
					return nil, errors.New("fail to cast the request to the get model")
				}
				return api.Get(getRequest)
			},
		},
		modelrequest.RequestTypeList: {
			generateModel: func() any {
				return &modelrequest.ListRequest{}
			},
			handle: func(api CDCApi, request any) (any, error) {
				listRequest, ok := request.(*modelrequest.ListRequest)
				if !ok {
					return nil, errors.New("fail to cast the request to the list model")
				}
				return api.List(listRequest)
			},
		},
	}
}
