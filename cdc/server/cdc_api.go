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
	"github.com/milvus-io/milvus/cdc/core/util"
	"github.com/milvus-io/milvus/cdc/server/model/request"
)

type CDCApi interface {
	util.CDCMark

	ReloadTask()
	Create(request *request.CreateRequest) (*request.CreateResponse, error)
	Delete(request *request.DeleteRequest) (*request.DeleteResponse, error)
	Pause(request *request.PauseRequest) (*request.PauseResponse, error)
	Resume(request *request.ResumeRequest) (*request.ResumeResponse, error)
	Get(request *request.GetRequest) (*request.GetResponse, error)
	List(request *request.ListRequest) (*request.ListResponse, error)
}

type BaseCDC struct {
	util.CDCMark
}

func NewBaseCdc() *BaseCDC {
	return &BaseCDC{}
}

func (b *BaseCDC) ReloadTask() {

}

func (b *BaseCDC) Create(request *request.CreateRequest) (*request.CreateResponse, error) {
	return nil, nil
}

func (b *BaseCDC) Delete(request *request.DeleteRequest) (*request.DeleteResponse, error) {
	return nil, nil
}

func (b *BaseCDC) Pause(request *request.PauseRequest) (*request.PauseResponse, error) {
	return nil, nil
}

func (b *BaseCDC) Resume(request *request.ResumeRequest) (*request.ResumeResponse, error) {
	return nil, nil
}

func (b *BaseCDC) Get(request *request.GetRequest) (*request.GetResponse, error) {
	return nil, nil
}

func (b *BaseCDC) List(request *request.ListRequest) (*request.ListResponse, error) {
	return nil, nil
}

func GetCDCApi(config *CdcServerConfig) CDCApi {
	return NewMetaCDC(config)
}
