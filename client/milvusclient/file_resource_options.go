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

package milvusclient

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

// AddFileResourceOption builds an AddFileResource request.
type AddFileResourceOption interface {
	Request() *milvuspb.AddFileResourceRequest
}

type addFileResourceOption struct {
	name string
	path string
}

// NewAddFileResourceOption creates an option for registering a remote file.
func NewAddFileResourceOption(name, path string) *addFileResourceOption {
	return &addFileResourceOption{name: name, path: path}
}

func (opt *addFileResourceOption) Request() *milvuspb.AddFileResourceRequest {
	return &milvuspb.AddFileResourceRequest{
		Base: &commonpb.MsgBase{},
		Name: opt.name,
		Path: opt.path,
	}
}

// RemoveFileResourceOption builds a RemoveFileResource request.
type RemoveFileResourceOption interface {
	Request() *milvuspb.RemoveFileResourceRequest
}

type removeFileResourceOption struct {
	name string
}

// NewRemoveFileResourceOption creates an option for unregistering a remote file.
func NewRemoveFileResourceOption(name string) *removeFileResourceOption {
	return &removeFileResourceOption{name: name}
}

func (opt *removeFileResourceOption) Request() *milvuspb.RemoveFileResourceRequest {
	return &milvuspb.RemoveFileResourceRequest{
		Base: &commonpb.MsgBase{},
		Name: opt.name,
	}
}

// ListFileResourcesOption builds a ListFileResources request.
type ListFileResourcesOption interface {
	Request() *milvuspb.ListFileResourcesRequest
}

type listFileResourcesOption struct{}

// NewListFileResourcesOption creates an option for listing registered files.
func NewListFileResourcesOption() *listFileResourcesOption {
	return &listFileResourcesOption{}
}

func (opt *listFileResourcesOption) Request() *milvuspb.ListFileResourcesRequest {
	return &milvuspb.ListFileResourcesRequest{Base: &commonpb.MsgBase{}}
}

var (
	_ AddFileResourceOption    = (*addFileResourceOption)(nil)
	_ RemoveFileResourceOption = (*removeFileResourceOption)(nil)
	_ ListFileResourcesOption  = (*listFileResourcesOption)(nil)
)
