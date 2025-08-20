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

package cdc

import (
	"context"

	"github.com/milvus-io/milvus/internal/cdc/resource"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

type CDCServer struct {
	ctx context.Context
}

// NewCDCServer will return a CDCServer.
func NewCDCServer(ctx context.Context) *CDCServer {
	return &CDCServer{
		ctx: ctx,
	}
}

// Init initializes the CDCServer.
func (svr *CDCServer) Init() error {
	log.Ctx(svr.ctx).Info("CDCServer init successfully")
	return nil
}

// Start starts CDCServer.
func (svr *CDCServer) Start() error {
	resource.Resource().Controller().Start()
	log.Ctx(svr.ctx).Info("CDCServer start successfully")
	return nil
}

// Stop stops CDCServer.
func (svr *CDCServer) Stop() error {
	resource.Resource().Controller().Stop()
	log.Ctx(svr.ctx).Info("CDCServer stop successfully")
	return nil
}
