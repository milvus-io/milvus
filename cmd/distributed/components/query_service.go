// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package components

import (
	"context"

	grpcqueryservice "github.com/zilliztech/milvus-distributed/internal/distributed/queryservice"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type QueryService struct {
	ctx context.Context
	svr *grpcqueryservice.Server
}

func NewQueryService(ctx context.Context, factory msgstream.Factory) (*QueryService, error) {
	svr, err := grpcqueryservice.NewServer(ctx, factory)
	if err != nil {
		panic(err)
	}

	return &QueryService{
		ctx: ctx,
		svr: svr,
	}, nil
}

func (qs *QueryService) Run() error {
	if err := qs.svr.Run(); err != nil {
		panic(err)
	}
	return nil
}

func (qs *QueryService) Stop() error {
	if err := qs.svr.Stop(); err != nil {
		return err
	}
	return nil
}
