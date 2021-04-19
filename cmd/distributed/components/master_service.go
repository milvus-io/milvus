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
	"io"

	"github.com/opentracing/opentracing-go"
	msc "github.com/zilliztech/milvus-distributed/internal/distributed/masterservice"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type MasterService struct {
	ctx context.Context
	svr *msc.Server

	tracer opentracing.Tracer
	closer io.Closer
}

func NewMasterService(ctx context.Context, factory msgstream.Factory) (*MasterService, error) {

	svr, err := msc.NewServer(ctx, factory)
	if err != nil {
		return nil, err
	}
	return &MasterService{
		ctx: ctx,
		svr: svr,
	}, nil
}

func (m *MasterService) Run() error {
	if err := m.svr.Run(); err != nil {
		return err
	}
	return nil
}

func (m *MasterService) Stop() error {
	if err := m.svr.Stop(); err != nil {
		return err
	}
	return nil
}
