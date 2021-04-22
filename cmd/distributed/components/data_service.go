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

	grpcdataserviceclient "github.com/milvus-io/milvus/internal/distributed/dataservice"
	"github.com/milvus-io/milvus/internal/msgstream"
)

type DataService struct {
	ctx context.Context
	svr *grpcdataserviceclient.Server
}

func NewDataService(ctx context.Context, factory msgstream.Factory) (*DataService, error) {
	s, err := grpcdataserviceclient.NewServer(ctx, factory)
	if err != nil {
		return nil, err
	}

	return &DataService{
		ctx: ctx,
		svr: s,
	}, nil
}

func (s *DataService) Run() error {
	if err := s.svr.Run(); err != nil {
		return err
	}
	return nil
}

func (s *DataService) Stop() error {
	if err := s.svr.Stop(); err != nil {
		return err
	}
	return nil
}
