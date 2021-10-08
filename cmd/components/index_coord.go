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

	"github.com/milvus-io/milvus/internal/proto/internalpb"

	grpcindexcoord "github.com/milvus-io/milvus/internal/distributed/indexcoord"
)

// IndexCoord implements IndexCoord grpc server
type IndexCoord struct {
	svr *grpcindexcoord.Server
}

// NewIndexCoord creates a new IndexCoord
func NewIndexCoord(ctx context.Context) (*IndexCoord, error) {
	var err error
	s := &IndexCoord{}
	svr, err := grpcindexcoord.NewServer(ctx)

	if err != nil {
		return nil, err
	}
	s.svr = svr
	return s, nil
}

// Run starts service
func (s *IndexCoord) Run() error {
	if err := s.svr.Run(); err != nil {
		return err
	}
	return nil
}

// Stop terminates service
func (s *IndexCoord) Stop() error {
	if err := s.svr.Stop(); err != nil {
		return err
	}
	return nil
}

// GetComponentStates returns indexnode's states
func (s *IndexCoord) GetComponentStates(ctx context.Context, request *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error) {
	return s.svr.GetComponentStates(ctx, request)
}
