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

package components

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// IndexCoord implements IndexCoord grpc server
type IndexCoord struct{}

// NewIndexCoord creates a new IndexCoord
func NewIndexCoord(ctx context.Context, factory dependency.Factory) (*IndexCoord, error) {
	return &IndexCoord{}, nil
}

func (s *IndexCoord) Prepare() error {
	return nil
}

// Run starts service
func (s *IndexCoord) Run() error {
	log.Ctx(context.TODO()).Info("IndexCoord running ...")
	return nil
}

// Stop terminates service
func (s *IndexCoord) Stop() error {
	log.Ctx(context.TODO()).Info("IndexCoord stopping ...")
	return nil
}

// GetComponentStates returns indexnode's states
func (s *IndexCoord) Health(ctx context.Context) commonpb.StateCode {
	return commonpb.StateCode_Healthy
}

func (s *IndexCoord) GetName() string {
	return typeutil.IndexCoordRole
}
