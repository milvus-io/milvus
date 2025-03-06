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
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type CommonSuite struct {
	MockSuiteBase
}

func (s *CommonSuite) TestRetryIfSchemaError() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Run("normal_no_error", func() {
		counter := atomic.Int32{}
		err := s.client.retryIfSchemaError(ctx, "test_coll", func(ctx context.Context) (uint64, error) {
			counter.Add(1)
			return 10, nil
		})
		s.NoError(err)
		s.EqualValues(1, counter.Load())
	})

	s.Run("other_error", func() {
		counter := atomic.Int32{}
		err := s.client.retryIfSchemaError(ctx, "test_coll", func(ctx context.Context) (uint64, error) {
			counter.Add(1)
			return 10, merr.WrapErrServiceInternal("mocked")
		})
		s.Error(err)
		s.EqualValues(1, counter.Load())
	})

	s.Run("transient_schema_err", func() {
		counter := atomic.Int32{}
		err := s.client.retryIfSchemaError(ctx, "test_coll", func(ctx context.Context) (uint64, error) {
			epoch := counter.Load()
			counter.Add(1)
			if epoch == 0 {
				return 10, merr.WrapErrCollectionSchemaMisMatch("mocked")
			}
			return 11, nil
		})
		s.NoError(err)
		s.EqualValues(2, counter.Load())
	})

	s.Run("consistent_schema_err", func() {
		counter := atomic.Int32{}
		err := s.client.retryIfSchemaError(ctx, "test_coll", func(ctx context.Context) (uint64, error) {
			counter.Add(1)
			return 10, merr.WrapErrCollectionSchemaMisMatch("mocked")
		})
		s.Error(err)
		s.EqualValues(2, counter.Load())
	})
}

func TestCommonFunc(t *testing.T) {
	suite.Run(t, new(CommonSuite))
}
