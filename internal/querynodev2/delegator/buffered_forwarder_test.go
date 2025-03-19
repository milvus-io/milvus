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

package delegator

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/storage"
)

type BufferForwarderSuite struct {
	suite.Suite
}

func (s *BufferForwarderSuite) TestNormalSync() {
	s.Run("large_buffer", func() {
		counter := atomic.NewInt64(0)
		buf := NewBufferedForwarder(16*1024*1024, func(pks storage.PrimaryKeys, tss []uint64) error {
			counter.Add(1)
			return nil
		})

		err := buf.Buffer(storage.NewInt64PrimaryKey(100), 100)
		s.NoError(err)
		err = buf.Flush()
		s.NoError(err)
		s.EqualValues(1, counter.Load())
	})

	s.Run("small_buffer", func() {
		counter := atomic.NewInt64(0)
		buf := NewBufferedForwarder(1, func(pks storage.PrimaryKeys, tss []uint64) error {
			counter.Add(1)
			return nil
		})

		err := buf.Buffer(storage.NewInt64PrimaryKey(100), 100)
		s.NoError(err)
		err = buf.Flush()
		s.NoError(err)
		s.EqualValues(1, counter.Load())
	})
}

func (s *BufferForwarderSuite) TestSyncFailure() {
	s.Run("large_buffer", func() {
		buf := NewBufferedForwarder(16*1024*1024, func(pks storage.PrimaryKeys, tss []uint64) error {
			return errors.New("mocked")
		})

		err := buf.Buffer(storage.NewInt64PrimaryKey(100), 100)
		s.NoError(err)
		err = buf.Flush()
		s.Error(err)
	})

	s.Run("small_buffer", func() {
		buf := NewBufferedForwarder(1, func(pks storage.PrimaryKeys, tss []uint64) error {
			return errors.New("mocked")
		})

		err := buf.Buffer(storage.NewInt64PrimaryKey(100), 100)
		s.Error(err)
	})
}

func TestBufferedForwarder(t *testing.T) {
	suite.Run(t, new(BufferForwarderSuite))
}
