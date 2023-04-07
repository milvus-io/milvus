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

package etcd

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/registry/common"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
)

type RawEntryProviderSuite struct {
	suite.Suite
}

func (s *RawEntryProviderSuite) TestSuccessRun() {
	kv := NewMockV3KV(s.T())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	session := &etcdSession{
		ServiceEntryBase: common.NewServiceEntryBase("addr", "role"),
		stopping:         atomic.NewBool(false),
	}
	session.SetID(100)
	bs, err := json.Marshal(session)
	s.NoError(err)
	kv.EXPECT().Get(ctx, mock.AnythingOfType("string")).Return(&clientv3.GetResponse{
		Kvs: []*mvccpb.KeyValue{
			{
				Key:   []byte("meta/session/role"),
				Value: bs,
			},
		},
	}, nil)
	rp := NewRawEntryProvider(kv, "meta", "role")
	addr, id, err := rp.GetServiceEntry(ctx)
	s.NoError(err)
	s.Equal(session.Addr(), addr)
	s.Equal(session.ID(), id)
}

func (s *RawEntryProviderSuite) TestGetError() {
	kv := NewMockV3KV(s.T())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mockedErr := errors.New("mocked")
	kv.EXPECT().Get(ctx, mock.AnythingOfType("string")).Return(nil, mockedErr)
	rp := NewRawEntryProvider(kv, "meta", "role")
	_, _, err := rp.GetServiceEntry(ctx)
	s.Error(err)
	s.True(merr.ErrIoFailed.Is(err))
}

func (s *RawEntryProviderSuite) TestKeyNotFound() {
	kv := NewMockV3KV(s.T())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kv.EXPECT().Get(ctx, mock.AnythingOfType("string")).Return(&clientv3.GetResponse{}, nil)
	rp := NewRawEntryProvider(kv, "meta", "role")

	_, _, err := rp.GetServiceEntry(ctx)
	s.Error(err)
	s.True(merr.ErrIoKeyNotFound.Is(err))
}

func (s *RawEntryProviderSuite) TestUnmarshalFailed() {
	kv := NewMockV3KV(s.T())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kv.EXPECT().Get(ctx, mock.AnythingOfType("string")).Return(&clientv3.GetResponse{
		Kvs: []*mvccpb.KeyValue{
			{
				Key:   []byte("meta/session/role"),
				Value: nil,
			},
		},
	}, nil)
	rp := NewRawEntryProvider(kv, "meta", "role")

	_, _, err := rp.GetServiceEntry(ctx)
	s.Error(err)
	s.True(merr.ErrIoKeyNotFound.Is(err))
}

func TestRawEntryProvider(t *testing.T) {
	suite.Run(t, new(RawEntryProviderSuite))
}
