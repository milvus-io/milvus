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

package config

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v2/util/etcd"
)

type EtcdSourceSuite struct {
	suite.Suite

	embedEtcdServer *embed.Etcd
	tempDir         string
	endpoints       []string
}

func (s *EtcdSourceSuite) SetupSuite() {
	// init embed etcd
	embedServer, tempDir, err := etcd.StartTestEmbedEtcdServer()

	s.Require().NoError(err)

	s.embedEtcdServer = embedServer
	s.tempDir = tempDir
	s.endpoints = etcd.GetEmbedEtcdEndpoints(embedServer)
}

func (s *EtcdSourceSuite) TearDownSuite() {
	if s.embedEtcdServer != nil {
		s.embedEtcdServer.Close()
	}
	if s.tempDir != "" {
		os.RemoveAll(s.tempDir)
	}
}

func (s *EtcdSourceSuite) TestNewSource() {
	source, err := NewEtcdSource(&EtcdInfo{
		Endpoints:       s.endpoints,
		KeyPrefix:       "by-dev",
		RefreshInterval: time.Second,
	})
	s.NoError(err)
	s.NotNil(source)
	source.Close()
}

func (s *EtcdSourceSuite) TestUpdateOptions() {
	source, err := NewEtcdSource(&EtcdInfo{
		Endpoints:       s.endpoints,
		KeyPrefix:       "test_update_options_1",
		RefreshInterval: time.Second,
	})
	s.Require().NoError(err)
	s.Require().NotNil(source)
	defer source.Close()

	called := atomic.NewBool(false)

	handler := NewHandler("test_update_options", func(evt *Event) {
		called.Store(true)
	})

	source.SetEventHandler(handler)

	source.UpdateOptions(Options{
		EtcdInfo: &EtcdInfo{
			Endpoints:       s.endpoints,
			KeyPrefix:       "test_update_options_2",
			RefreshInterval: time.Millisecond * 100,
		},
	})

	client, err := etcd.GetRemoteEtcdClient(s.endpoints)
	s.Require().NoError(err)
	client.Put(context.Background(), "test_update_options_2/config/abc", "def")

	s.Eventually(func() bool {
		return called.Load()
	}, time.Second*2, time.Millisecond*100)
}

func TestEtcdSource(t *testing.T) {
	suite.Run(t, new(EtcdSourceSuite))
}
