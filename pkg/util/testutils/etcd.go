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

package testutils

import (
	"os"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/etcd"
)

type EmbedEtcdUtil struct {
	server  *embed.Etcd
	tempDir string
}

func (util *EmbedEtcdUtil) SetupEtcd() ([]string, error) {
	// init embed etcd
	embedetcdServer, tempDir, err := etcd.StartTestEmbedEtcdServer()
	if err != nil {
		return nil, err
	}
	util.server, util.tempDir = embedetcdServer, tempDir

	return etcd.GetEmbedEtcdEndpoints(embedetcdServer), nil
}

func (util *EmbedEtcdUtil) TearDownEmbedEtcd() {
	if util.server != nil {
		util.server.Close()
	}
	if util.tempDir != "" {
		os.RemoveAll(util.tempDir)
	}
}

// GetEtcdClient returns etcd client
// Note: should only used for test
func GetEtcdClient(
	useEmbedEtcd bool,
	useSSL bool,
	endpoints []string,
	certFile string,
	keyFile string,
	caCertFile string,
	minVersion string,
) (*clientv3.Client, error) {
	log.Info("create etcd client",
		zap.Bool("useEmbedEtcd", useEmbedEtcd),
		zap.Bool("useSSL", useSSL),
		zap.Any("endpoints", endpoints),
		zap.String("minVersion", minVersion))
	if useEmbedEtcd {
		return etcd.GetEmbedEtcdClient()
	}

	var cfg clientv3.Config
	var err error
	if useSSL {
		cfg, err = etcd.GetSSLCfg(&etcd.EtcdCfg{
			Endpoints:  endpoints,
			CertFile:   certFile,
			KeyFile:    keyFile,
			CaCertFile: caCertFile,
			MinVersion: minVersion,
		})
	} else {
		cfg = etcd.GetBasicClientCfg(&etcd.EtcdCfg{Endpoints: endpoints})
	}

	if err != nil {
		return nil, err
	}
	return clientv3.New(cfg)
}
