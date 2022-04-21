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

package etcdkv

import (
	"testing"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

// NewMetaKvFactory returns an object that implements the kv.MetaKv interface using etcd.
// The UseEmbedEtcd in the param is used to determine whether the etcd service is external or embedded.
func NewMetaKvFactory(rootPath string, etcdCfg *paramtable.EtcdConfig, t *testing.T) kv.MetaKv {
	log.Info("start etcd with rootPath",
		zap.String("rootpath", rootPath),
		zap.Bool("isEmbed", etcdCfg.UseEmbedEtcd))
	if etcdCfg.UseEmbedEtcd {
		path := etcdCfg.ConfigPath
		var cfg *embed.Config
		if len(path) > 0 {
			cfgFromFile, err := embed.ConfigFromFile(path)
			assert.NoError(t, err)
			cfg = cfgFromFile
		} else {
			cfg = embed.NewConfig()
		}
		cfg.Dir = etcdCfg.DataDir
		metaKv, err := NewEmbededEtcdKV(cfg, rootPath)
		assert.NoError(t, err)
		return metaKv
	}
	client := etcd.GetEtcdTestClient(t)
	metaKv := NewEtcdKV(client, rootPath)
	return metaKv
}
