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

package etcdkv

import (
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

// NewMetaKvFactory returns an object that implements the kv.MetaKv interface using etcd.
// The UseEmbedEtcd in the param is used to determine whether the etcd service is external or embedded.
func NewMetaKvFactory(rootPath string, param *paramtable.BaseParamTable) (kv.MetaKv, error) {
	log.Info("start etcd with rootPath",
		zap.String("rootpath", rootPath),
		zap.Bool("isEmbed", param.UseEmbedEtcd))
	if param.UseEmbedEtcd {
		path := param.EtcdConfigPath
		var cfg *embed.Config
		if len(path) > 0 {
			cfgFromFile, err := embed.ConfigFromFile(path)
			if err != nil {
				return nil, err
			}
			cfg = cfgFromFile
		} else {
			cfg = embed.NewConfig()
		}
		cfg.Dir = param.EtcdDataDir
		metaKv, err := NewEmbededEtcdKV(cfg, rootPath)
		if err != nil {
			return nil, err
		}
		return metaKv, err
	}
	metaKv, err := NewEtcdKV(param.EtcdEndpoints, rootPath)
	if err != nil {
		return nil, err
	}
	return metaKv, err
}
