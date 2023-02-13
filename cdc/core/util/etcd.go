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

package util

import (
	"context"
	"path"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	EtcdOpTimeout   = 5 * time.Second
	EtcdOpRetryTime = 5
)

type KVApi interface {
	clientv3.KV
	clientv3.Watcher
	// Status From clientv3.Maintenance interface
	Status(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error)
	Endpoints() []string
}

// retry times
func GetEtcdClient(endpoints []string) (KVApi, error) {
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
		Logger:      Log,
	})
	if err != nil {
		Log.Warn("fail to etcd client", zap.Error(err))
	}
	err = EtcdStatus(etcdCli)
	if err != nil {
		Log.Warn("unavailable etcd server, please check it", zap.Error(err))
		return nil, err
	}
	return etcdCli, err
}

func GetCollectionPrefix(rootPath string, metaSubPath string, collectionKey string) string {
	return path.Join(rootPath, metaSubPath, collectionKey)
}

func GetFieldPrefix(rootPath string, metaSubPath string, fieldKey string) string {
	return path.Join(rootPath, metaSubPath, fieldKey)
}

func EtcdPut(etcdCli KVApi, key, val string, opts ...clientv3.OpOption) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdOpTimeout)
	defer cancel()
	return Do(ctx, func() error {
		_, err := etcdCli.Put(ctx, key, val, opts...)
		return err
	}, Attempts(EtcdOpRetryTime))
}

func EtcdGet(etcdCli KVApi, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdOpTimeout)
	defer cancel()
	var (
		resp *clientv3.GetResponse
		err  error
	)

	err = Do(ctx, func() error {
		resp, err = etcdCli.Get(ctx, key, opts...)
		return err
	}, Attempts(EtcdOpRetryTime))
	return resp, err
}

func EtcdDelete(etcdCli KVApi, key string, opts ...clientv3.OpOption) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdOpTimeout)
	defer cancel()

	return Do(ctx, func() error {
		_, err := etcdCli.Delete(ctx, key, opts...)
		return err
	}, Attempts(EtcdOpRetryTime))
}

func EtcdTxn(etcdCli KVApi, fun func(txn clientv3.Txn) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdOpTimeout)
	defer cancel()

	return Do(ctx, func() error {
		etcdTxn := etcdCli.Txn(ctx)
		err := fun(etcdTxn)
		return err
	}, Attempts(EtcdOpRetryTime))
}

func EtcdStatus(etcdCli KVApi) error {
	ctx, cancel := context.WithTimeout(context.Background(), EtcdOpTimeout)
	defer cancel()
	for _, endpoint := range etcdCli.Endpoints() {
		_, err := etcdCli.Status(ctx, endpoint)
		if err != nil {
			return err
		}
	}
	return nil
}
