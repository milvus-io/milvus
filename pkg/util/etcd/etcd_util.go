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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"time"

	"github.com/cockroachdb/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/log"
)

type EtcdCfg struct {
	UseEmbed           bool
	EnableAuth         bool
	UserName           string
	PassWord           string
	UseSSL             bool
	Endpoints          []string
	CertFile           string
	KeyFile            string
	CaCertFile         string
	MinVersion         string
	KeyPrefix          string
	MaxRPCSendMsgBytes int
	MaxRPCRecvMsgBytes int
}

func GetBasicClientCfg(etcdCfg *EtcdCfg) clientv3.Config {
	cfg := clientv3.Config{
		Endpoints:   etcdCfg.Endpoints,
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{
			grpc.WithBlock(),
		},
	}
	if etcdCfg.MaxRPCSendMsgBytes > 0 {
		cfg.MaxCallSendMsgSize = etcdCfg.MaxRPCSendMsgBytes
	}
	if etcdCfg.MaxRPCRecvMsgBytes > 0 {
		cfg.MaxCallRecvMsgSize = etcdCfg.MaxRPCRecvMsgBytes
	}
	return cfg
}

func GetAuthCfg(etcdCfg *EtcdCfg) clientv3.Config {
	cfg := GetBasicClientCfg(etcdCfg)
	cfg.Username = etcdCfg.UserName
	cfg.Password = etcdCfg.PassWord
	return cfg
}

func GetSSLCfg(etcdCfg *EtcdCfg) (clientv3.Config, error) {
	cfg := GetBasicClientCfg(etcdCfg)
	cert, err := tls.LoadX509KeyPair(etcdCfg.CertFile, etcdCfg.KeyFile)
	if err != nil {
		return clientv3.Config{}, errors.Wrap(err, "load etcd cert key pair error")
	}
	caCert, err := os.ReadFile(etcdCfg.CaCertFile)
	if err != nil {
		return clientv3.Config{}, errors.Wrapf(err, "load etcd CACert file error, filename = %s", etcdCfg.CaCertFile)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	cfg.TLS = &tls.Config{
		MinVersion: tls.VersionTLS13,
		Certificates: []tls.Certificate{
			cert,
		},
		RootCAs: caCertPool,
	}
	switch etcdCfg.MinVersion {
	case "1.0":
		cfg.TLS.MinVersion = tls.VersionTLS10
	case "1.1":
		cfg.TLS.MinVersion = tls.VersionTLS11
	case "1.2":
		cfg.TLS.MinVersion = tls.VersionTLS12
	case "1.3":
		cfg.TLS.MinVersion = tls.VersionTLS13
	default:
		cfg.TLS.MinVersion = 0
	}

	if cfg.TLS.MinVersion == 0 {
		return clientv3.Config{}, errors.Errorf("unknown TLS version,%s", etcdCfg.MinVersion)
	}
	return cfg, nil
}

// GetRemoteEtcdClient returns client of remote etcd by given endpoints
func GetRemoteEtcdClient(endpoints []string) (*clientv3.Client, error) {
	return clientv3.New(GetBasicClientCfg(&EtcdCfg{Endpoints: endpoints}))
}

func CreateEtcdClient(etcdCfg *EtcdCfg) (*clientv3.Client, error) {
	log.Info("create etcd client, ", zap.Any("etcdCfg", etcdCfg))
	if etcdCfg.UseEmbed {
		return GetEmbedEtcdClient()
	}

	var cfg clientv3.Config
	var err error
	if etcdCfg.UseSSL {
		cfg, err = GetSSLCfg(etcdCfg)
	} else if etcdCfg.EnableAuth {
		cfg = GetAuthCfg(etcdCfg)
	} else {
		cfg = GetBasicClientCfg(etcdCfg)
	}

	if err != nil {
		return nil, err
	}
	return clientv3.New(cfg)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// SaveByBatchWithLimit is SaveByBatch with customized limit.
func SaveByBatchWithLimit(kvs map[string]string, limit int, op func(partialKvs map[string]string) error) error {
	if len(kvs) == 0 {
		return nil
	}

	keys := make([]string, 0, len(kvs))
	values := make([]string, 0, len(kvs))

	for k, v := range kvs {
		keys = append(keys, k)
		values = append(values, v)
	}

	for i := 0; i < len(kvs); i = i + limit {
		end := min(i+limit, len(keys))
		batch, err := buildKvGroup(keys[i:end], values[i:end])
		if err != nil {
			return err
		}

		if err := op(batch); err != nil {
			return err
		}
	}
	return nil
}

func RemoveByBatchWithLimit(removals []string, limit int, op func(partialKeys []string) error) error {
	if len(removals) == 0 {
		return nil
	}

	for i := 0; i < len(removals); i = i + limit {
		end := min(i+limit, len(removals))
		batch := removals[i:end]
		if err := op(batch); err != nil {
			return err
		}
	}
	return nil
}

func buildKvGroup(keys, values []string) (map[string]string, error) {
	if len(keys) != len(values) {
		return nil, fmt.Errorf("length of keys (%d) and values (%d) are not equal", len(keys), len(values))
	}
	ret := make(map[string]string, len(keys))
	for i, k := range keys {
		_, ok := ret[k]
		if ok {
			return nil, fmt.Errorf("duplicated key was found: %s", k)
		}
		ret[k] = values[i]
	}
	return ret, nil
}

// StartTestEmbedEtcdServer returns a newly created embed etcd server.
// ### USED FOR UNIT TEST ONLY ###
func StartTestEmbedEtcdServer() (*embed.Etcd, string, error) {
	dir, err := os.MkdirTemp(os.TempDir(), "milvus_ut")
	if err != nil {
		return nil, "", err
	}
	config := embed.NewConfig()

	config.Dir = dir
	config.LogLevel = "warn"
	config.LogOutputs = []string{"default"}
	u, err := url.Parse("http://localhost:0")
	if err != nil {
		return nil, "", err
	}
	config.LCUrls = []url.URL{*u}
	u, err = url.Parse("http://localhost:0")
	if err != nil {
		return nil, "", err
	}
	config.LPUrls = []url.URL{*u}

	server, err := embed.StartEtcd(config)
	return server, dir, err
}

// GetEmbedEtcdEndpoints returns etcd listener address for endpoint config.
func GetEmbedEtcdEndpoints(server *embed.Etcd) []string {
	addrs := make([]string, 0, len(server.Clients))
	for _, l := range server.Clients {
		addrs = append(addrs, l.Addr().String())
	}
	return addrs
}
