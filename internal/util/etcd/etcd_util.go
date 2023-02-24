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
	"io/ioutil"
	"net/url"
	"os"
	"time"

	"github.com/cockroachdb/errors"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

var (
	maxTxnNum = 128
)

// GetEtcdClient returns etcd client
func GetEtcdClient(
	useEmbedEtcd bool,
	useSSL bool,
	endpoints []string,
	certFile string,
	keyFile string,
	caCertFile string,
	minVersion string) (*clientv3.Client, error) {
	if useEmbedEtcd {
		return GetEmbedEtcdClient()
	}
	if useSSL {
		return GetRemoteEtcdSSLClient(endpoints, certFile, keyFile, caCertFile, minVersion)
	}
	return GetRemoteEtcdClient(endpoints)
}

// GetRemoteEtcdClient returns client of remote etcd by given endpoints
func GetRemoteEtcdClient(endpoints []string) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
}

func GetRemoteEtcdSSLClient(endpoints []string, certFile string, keyFile string, caCertFile string, minVersion string) (*clientv3.Client, error) {
	var cfg clientv3.Config
	cfg.Endpoints = endpoints
	cfg.DialTimeout = 5 * time.Second
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, errors.Wrap(err, "load etcd cert key pair error")
	}
	caCert, err := ioutil.ReadFile(caCertFile)
	if err != nil {
		return nil, errors.Wrapf(err, "load etcd CACert file error, filename = %s", caCertFile)
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
	switch minVersion {
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
		return nil, errors.Errorf("unknown TLS version,%s", minVersion)
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

// SaveByBatch there will not guarantee atomicity.
func SaveByBatch(kvs map[string]string, op func(partialKvs map[string]string) error) error {
	return SaveByBatchWithLimit(kvs, maxTxnNum, op)
}

func RemoveByBatch(removals []string, op func(partialKeys []string) error) error {
	if len(removals) == 0 {
		return nil
	}

	for i := 0; i < len(removals); i = i + maxTxnNum {
		end := min(i+maxTxnNum, len(removals))
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
	dir, err := ioutil.TempDir(os.TempDir(), "milvus_datanode_ut")
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
