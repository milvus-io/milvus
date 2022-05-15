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
	"io/ioutil"
	"time"

	"github.com/pkg/errors"

	"github.com/milvus-io/milvus/internal/util/paramtable"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// GetEtcdClient returns etcd client
func GetEtcdClient(cfg *paramtable.EtcdConfig) (*clientv3.Client, error) {
	if cfg.UseEmbedEtcd {
		return GetEmbedEtcdClient()
	}
	if cfg.EtcdUseSSL {
		return GetRemoteEtcdSSLClient(cfg.Endpoints, cfg.EtcdTLSCert, cfg.EtcdTLSKey, cfg.EtcdTLSCACert, cfg.EtcdTLSMinVersion)
	}
	return GetRemoteEtcdClient(cfg.Endpoints)
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
