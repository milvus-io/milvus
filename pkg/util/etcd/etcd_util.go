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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util"
)

// GetEtcdClient returns etcd client
// should only used for test
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
		DialOptions: []grpc.DialOption{
			grpc.WithBlock(),
		},
	})
}

func GetRemoteEtcdClientWithAuth(endpoints []string, userName, password string) (*clientv3.Client, error) {
	return clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
		Username:    userName,
		Password:    password,
		DialOptions: []grpc.DialOption{
			grpc.WithBlock(),
		},
	})
}

func GetRemoteEtcdSSLClient(endpoints []string, certFile string, keyFile string, caCertFile string, minVersion string) (*clientv3.Client, error) {
	var cfg clientv3.Config
	return GetRemoteEtcdSSLClientWithCfg(endpoints, certFile, keyFile, caCertFile, minVersion, cfg)
}

func GetRemoteEtcdSSLClientWithCfg(endpoints []string, certFile string, keyFile string, caCertFile string, minVersion string, cfg clientv3.Config) (*clientv3.Client, error) {
	cfg.Endpoints = endpoints
	cfg.DialTimeout = 5 * time.Second
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, errors.Wrap(err, "load etcd cert key pair error")
	}
	caCert, err := os.ReadFile(caCertFile)
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

	cfg.DialOptions = append(cfg.DialOptions, grpc.WithBlock())

	return clientv3.New(cfg)
}

func CreateEtcdClient(
	useEmbedEtcd bool,
	enableAuth bool,
	userName,
	password string,
	useSSL bool,
	endpoints []string,
	certFile string,
	keyFile string,
	caCertFile string,
	minVersion string,
) (*clientv3.Client, error) {
	if !enableAuth || useEmbedEtcd {
		return GetEtcdClient(useEmbedEtcd, useSSL, endpoints, certFile, keyFile, caCertFile, minVersion)
	}
	log.Info("create etcd client(enable auth)",
		zap.Bool("useSSL", useSSL),
		zap.Any("endpoints", endpoints),
		zap.String("minVersion", minVersion))
	if useSSL {
		return GetRemoteEtcdSSLClientWithCfg(endpoints, certFile, keyFile, caCertFile, minVersion, clientv3.Config{Username: userName, Password: password})
	}
	return GetRemoteEtcdClientWithAuth(endpoints, userName, password)
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

func HealthCheck(useEmbedEtcd bool,
	enableAuth bool,
	userName,
	password string,
	useSSL bool,
	endpoints []string,
	certFile string,
	keyFile string,
	caCertFile string,
	minVersion string,
) common.MetaClusterStatus {
	var healthList []common.EPHealth
	clusterStatus := common.MetaClusterStatus{MetaType: util.MetaStoreTypeEtcd}

	client, err := CreateEtcdClient(useEmbedEtcd, enableAuth, userName, password, useSSL, endpoints, certFile, keyFile, caCertFile, minVersion)
	if err != nil {
		clusterStatus.Reason = fmt.Sprintf("establish client connection failed, err: %v", err)
		return clusterStatus
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	defer client.Close()
	resp, err := client.MemberList(ctx)
	if err != nil {
		clusterStatus.Reason = fmt.Sprintf("got member list failed, err: %v", err)
		return clusterStatus
	}

	var wg sync.WaitGroup
	hch := make(chan common.EPHealth, len(resp.Members))
	for _, member := range resp.Members {
		wg.Add(1)
		member := member
		go func() {
			defer wg.Done()
			ep := member.ClientURLs[0]
			client, err := CreateEtcdClient(useEmbedEtcd, enableAuth, userName, password, useSSL, member.ClientURLs, certFile, keyFile, caCertFile, minVersion)
			if err != nil {
				hch <- common.EPHealth{EP: ep, Health: false, Reason: err.Error()}
				return
			}

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			defer client.Close()

			_, err = client.Get(ctx, "health")
			eh := common.EPHealth{EP: ep, Health: false}

			// permission denied is OK since proposal goes through consensus to get it
			if err == nil || errors.Is(err, rpctypes.ErrPermissionDenied) {
				eh.Health = true
			} else {
				eh.Reason = err.Error()
			}

			if eh.Health {
				resp, err := client.AlarmList(ctx)
				if err == nil && len(resp.Alarms) > 0 {
					eh.Health = false
					eh.Reason = "Active Alarm(s): "
					for _, v := range resp.Alarms {
						switch v.Alarm {
						case etcdserverpb.AlarmType_NOSPACE:
							eh.Reason = eh.Reason + "NOSPACE "
						case etcdserverpb.AlarmType_CORRUPT:
							eh.Reason = eh.Reason + "CORRUPT "
						default:
							eh.Reason = eh.Reason + "UNKNOWN "
						}
					}
				} else if err != nil {
					eh.Health = false
					eh.Reason = "Unable to fetch the alarm list"
				}
			}
			hch <- eh
		}()
	}

	wg.Wait()
	close(hch)

	var hasUnhealthyEP bool
	for h := range hch {
		healthList = append(healthList, h)
		if !h.Health {
			hasUnhealthyEP = true
		}
	}

	if hasUnhealthyEP {
		clusterStatus.Reason = "some members are unavailable"
	} else {
		clusterStatus.Health = true
	}
	clusterStatus.Members = healthList
	return clusterStatus
}
