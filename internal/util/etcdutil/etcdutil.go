// Copyright 2016 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package etcdutil

import (
	"context"
	"crypto/tls"
	"net/http"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/log"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver"
	"go.etcd.io/etcd/pkg/types"
	"go.uber.org/zap"
)

const (
	// DefaultDialTimeout is the maximum amount of time a dial will wait for a
	// connection to setup. 30s is long enough for most of the network conditions.
	DefaultDialTimeout = 30 * time.Second

	// DefaultRequestTimeout 10s is long enough for most of etcd clusters.
	DefaultRequestTimeout = 10 * time.Second

	// DefaultSlowRequestTime 1s for the threshold for normal request, for those
	// longer then 1s, they are considered as slow requests.
	DefaultSlowRequestTime = 1 * time.Second
)

// CheckClusterID checks Etcd's cluster ID, returns an error if mismatch.
// This function will never block even quorum is not satisfied.
func CheckClusterID(localClusterID types.ID, um types.URLsMap, tlsConfig *tls.Config) error {
	if len(um) == 0 {
		return nil
	}

	var peerURLs []string
	for _, urls := range um {
		peerURLs = append(peerURLs, urls.StringSlice()...)
	}

	for _, u := range peerURLs {
		trp := &http.Transport{
			TLSClientConfig: tlsConfig,
		}
		remoteCluster, gerr := etcdserver.GetClusterFromRemotePeers(nil, []string{u}, trp)
		trp.CloseIdleConnections()
		if gerr != nil {
			// Do not return error, because other members may be not ready.
			log.Error("failed to get cluster from remote")
			continue
		}

		remoteClusterID := remoteCluster.ID()
		if remoteClusterID != localClusterID {
			return errors.Errorf("Etcd cluster ID mismatch, expect %d, got %d", localClusterID, remoteClusterID)
		}
	}
	return nil
}

// AddEtcdMember adds an etcd member.
func AddEtcdMember(client *clientv3.Client, urls []string) (*clientv3.MemberAddResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	addResp, err := client.MemberAdd(ctx, urls)
	cancel()
	return addResp, errors.WithStack(err)
}

// ListEtcdMembers returns a list of internal etcd members.
func ListEtcdMembers(client *clientv3.Client) (*clientv3.MemberListResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	listResp, err := client.MemberList(ctx)
	cancel()
	return listResp, errors.WithStack(err)
}

// RemoveEtcdMember removes a member by the given id.
func RemoveEtcdMember(client *clientv3.Client, id uint64) (*clientv3.MemberRemoveResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	rmResp, err := client.MemberRemove(ctx, id)
	cancel()
	return rmResp, errors.WithStack(err)
}

// EtcdKVGet returns the etcd GetResponse by given key or key prefix
func EtcdKVGet(c *clientv3.Client, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(c.Ctx(), DefaultRequestTimeout)
	defer cancel()

	start := time.Now()
	resp, err := clientv3.NewKV(c).Get(ctx, key, opts...)
	if err != nil {
		log.Error("load from etcd meet error")
	}
	if cost := time.Since(start); cost > DefaultSlowRequestTime {
		log.Warn("kv gets too slow", zap.String("request-key", key), zap.Duration("cost", cost), zap.Error(err))
	}

	return resp, errors.WithStack(err)
}

// GetValue gets value with key from etcd.
func GetValue(c *clientv3.Client, key string, opts ...clientv3.OpOption) ([]byte, error) {
	resp, err := get(c, key, opts...)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, nil
	}
	return resp.Kvs[0].Value, nil
}

func get(c *clientv3.Client, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	resp, err := EtcdKVGet(c, key, opts...)
	if err != nil {
		return nil, err
	}

	if n := len(resp.Kvs); n == 0 {
		return nil, nil
	} else if n > 1 {
		return nil, errors.Errorf("invalid get value resp %v, must only one", resp.Kvs)
	}
	return resp, nil
}

// GetProtoMsgWithModRev returns boolean to indicate whether the key exists or not.
func GetProtoMsgWithModRev(c *clientv3.Client, key string, msg proto.Message, opts ...clientv3.OpOption) (bool, int64, error) {
	resp, err := get(c, key, opts...)
	if err != nil {
		return false, 0, err
	}
	if resp == nil {
		return false, 0, nil
	}
	value := resp.Kvs[0].Value
	if err = proto.Unmarshal(value, msg); err != nil {
		return false, 0, errors.WithStack(err)
	}
	return true, resp.Kvs[0].ModRevision, nil
}
