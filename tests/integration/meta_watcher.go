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

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sort"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
)

// MetaWatcher to observe meta data of milvus cluster
type MetaWatcher interface {
	ShowSessions() ([]*sessionutil.SessionRaw, error)
	ShowSegments() ([]*datapb.SegmentInfo, error)
	ShowReplicas() ([]*querypb.Replica, error)
}

type EtcdMetaWatcher struct {
	MetaWatcher
	rootPath string
	etcdCli  *clientv3.Client
}

func (watcher *EtcdMetaWatcher) ShowSessions() ([]*sessionutil.SessionRaw, error) {
	metaPath := watcher.rootPath + "/meta/session"
	return listSessionsByPrefix(watcher.etcdCli, metaPath)
}

func (watcher *EtcdMetaWatcher) ShowSegments() ([]*datapb.SegmentInfo, error) {
	metaBasePath := path.Join(watcher.rootPath, "/meta/datacoord-meta/s/") + "/"
	return listSegments(watcher.etcdCli, watcher.rootPath, metaBasePath, func(s *datapb.SegmentInfo) bool {
		return true
	})
}

func (watcher *EtcdMetaWatcher) ShowReplicas() ([]*querypb.Replica, error) {
	metaBasePath := path.Join(watcher.rootPath, "/meta/querycoord-replica/")
	return listReplicas(watcher.etcdCli, metaBasePath)
}

//=================== Below largely copied from birdwatcher ========================

// listSessions returns all session
func listSessionsByPrefix(cli *clientv3.Client, prefix string) ([]*sessionutil.SessionRaw, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	sessions := make([]*sessionutil.SessionRaw, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		session := &sessionutil.SessionRaw{}
		err := json.Unmarshal(kv.Value, session)
		if err != nil {
			continue
		}

		sessions = append(sessions, session)
	}
	return sessions, nil
}

func listSegments(cli *clientv3.Client, rootPath string, prefix string, filter func(*datapb.SegmentInfo) bool) ([]*datapb.SegmentInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	segments := make([]*datapb.SegmentInfo, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		info := &datapb.SegmentInfo{}
		err = proto.Unmarshal(kv.Value, info)
		if err != nil {
			continue
		}
		if filter == nil || filter(info) {
			segments = append(segments, info)
		}
	}

	sort.Slice(segments, func(i, j int) bool {
		return segments[i].GetID() < segments[j].GetID()
	})

	for _, segment := range segments {
		segment.Binlogs, segment.Deltalogs, segment.Statslogs, err = getSegmentBinlogs(cli, rootPath, segment)
		if err != nil {
			return nil, err
		}
	}

	return segments, nil
}

func getSegmentBinlogs(cli *clientv3.Client, rootPath string, segment *datapb.SegmentInfo) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {
	fn := func(prefix string) ([]*datapb.FieldBinlog, error) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
		defer cancel()
		resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
		if err != nil {
			return nil, err
		}
		fieldBinlogs := make([]*datapb.FieldBinlog, 0, len(resp.Kvs))
		for _, kv := range resp.Kvs {
			info := &datapb.FieldBinlog{}
			err = proto.Unmarshal(kv.Value, info)
			if err != nil {
				return nil, err
			}
			fieldBinlogs = append(fieldBinlogs, info)
		}
		return fieldBinlogs, nil
	}
	prefix := path.Join(rootPath, "/meta/datacoord-meta", fmt.Sprintf("binlog/%d/%d/%d", segment.CollectionID, segment.PartitionID, segment.ID))
	binlogs, err := fn(prefix)
	if err != nil {
		return nil, nil, nil, err
	}

	prefix = path.Join(rootPath, "/meta/datacoord-meta", fmt.Sprintf("deltalog/%d/%d/%d", segment.CollectionID, segment.PartitionID, segment.ID))
	deltalogs, err := fn(prefix)
	if err != nil {
		return nil, nil, nil, err
	}

	prefix = path.Join(rootPath, "/meta/datacoord-meta", fmt.Sprintf("statslog/%d/%d/%d", segment.CollectionID, segment.PartitionID, segment.ID))
	statslogs, err := fn(prefix)
	if err != nil {
		return nil, nil, nil, err
	}

	return binlogs, deltalogs, statslogs, nil
}

func listReplicas(cli *clientv3.Client, prefix string) ([]*querypb.Replica, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()
	resp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	replicas := make([]*querypb.Replica, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		replica := &querypb.Replica{}
		if err := proto.Unmarshal(kv.Value, replica); err != nil {
			log.Warn("failed to unmarshal replica info", zap.Error(err))
			continue
		}
		replicas = append(replicas, replica)
	}

	return replicas, nil
}

func PrettyReplica(replica *querypb.Replica) string {
	res := fmt.Sprintf("ReplicaID: %d CollectionID: %d\n", replica.ID, replica.CollectionID)
	res = res + fmt.Sprintf("Nodes:%v\n", replica.Nodes)
	return res
}
