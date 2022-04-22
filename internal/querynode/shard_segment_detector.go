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

package querynode

import (
	"context"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	v3rpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// etcdShardSegmentDetector watch etcd prefix for segment event.
type etcdShardSegmentDetector struct {
	client *clientv3.Client
	path   string
	evtCh  chan segmentEvent

	wg        sync.WaitGroup
	closeCh   chan struct{}
	closeOnce sync.Once
}

// NewEtcdShardSegmentDetector returns a segmentDetector with provided etcd client and root path.
func NewEtcdShardSegmentDetector(client *clientv3.Client, rootPath string) *etcdShardSegmentDetector {
	return &etcdShardSegmentDetector{
		client:  client,
		path:    rootPath,
		evtCh:   make(chan segmentEvent, 32),
		closeCh: make(chan struct{}),
	}
}

// Close perform closing procedure and notify all watcher to quit.
func (sd *etcdShardSegmentDetector) Close() {
	sd.closeOnce.Do(func() {
		close(sd.closeCh)
		sd.wg.Wait()
		close(sd.evtCh)
	})
}

func (sd *etcdShardSegmentDetector) afterClose(fn func()) {
	<-sd.closeCh
	fn()
}

func (sd *etcdShardSegmentDetector) getCtx() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go sd.afterClose(cancel)
	return ctx
}

func (sd *etcdShardSegmentDetector) watchSegments(collectionID int64, replicaID int64, vchannelName string) ([]segmentEvent, <-chan segmentEvent) {
	log.Debug("segmentDetector start watch", zap.Int64("collectionID", collectionID),
		zap.Int64("replicaID", replicaID),
		zap.String("vchannelName", vchannelName),
		zap.String("rootPath", sd.path))
	resp, err := sd.client.Get(context.Background(), sd.path, clientv3.WithPrefix())
	if err != nil {
		log.Warn("Etcd SegmentDetector get replica info failed", zap.Error(err))
		panic(err)
	}

	var events []segmentEvent
	for _, kv := range resp.Kvs {
		info, err := sd.parseSegmentInfo(kv.Value)
		if err != nil {
			log.Warn("SegmentDetector failed to parse segmentInfo", zap.Error(err))
			continue
		}
		if info.CollectionID != collectionID || info.GetDmChannel() != vchannelName {
			continue
		}

		if inList(info.GetReplicaIds(), replicaID) {
			events = append(events, segmentEvent{
				eventType:   segmentAdd,
				segmentID:   info.GetSegmentID(),
				partitionID: info.GetPartitionID(),
				nodeID:      info.GetNodeID(),
				state:       segmentStateLoaded,
			})
		}
	}

	sd.wg.Add(1)
	watchCh := sd.client.Watch(sd.getCtx(), sd.path, clientv3.WithRev(resp.Header.GetRevision()+1), clientv3.WithPrefix(), clientv3.WithPrevKV())
	go sd.watch(watchCh, collectionID, replicaID, vchannelName)

	return events, sd.evtCh
}

func (sd *etcdShardSegmentDetector) watch(ch clientv3.WatchChan, collectionID int64, replicaID int64, vchannel string) {
	defer sd.wg.Done()
	log.Debug("etcdSegmentDetector start watch")
	for {
		select {
		case <-sd.closeCh:
			log.Warn("Closed SegmentDetector watch loop quit", zap.Int64("collectionID", collectionID), zap.Int64("replicaID", replicaID))
			return
		case evt, ok := <-ch:
			if !ok {
				log.Warn("SegmentDetector event channel closed")
				return
			}
			if err := evt.Err(); err != nil {
				if err == v3rpc.ErrCompacted {
					sd.wg.Add(1)
					watchCh := sd.client.Watch(sd.getCtx(), sd.path, clientv3.WithPrefix())
					go sd.watch(watchCh, collectionID, replicaID, vchannel)
					return
				}
			}
			for _, e := range evt.Events {
				switch e.Type {
				case mvccpb.PUT:
					sd.handlePutEvent(e, collectionID, replicaID, vchannel)
				case mvccpb.DELETE:
					sd.handleDelEvent(e, collectionID, replicaID, vchannel)
				}
			}
		}
	}
}

func (sd *etcdShardSegmentDetector) handlePutEvent(e *clientv3.Event, collectionID int64, replicaID int64, vchannel string) {
	info, err := sd.parseSegmentInfo(e.Kv.Value)
	if err != nil {
		log.Warn("Segment detector failed to parse event", zap.Any("event", e), zap.Error(err))
		return
	}

	if info.GetCollectionID() != collectionID || vchannel != info.GetDmChannel() || !inList(info.GetReplicaIds(), replicaID) {
		// ignore not match events
		return
	}

	sd.evtCh <- segmentEvent{
		eventType:   segmentAdd,
		segmentID:   info.GetSegmentID(),
		partitionID: info.GetPartitionID(),
		nodeID:      info.GetNodeID(),
		state:       segmentStateLoaded,
	}
}

func (sd *etcdShardSegmentDetector) handleDelEvent(e *clientv3.Event, collectionID int64, replicaID int64, vchannel string) {
	if e.PrevKv == nil {
		return
	}
	info, err := sd.parseSegmentInfo(e.PrevKv.Value)
	if err != nil {
		log.Warn("SegmentDetector failed to parse delete event", zap.Any("event", e), zap.Error(err))
		return
	}

	if info.GetCollectionID() != collectionID || vchannel != info.GetDmChannel() || !inList(info.GetReplicaIds(), replicaID) {
		// ignore not match events
		return
	}

	sd.evtCh <- segmentEvent{
		eventType:   segmentDel,
		segmentID:   info.GetSegmentID(),
		partitionID: info.GetPartitionID(),
		nodeID:      info.GetNodeID(),
		state:       segmentStateOffline,
	}
}

// TODO maybe should use other proto
func (sd *etcdShardSegmentDetector) parseSegmentInfo(bs []byte) (*querypb.SegmentInfo, error) {
	info := &querypb.SegmentInfo{}
	err := proto.Unmarshal(bs, info)
	return info, err
}

func inList(list []int64, target int64) bool {
	for _, i := range list {
		if i == target {
			return true
		}
	}
	return false
}
