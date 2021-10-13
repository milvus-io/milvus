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

package rootcoord

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	// RequestTimeout timeout for request
	RequestTimeout = 10 * time.Second
)

type rtPair struct {
	rev int64
	ts  typeutil.Timestamp
}

type metaSnapshot struct {
	cli   *clientv3.Client
	root  string
	tsKey string
	lock  sync.RWMutex

	ts2Rev []rtPair
	minPos int
	maxPos int
	numTs  int
}

func newMetaSnapshot(cli *clientv3.Client, root, tsKey string, bufSize int) (*metaSnapshot, error) {
	if bufSize <= 0 {
		bufSize = 1024
	}
	ms := &metaSnapshot{
		cli:    cli,
		root:   root,
		tsKey:  tsKey,
		lock:   sync.RWMutex{},
		ts2Rev: make([]rtPair, bufSize),
		minPos: 0,
		maxPos: 0,
		numTs:  0,
	}
	if err := ms.loadTs(); err != nil {
		return nil, err
	}
	return ms, nil
}

func (ms *metaSnapshot) loadTs() error {
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	key := path.Join(ms.root, ms.tsKey)
	resp, err := ms.cli.Get(ctx, key)
	if err != nil {
		return err
	}
	if len(resp.Kvs) <= 0 {
		return nil
	}
	version := resp.Kvs[0].Version
	revision := resp.Kvs[0].ModRevision
	createRevision := resp.Kvs[0].CreateRevision
	strTs := string(resp.Kvs[0].Value)
	ts, err := strconv.ParseUint(strTs, 10, 64)
	if err != nil {
		return err
	}
	log.Info("load last ts", zap.Int64("version", version), zap.Int64("revision", revision))

	ms.initTs(revision, ts)
	// start from revision-1, until equals to create revision
	for revision--; revision >= createRevision; revision-- {
		if ms.numTs == len(ms.ts2Rev) {
			break
		}
		resp, err = ms.cli.Get(ctx, key, clientv3.WithRev(revision))
		if err != nil {
			return err
		}
		if len(resp.Kvs) <= 0 {
			return nil
		}

		curVer := resp.Kvs[0].Version
		curRev := resp.Kvs[0].ModRevision
		if curVer > version {
			log.Warn("version go backwards", zap.Int64("curVer", curVer), zap.Int64("version", version))
			return nil
		}
		if curVer == version {
			log.Debug("snapshot found save version with different revision", zap.Int64("revision", revision), zap.Int64("version", version))
		}
		strTs := string(resp.Kvs[0].Value)
		if strTs == "0" {
			//#issue 7150, index building inserted "0", skipping
			//this is a special fix for backward compatibility, previous version will put 0 ts into snapshot building index
			continue
		}
		curTs, err := strconv.ParseUint(strTs, 10, 64)
		if err != nil {
			return err
		}
		if curTs >= ts {
			return fmt.Errorf("timestamp go back, curTs=%d,ts=%d", curTs, ts)
		}
		ms.initTs(curRev, curTs)
		ts = curTs
		revision = curRev
		version = curVer
	}

	return nil
}

func (ms *metaSnapshot) maxTs() typeutil.Timestamp {
	return ms.ts2Rev[ms.maxPos].ts
}

func (ms *metaSnapshot) minTs() typeutil.Timestamp {
	return ms.ts2Rev[ms.minPos].ts
}

func (ms *metaSnapshot) initTs(rev int64, ts typeutil.Timestamp) {
	log.Debug("init meta snapshot ts", zap.Int64("rev", rev), zap.Uint64("ts", ts))
	if ms.numTs == 0 {
		ms.maxPos = len(ms.ts2Rev) - 1
		ms.minPos = len(ms.ts2Rev) - 1
		ms.numTs = 1
		ms.ts2Rev[ms.maxPos].rev = rev
		ms.ts2Rev[ms.maxPos].ts = ts
	} else if ms.numTs < len(ms.ts2Rev) {
		ms.minPos--
		ms.numTs++
		ms.ts2Rev[ms.minPos].rev = rev
		ms.ts2Rev[ms.minPos].ts = ts
	}
}

func (ms *metaSnapshot) putTs(rev int64, ts typeutil.Timestamp) {
	log.Debug("put meta snapshto ts", zap.Int64("rev", rev), zap.Uint64("ts", ts))
	ms.maxPos++
	if ms.maxPos == len(ms.ts2Rev) {
		ms.maxPos = 0
	}

	ms.ts2Rev[ms.maxPos].rev = rev
	ms.ts2Rev[ms.maxPos].ts = ts
	if ms.numTs < len(ms.ts2Rev) {
		ms.numTs++
	} else {
		ms.minPos++
		if ms.minPos == len(ms.ts2Rev) {
			ms.minPos = 0
		}
	}
}

func (ms *metaSnapshot) searchOnCache(ts typeutil.Timestamp, start, length int) int64 {
	if length == 1 {
		return ms.ts2Rev[start].rev
	}
	begin := start
	end := begin + length
	mid := (begin + end) / 2
	for {
		if ms.ts2Rev[mid].ts == ts {
			return ms.ts2Rev[mid].rev
		}
		if mid == begin {
			if ms.ts2Rev[mid].ts < ts || mid == start {
				return ms.ts2Rev[mid].rev
			}
			return ms.ts2Rev[mid-1].rev
		}
		if ms.ts2Rev[mid].ts > ts {
			end = mid
		} else if ms.ts2Rev[mid].ts < ts {
			begin = mid + 1
		}
		mid = (begin + end) / 2
	}
}

func (ms *metaSnapshot) getRevOnCache(ts typeutil.Timestamp) int64 {
	if ms.numTs == 0 {
		return 0
	}
	if ts >= ms.ts2Rev[ms.maxPos].ts {
		return ms.ts2Rev[ms.maxPos].rev
	}
	if ts < ms.ts2Rev[ms.minPos].ts {
		return 0
	}
	if ms.maxPos > ms.minPos {
		return ms.searchOnCache(ts, ms.minPos, ms.maxPos-ms.minPos+1)
	}
	topVal := ms.ts2Rev[len(ms.ts2Rev)-1]
	botVal := ms.ts2Rev[0]
	minVal := ms.ts2Rev[ms.minPos]
	maxVal := ms.ts2Rev[ms.maxPos]
	if ts >= topVal.ts && ts < botVal.ts {
		return topVal.rev
	} else if ts >= minVal.ts && ts < topVal.ts {
		return ms.searchOnCache(ts, ms.minPos, len(ms.ts2Rev)-ms.minPos)
	} else if ts >= botVal.ts && ts < maxVal.ts {
		return ms.searchOnCache(ts, 0, ms.maxPos+1)
	}

	return 0
}

func (ms *metaSnapshot) getRevOnEtcd(ts typeutil.Timestamp, rev int64) int64 {
	if rev < 2 {
		return 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	for rev--; rev >= 2; rev-- {
		resp, err := ms.cli.Get(ctx, path.Join(ms.root, ms.tsKey), clientv3.WithRev(rev))
		if err != nil {
			log.Debug("get ts from etcd failed", zap.Error(err))
			return 0
		}
		if len(resp.Kvs) <= 0 {
			return 0
		}
		rev = resp.Kvs[0].ModRevision
		curTs, err := strconv.ParseUint(string(resp.Kvs[0].Value), 10, 64)
		if err != nil {
			log.Debug("parse timestam error", zap.String("input", string(resp.Kvs[0].Value)), zap.Error(err))
			return 0
		}
		if curTs <= ts {
			return rev
		}
	}
	return 0
}

func (ms *metaSnapshot) getRev(ts typeutil.Timestamp) (int64, error) {
	rev := ms.getRevOnCache(ts)
	if rev > 0 {
		return rev, nil
	}
	rev = ms.ts2Rev[ms.minPos].rev
	rev = ms.getRevOnEtcd(ts, rev)
	if rev > 0 {
		return rev, nil
	}
	return 0, fmt.Errorf("can't find revision on ts=%d", ts)
}

func (ms *metaSnapshot) Save(key, value string, ts typeutil.Timestamp) error {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	strTs := strconv.FormatInt(int64(ts), 10)
	resp, err := ms.cli.Txn(ctx).If().Then(
		clientv3.OpPut(path.Join(ms.root, key), value),
		clientv3.OpPut(path.Join(ms.root, ms.tsKey), strTs),
	).Commit()
	if err != nil {
		return err
	}
	ms.putTs(resp.Header.Revision, ts)

	return nil
}

func (ms *metaSnapshot) Load(key string, ts typeutil.Timestamp) (string, error) {
	ms.lock.RLock()
	defer ms.lock.RUnlock()
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	var resp *clientv3.GetResponse
	var err error
	var rev int64
	if ts == 0 {
		resp, err = ms.cli.Get(ctx, path.Join(ms.root, key))
		if err != nil {
			return "", err
		}
	} else {
		rev, err = ms.getRev(ts)
		if err != nil {
			return "", err
		}
		resp, err = ms.cli.Get(ctx, path.Join(ms.root, key), clientv3.WithRev(rev))
		if err != nil {
			return "", err
		}
	}
	if len(resp.Kvs) == 0 {
		return "", fmt.Errorf("there is no value on key = %s, ts = %d", key, ts)
	}
	return string(resp.Kvs[0].Value), nil
}

func (ms *metaSnapshot) MultiSave(kvs map[string]string, ts typeutil.Timestamp) error {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	ops := make([]clientv3.Op, 0, len(kvs)+1)
	for key, value := range kvs {
		ops = append(ops, clientv3.OpPut(path.Join(ms.root, key), value))
	}

	strTs := strconv.FormatInt(int64(ts), 10)
	ops = append(ops, clientv3.OpPut(path.Join(ms.root, ms.tsKey), strTs))
	resp, err := ms.cli.Txn(ctx).If().Then(ops...).Commit()
	if err != nil {
		return err
	}
	ms.putTs(resp.Header.Revision, ts)
	return nil
}

func (ms *metaSnapshot) LoadWithPrefix(key string, ts typeutil.Timestamp) ([]string, []string, error) {
	ms.lock.RLock()
	defer ms.lock.RUnlock()
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	var resp *clientv3.GetResponse
	var err error
	var rev int64
	if ts == 0 {
		resp, err = ms.cli.Get(ctx, path.Join(ms.root, key), clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		if err != nil {
			return nil, nil, err
		}
	} else {
		rev, err = ms.getRev(ts)
		if err != nil {
			return nil, nil, err
		}
		resp, err = ms.cli.Get(ctx, path.Join(ms.root, key), clientv3.WithPrefix(), clientv3.WithRev(rev), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend))
		if err != nil {
			return nil, nil, err
		}
	}
	keys := make([]string, 0, len(resp.Kvs))
	values := make([]string, 0, len(resp.Kvs))
	tk := path.Join(ms.root, "k")
	prefixLen := len(tk) - 1
	for _, kv := range resp.Kvs {
		tk = string(kv.Key)
		tk = tk[prefixLen:]
		keys = append(keys, tk)
		values = append(values, string(kv.Value))
	}
	return keys, values, nil
}

func (ms *metaSnapshot) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string, ts typeutil.Timestamp) error {
	ms.lock.Lock()
	defer ms.lock.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), RequestTimeout)
	defer cancel()

	ops := make([]clientv3.Op, 0, len(saves)+len(removals)+1)
	for key, value := range saves {
		ops = append(ops, clientv3.OpPut(path.Join(ms.root, key), value))
	}

	strTs := strconv.FormatInt(int64(ts), 10)
	for _, key := range removals {
		ops = append(ops, clientv3.OpDelete(path.Join(ms.root, key), clientv3.WithPrefix()))
	}
	ops = append(ops, clientv3.OpPut(path.Join(ms.root, ms.tsKey), strTs))
	resp, err := ms.cli.Txn(ctx).If().Then(ops...).Commit()
	if err != nil {
		return err
	}
	ms.putTs(resp.Header.Revision, ts)
	return nil
}
