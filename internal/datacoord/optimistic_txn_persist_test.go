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

package datacoord

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type stringMarshaler struct{}

func (stringMarshaler) Marshal(value string) ([]byte, error) {
	return []byte(value), nil
}

func (stringMarshaler) Unmarshal(value []byte) (string, error) {
	return string(value), nil
}

type unmarshalErrorMarshaler struct {
	err error
}

func (unmarshalErrorMarshaler) Marshal(value string) ([]byte, error) {
	return []byte(value), nil
}

func (m unmarshalErrorMarshaler) Unmarshal([]byte) (string, error) {
	return "", m.err
}

func TestMemoryPersistConcurrentTransactions(t *testing.T) {
	const entries = 64
	persist := NewOptimisticTxnMemoryPersist[string, string](stringMarshaler{})
	seed := persist.Txn(context.Background())
	for i := 0; i < entries; i++ {
		key := fmt.Sprintf("segments/%d", i)
		seed.Insert(key, key)
	}
	_, err := seed.Commit()
	require.NoError(t, err)

	errCh := make(chan error, entries)
	var wg sync.WaitGroup
	for i := 0; i < entries; i++ {
		key := fmt.Sprintf("segments/%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			txn := persist.Txn(context.Background())
			txn.Delete(key)
			_, err := txn.Commit()
			errCh <- err
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err)
	}

	keys, values, versions, err := persist.Scan(context.Background(), "segments/")
	require.NoError(t, err)
	require.Empty(t, keys)
	require.Empty(t, values)
	require.Empty(t, versions)
}

func TestMemoryPersistDeleteIfVersion(t *testing.T) {
	ctx := context.Background()
	persist := NewOptimisticTxnMemoryPersist[string, string](stringMarshaler{})
	seed := persist.Txn(ctx)
	seed.Insert("segments/1", "value")
	results, err := seed.Commit()
	require.NoError(t, err)
	require.Len(t, results, 1)
	version := results[0].Version

	stale := persist.Txn(ctx)
	stale.DeleteIfVersion("segments/1", version+1)
	_, err = stale.Commit()
	require.ErrorIs(t, err, ErrVersionConflict)
	keys, _, _, err := persist.Scan(ctx, "segments/")
	require.NoError(t, err)
	require.Equal(t, []string{"segments/1"}, keys)

	remove := persist.Txn(ctx)
	remove.DeleteIfVersion("segments/1", version)
	_, err = remove.Commit()
	require.NoError(t, err)
	keys, _, _, err = persist.Scan(ctx, "segments/")
	require.NoError(t, err)
	require.Empty(t, keys)
}

type scanTestKVServer struct {
	etcdserverpb.UnimplementedKVServer

	kvs          []*mvccpb.KeyValue
	pageSize     int
	rangeErr     error
	rangeCalls   atomic.Int32
	txnCalls     atomic.Int32
	txnSucceeded bool
	lastTxn      atomic.Pointer[etcdserverpb.TxnRequest]
}

func (s *scanTestKVServer) Range(_ context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	if s.rangeCalls.Add(1) > 1 && s.rangeErr != nil {
		return nil, s.rangeErr
	}
	kvs := s.filter(req)
	pageSize := s.pageSize
	if pageSize <= 0 || pageSize > len(kvs) {
		pageSize = len(kvs)
	}
	if req.Limit > 0 && int64(pageSize) > req.Limit {
		pageSize = int(req.Limit)
	}
	return &etcdserverpb.RangeResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 100},
		Kvs:    kvs[:pageSize],
		More:   pageSize < len(kvs),
		Count:  int64(len(kvs)),
	}, nil
}

func (s *scanTestKVServer) Txn(_ context.Context, req *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	s.txnCalls.Add(1)
	s.lastTxn.Store(req)
	return &etcdserverpb.TxnResponse{
		Header:    &etcdserverpb.ResponseHeader{Revision: 101},
		Succeeded: s.txnSucceeded,
	}, nil
}

func (s *scanTestKVServer) filter(req *etcdserverpb.RangeRequest) []*mvccpb.KeyValue {
	result := make([]*mvccpb.KeyValue, 0, len(s.kvs))
	for _, kv := range s.kvs {
		if len(req.RangeEnd) == 0 && !bytes.Equal(kv.Key, req.Key) {
			continue
		}
		if bytes.Compare(kv.Key, req.Key) < 0 {
			continue
		}
		if len(req.RangeEnd) > 0 && bytes.Compare(kv.Key, req.RangeEnd) >= 0 {
			continue
		}
		result = append(result, kv)
	}
	return result
}

func newScanTestClient(t *testing.T, kvServer etcdserverpb.KVServer) *clientv3.Client {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	etcdserverpb.RegisterKVServer(server, kvServer)
	go func() {
		_ = server.Serve(listener)
	}()

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://" + listener.Addr().String()},
		DialTimeout: 5 * time.Second,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = client.Close()
		server.Stop()
		_ = listener.Close()
	})
	return client
}

func scanTestKVs() []*mvccpb.KeyValue {
	return []*mvccpb.KeyValue{
		{Key: []byte("other/1"), Value: []byte("ignored"), ModRevision: 1},
		{Key: []byte("segments/1"), Value: []byte("value-1"), ModRevision: 11},
		{Key: []byte("segments/2"), Value: []byte("value-2"), ModRevision: 12},
		{Key: []byte("segments/3"), Value: []byte("value-3"), ModRevision: 13},
	}
}

func TestEtcdPersistScanPagesThroughRange(t *testing.T) {
	server := &scanTestKVServer{kvs: scanTestKVs(), pageSize: 2}
	client := newScanTestClient(t, server)
	persist := NewOptimisticTxnEtcdPersist[string, string](client, stringMarshaler{})

	keys, values, versions, err := persist.Scan(context.Background(), "segments/")

	require.NoError(t, err)
	require.Equal(t, []string{"segments/1", "segments/2", "segments/3"}, keys)
	require.Equal(t, []string{"value-1", "value-2", "value-3"}, values)
	require.Equal(t, []int64{11, 12, 13}, versions)
	require.Equal(t, int32(2), server.rangeCalls.Load())
}

func TestEtcdPersistDeleteIfVersionAvoidsRange(t *testing.T) {
	server := &scanTestKVServer{txnSucceeded: true}
	client := newScanTestClient(t, server)
	persist := NewOptimisticTxnEtcdPersist[string, string](client, stringMarshaler{})
	txn := persist.Txn(context.Background())
	txn.DeleteIfVersion("segments/1", 11)

	results, err := txn.Commit()

	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, int64(101), results[0].Version)
	require.Zero(t, server.rangeCalls.Load(), "a known revision must avoid a per-key etcd GET")
	require.Equal(t, int32(1), server.txnCalls.Load())
	request := server.lastTxn.Load()
	require.NotNil(t, request)
	require.Len(t, request.Compare, 1)
	require.Len(t, request.Success, 1)
	require.Equal(t, []byte("segments/1"), request.Compare[0].Key)
	require.Equal(t, int64(11), request.Compare[0].GetModRevision())
}

func TestEtcdPersistDeleteIfVersionFailsOnRevisionChange(t *testing.T) {
	server := &scanTestKVServer{
		kvs:          []*mvccpb.KeyValue{{Key: []byte("segments/1"), ModRevision: 12}},
		txnSucceeded: false,
	}
	client := newScanTestClient(t, server)
	persist := NewOptimisticTxnEtcdPersist[string, string](client, stringMarshaler{})
	txn := persist.Txn(context.Background())
	txn.DeleteIfVersion("segments/1", 11)

	_, err := txn.Commit()

	require.ErrorIs(t, err, ErrVersionConflict)
	require.Equal(t, int32(1), server.rangeCalls.Load(), "a failed compare is diagnosed once")
	require.Equal(t, int32(1), server.txnCalls.Load(), "a fixed stale revision must not retry forever")
}

func TestEtcdPersistDeleteIfVersionTreatsMissingKeyAsApplied(t *testing.T) {
	// A failed compare on an absent key is an earlier delete whose response
	// was lost: the conditional delete has already taken effect.
	server := &scanTestKVServer{txnSucceeded: false}
	client := newScanTestClient(t, server)
	persist := NewOptimisticTxnEtcdPersist[string, string](client, stringMarshaler{})
	txn := persist.Txn(context.Background())
	txn.DeleteIfVersion("segments/1", 11)

	_, err := txn.Commit()

	require.NoError(t, err)
	require.Equal(t, int32(1), server.rangeCalls.Load())
	require.Equal(t, int32(1), server.txnCalls.Load())
}

func TestEtcdPersistScanFailsOnAFailedPage(t *testing.T) {
	server := &scanTestKVServer{
		kvs:      scanTestKVs(),
		pageSize: 1,
		rangeErr: status.Error(codes.Unavailable, "range interrupted"),
	}
	client := newScanTestClient(t, server)
	persist := NewOptimisticTxnEtcdPersist[string, string](client, stringMarshaler{})

	keys, values, versions, err := persist.Scan(context.Background(), "segments/")

	require.ErrorIs(t, err, merr.ErrIoFailed)
	require.Nil(t, keys)
	require.Nil(t, values)
	require.Nil(t, versions)
}

func TestEtcdPersistScanFailsOnUnmarshalFailure(t *testing.T) {
	server := &scanTestKVServer{kvs: scanTestKVs(), pageSize: 1}
	client := newScanTestClient(t, server)
	decodeErr := status.Error(codes.Unimplemented, "decode failed")
	persist := NewOptimisticTxnEtcdPersist[string, string](client, unmarshalErrorMarshaler{err: decodeErr})

	keys, values, versions, err := persist.Scan(context.Background(), "segments/")

	require.ErrorIs(t, err, decodeErr)
	require.Nil(t, keys)
	require.Nil(t, values)
	require.Nil(t, versions)
	require.Equal(t, int32(1), server.rangeCalls.Load())
}
