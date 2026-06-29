package datacoord

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	tikverr "github.com/tikv/client-go/v2/error"
	tikvkv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

var (
	ErrKeyAlreadyExists = fmt.Errorf("key already exists")
	ErrKeyNotFound      = fmt.Errorf("key not found")
)

// OptimisticTxnPersist is a persist layer that uses optimistic transactions.
type OptimisticTxnPersist[K comparable, V any] interface {
	// Txn creates a new transaction. Add operations via Insert/Update/Upsert/Delete,
	// then call Commit to execute them atomically.
	Txn(ctx context.Context) Txn[K, V]
	// Scan reads all key-value pairs with the given prefix.
	Scan(ctx context.Context, prefix K) (keys []K, values []V, versions []int64, err error)
}

// Txn collects operations and commits them atomically.
type Txn[K comparable, V any] interface {
	// Insert adds a write for a key that must not exist.
	Insert(key K, value V)
	// Update adds a read-modify-write for a key that must exist.
	Update(key K, f UpdateFunc[V])
	// Upsert adds an insert-or-update. If key doesn't exist, inserts value. If exists, applies f.
	Upsert(key K, value V, f UpdateFunc[V])
	// Delete adds a delete for a key that must exist.
	Delete(key K)
	// Commit executes all operations atomically. Returns results in the order ops were added.
	// On CAS failure, retries automatically (re-reads and re-applies UpdateFuncs).
	Commit() ([]TxnResult[V], error)
}

// TxnResult is the result of a single operation after commit.
type TxnResult[V any] struct {
	Value   V
	Version int64
}

// UpdateFunc transforms an existing value. Returns (newValue, shouldWrite).
// If shouldWrite is false, the write is skipped and the existing value/version are returned.
type UpdateFunc[T any] func(existing T) (T, bool)

type Marshaler[T any] interface {
	Marshal(v T) ([]byte, error)
	Unmarshal(v []byte) (T, error)
}

// --- Segment key helpers ---

const segmentMetaPrefix = "datacoord-meta/s/"

func segmentKey(collectionID, partitionID, segmentID int64) string {
	return fmt.Sprintf("%s%d/%d/%d", segmentMetaPrefix, collectionID, partitionID, segmentID)
}

func segmentIDFromKey(key string) (int64, error) {
	parts := strings.Split(key, "/")
	if len(parts) == 0 {
		return 0, fmt.Errorf("invalid segment key: %s", key)
	}
	return strconv.ParseInt(parts[len(parts)-1], 10, 64)
}

// --- SegmentInfo Marshaler (protobuf) ---

type SegmentInfoMarshaler struct{}

func (m *SegmentInfoMarshaler) Marshal(v *datapb.SegmentInfo) ([]byte, error) {
	return proto.Marshal(v)
}

func (m *SegmentInfoMarshaler) Unmarshal(data []byte) (*datapb.SegmentInfo, error) {
	v := &datapb.SegmentInfo{}
	if err := proto.Unmarshal(data, v); err != nil {
		return nil, err
	}
	return v, nil
}

// ============================================================
// op types (internal)
// ============================================================

type opKind int

const (
	opInsert opKind = iota
	opUpdate
	opUpsert
	opDelete
)

type txnOp[K comparable, V any] struct {
	kind       opKind
	key        K
	value      V             // used by Insert, Upsert (insert case)
	updateFunc UpdateFunc[V] // used by Update, Upsert (update case)
}

// ============================================================
// Etcd implementation
// ============================================================

type etcdPersist[K string, V any] struct {
	cli       *clientv3.Client
	marshaler Marshaler[V]
}

func NewOptimisticTxnEtcdPersist[K string, V any](cli *clientv3.Client, marshaler Marshaler[V]) OptimisticTxnPersist[K, V] {
	return &etcdPersist[K, V]{cli: cli, marshaler: marshaler}
}

func (p *etcdPersist[K, V]) Txn(ctx context.Context) Txn[K, V] {
	return &etcdTxn[K, V]{ctx: ctx, persist: p}
}

func (p *etcdPersist[K, V]) Scan(ctx context.Context, prefix K) ([]K, []V, []int64, error) {
	const batchSize int64 = 10000
	key := string(prefix)
	end := clientv3.GetPrefixRangeEnd(key)

	var ks []K
	var vals []V
	var vers []int64

	for {
		resp, err := p.cli.Get(ctx, key, clientv3.WithRange(end), clientv3.WithLimit(batchSize), clientv3.WithSerializable())
		if err != nil {
			return nil, nil, nil, err
		}
		for _, kv := range resp.Kvs {
			v, err := p.marshaler.Unmarshal(kv.Value)
			if err != nil {
				return nil, nil, nil, err
			}
			ks = append(ks, K(kv.Key))
			vals = append(vals, v)
			vers = append(vers, kv.ModRevision)
		}
		if !resp.More {
			break
		}
		// Next batch starts after the last key
		key = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
		mlog.Info(ctx, "etcdPersist.Scan next batch", zap.String("key", key))
	}
	return ks, vals, vers, nil
}

type etcdTxn[K string, V any] struct {
	ctx     context.Context
	persist *etcdPersist[K, V]
	ops     []txnOp[K, V]
}

func (t *etcdTxn[K, V]) Insert(key K, value V) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opInsert, key: key, value: value})
}

func (t *etcdTxn[K, V]) Update(key K, f UpdateFunc[V]) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opUpdate, key: key, updateFunc: f})
}

func (t *etcdTxn[K, V]) Upsert(key K, value V, f UpdateFunc[V]) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opUpsert, key: key, value: value, updateFunc: f})
}

func (t *etcdTxn[K, V]) Delete(key K) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opDelete, key: key})
}

func (t *etcdTxn[K, V]) Commit() ([]TxnResult[V], error) {
	results := make([]TxnResult[V], len(t.ops))

	// Split ops into batches to avoid exceeding etcd txn op limits.
	batchSize := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	for batchStart := 0; batchStart < len(t.ops); batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > len(t.ops) {
			batchEnd = len(t.ops)
		}
		if err := t.commitBatch(t.ops[batchStart:batchEnd], results[batchStart:batchEnd]); err != nil {
			return nil, err
		}
	}
	return results, nil
}

func (t *etcdTxn[K, V]) commitBatch(ops []txnOp[K, V], results []TxnResult[V]) error {
	commitStart := time.Now()
	retryCount := 0

	return retry.Do(t.ctx, func() error {
		retryCount++
		cmps := make([]clientv3.Cmp, 0, len(ops))
		putOps := make([]clientv3.Op, 0, len(ops))
		written := make([]bool, len(ops))

		// Phase 1: read existing values from etcd
		getStart := time.Now()
		for i, op := range ops {
			keyStr := string(op.key)
			switch op.kind {
			case opInsert:
				resp, err := t.persist.cli.Get(t.ctx, keyStr, clientv3.WithSerializable(), clientv3.WithKeysOnly())
				if err != nil {
					return err
				}
				exists := len(resp.Kvs) > 0
				if exists {
					return retry.Unrecoverable(fmt.Errorf("%w: %s", ErrKeyAlreadyExists, keyStr))
				}
				cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(keyStr), "=", 0))
				valBytes, err := t.persist.marshaler.Marshal(op.value)
				if err != nil {
					return retry.Unrecoverable(err)
				}
				putOps = append(putOps, clientv3.OpPut(keyStr, string(valBytes)))
				results[i].Value = op.value
				written[i] = true

			case opUpdate:
				resp, err := t.persist.cli.Get(t.ctx, keyStr, clientv3.WithSerializable())
				if err != nil {
					return err
				}
				exists := len(resp.Kvs) > 0
				if !exists {
					return retry.Unrecoverable(fmt.Errorf("%w: %s", ErrKeyNotFound, keyStr))
				}
				existing, err := t.persist.marshaler.Unmarshal(resp.Kvs[0].Value)
				if err != nil {
					return retry.Unrecoverable(err)
				}
				newV, shouldWrite := op.updateFunc(existing)
				if !shouldWrite {
					results[i] = TxnResult[V]{Value: existing, Version: resp.Kvs[0].ModRevision}
					written[i] = false
					continue
				}
				cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(keyStr), "=", resp.Kvs[0].ModRevision))
				valBytes, err := t.persist.marshaler.Marshal(newV)
				if err != nil {
					return retry.Unrecoverable(err)
				}
				putOps = append(putOps, clientv3.OpPut(keyStr, string(valBytes)))
				results[i].Value = newV
				written[i] = true

			case opUpsert:
				resp, err := t.persist.cli.Get(t.ctx, keyStr, clientv3.WithSerializable())
				if err != nil {
					return err
				}
				exists := len(resp.Kvs) > 0
				if exists {
					existing, err := t.persist.marshaler.Unmarshal(resp.Kvs[0].Value)
					if err != nil {
						return retry.Unrecoverable(err)
					}
					newV, shouldWrite := op.updateFunc(existing)
					if !shouldWrite {
						results[i] = TxnResult[V]{Value: existing, Version: resp.Kvs[0].ModRevision}
						written[i] = false
						continue
					}
					cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(keyStr), "=", resp.Kvs[0].ModRevision))
					valBytes, err := t.persist.marshaler.Marshal(newV)
					if err != nil {
						return retry.Unrecoverable(err)
					}
					putOps = append(putOps, clientv3.OpPut(keyStr, string(valBytes)))
					results[i].Value = newV
				} else {
					valBytes, err := t.persist.marshaler.Marshal(op.value)
					if err != nil {
						return retry.Unrecoverable(err)
					}
					putOps = append(putOps, clientv3.OpPut(keyStr, string(valBytes)))
					results[i].Value = op.value
				}
				written[i] = true

			case opDelete:
				resp, err := t.persist.cli.Get(t.ctx, keyStr, clientv3.WithSerializable(), clientv3.WithKeysOnly())
				if err != nil {
					return err
				}
				exists := len(resp.Kvs) > 0

				if !exists {
					return retry.Unrecoverable(fmt.Errorf("%w: %s", ErrKeyNotFound, keyStr))
				}
				cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(keyStr), "=", resp.Kvs[0].ModRevision))
				putOps = append(putOps, clientv3.OpDelete(keyStr))
				written[i] = true
			}
		}
		getDur := time.Since(getStart)

		if len(putOps) == 0 {
			return nil // all skipped
		}

		// Phase 2: commit the transaction
		txnStart := time.Now()
		var txnResp *clientv3.TxnResponse
		var err error
		if len(cmps) == 0 {
			txnResp, err = t.persist.cli.Txn(t.ctx).Then(putOps...).Commit()
		} else {
			txnResp, err = t.persist.cli.Txn(t.ctx).If(cmps...).Then(putOps...).Commit()
		}
		txnDur := time.Since(txnStart)

		totalDur := time.Since(commitStart)
		if totalDur > 40*time.Millisecond {
			mlog.Info(t.ctx, "etcdTxn.Commit slow",
				zap.Duration("total", totalDur),
				zap.Duration("etcdGet", getDur),
				zap.Duration("etcdTxn", txnDur),
				zap.Int("numOps", len(ops)),
				zap.Int("numPuts", len(putOps)),
				zap.Int("retryCount", retryCount))
		}

		if err != nil {
			return err
		}
		if !txnResp.Succeeded {
			return fmt.Errorf("CAS failed, concurrent modification")
		}
		for i := range ops {
			if written[i] {
				results[i].Version = txnResp.Header.Revision
			}
		}
		return nil
	}, retry.AttemptAlways())
}

// ============================================================
// TiKV implementation
// ============================================================

type tikvPersist[K string, V any] struct {
	cli       *txnkv.Client
	marshaler Marshaler[V]
}

func NewOptimisticTxnTiKVPersist[K string, V any](cli *txnkv.Client, marshaler Marshaler[V]) OptimisticTxnPersist[K, V] {
	return &tikvPersist[K, V]{cli: cli, marshaler: marshaler}
}

func (p *tikvPersist[K, V]) Txn(ctx context.Context) Txn[K, V] {
	return &tikvTxn[K, V]{ctx: ctx, persist: p}
}

func (p *tikvPersist[K, V]) captureCommitTS(txn interface{ SetCommitCallback(func(string, error)) }) *uint64 {
	var commitTS uint64
	txn.SetCommitCallback(func(info string, err error) {
		if err == nil {
			var txnInfo transaction.TxnInfo
			json.Unmarshal([]byte(info), &txnInfo)
			commitTS = txnInfo.CommitTS
		}
	})
	return &commitTS
}

func (p *tikvPersist[K, V]) tikvKeyExists(ctx context.Context, txn interface {
	Get(context.Context, []byte, ...tikvkv.GetOption) (tikvkv.ValueEntry, error)
}, key []byte) ([]byte, bool, error) {
	entry, err := txn.Get(ctx, key)
	if err != nil {
		if tikverr.IsErrNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return entry.Value, true, nil
}

func (p *tikvPersist[K, V]) Scan(ctx context.Context, prefix K) ([]K, []V, []int64, error) {
	txn, err := p.cli.Begin()
	if err != nil {
		return nil, nil, nil, err
	}
	defer txn.Rollback()
	prefixBytes := []byte(prefix)
	endKey := make([]byte, len(prefixBytes))
	copy(endKey, prefixBytes)
	endKey[len(endKey)-1]++

	iter, err := txn.Iter(prefixBytes, endKey)
	if err != nil {
		return nil, nil, nil, err
	}
	defer iter.Close()

	var ks []K
	var vals []V
	var vers []int64
	for iter.Valid() {
		v, err := p.marshaler.Unmarshal(iter.Value())
		if err != nil {
			return nil, nil, nil, err
		}
		ks = append(ks, K(iter.Key()))
		vals = append(vals, v)
		vers = append(vers, int64(txn.StartTS()))
		if err := iter.Next(); err != nil {
			return nil, nil, nil, err
		}
	}
	return ks, vals, vers, nil
}

type tikvTxn[K string, V any] struct {
	ctx     context.Context
	persist *tikvPersist[K, V]
	ops     []txnOp[K, V]
}

func (t *tikvTxn[K, V]) Insert(key K, value V) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opInsert, key: key, value: value})
}

func (t *tikvTxn[K, V]) Update(key K, f UpdateFunc[V]) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opUpdate, key: key, updateFunc: f})
}

func (t *tikvTxn[K, V]) Upsert(key K, value V, f UpdateFunc[V]) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opUpsert, key: key, value: value, updateFunc: f})
}

func (t *tikvTxn[K, V]) Delete(key K) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opDelete, key: key})
}

func (t *tikvTxn[K, V]) Commit() ([]TxnResult[V], error) {
	results := make([]TxnResult[V], len(t.ops))

	err := retry.Do(t.ctx, func() error {
		txn, err := t.persist.cli.Begin()
		if err != nil {
			return err
		}
		defer txn.Rollback()

		anyWrite := false
		for i, op := range t.ops {
			keyBytes := []byte(op.key)
			val, exists, err := t.persist.tikvKeyExists(t.ctx, txn, keyBytes)
			if err != nil {
				return err
			}

			switch op.kind {
			case opInsert:
				if exists {
					return retry.Unrecoverable(fmt.Errorf("%w: %s", ErrKeyAlreadyExists, string(op.key)))
				}
				valBytes, err := t.persist.marshaler.Marshal(op.value)
				if err != nil {
					return retry.Unrecoverable(err)
				}
				if err := txn.Set(keyBytes, valBytes); err != nil {
					return err
				}
				results[i].Value = op.value
				anyWrite = true

			case opUpdate:
				if !exists {
					return retry.Unrecoverable(fmt.Errorf("%w: %s", ErrKeyNotFound, string(op.key)))
				}
				existing, err := t.persist.marshaler.Unmarshal(val)
				if err != nil {
					return retry.Unrecoverable(err)
				}
				newV, shouldWrite := op.updateFunc(existing)
				if !shouldWrite {
					results[i].Value = existing
					continue
				}
				valBytes, err := t.persist.marshaler.Marshal(newV)
				if err != nil {
					return retry.Unrecoverable(err)
				}
				if err := txn.Set(keyBytes, valBytes); err != nil {
					return err
				}
				results[i].Value = newV
				anyWrite = true

			case opUpsert:
				if exists {
					existing, err := t.persist.marshaler.Unmarshal(val)
					if err != nil {
						return retry.Unrecoverable(err)
					}
					newV, shouldWrite := op.updateFunc(existing)
					if !shouldWrite {
						results[i].Value = existing
						continue
					}
					valBytes, err := t.persist.marshaler.Marshal(newV)
					if err != nil {
						return retry.Unrecoverable(err)
					}
					if err := txn.Set(keyBytes, valBytes); err != nil {
						return err
					}
					results[i].Value = newV
				} else {
					valBytes, err := t.persist.marshaler.Marshal(op.value)
					if err != nil {
						return retry.Unrecoverable(err)
					}
					if err := txn.Set(keyBytes, valBytes); err != nil {
						return err
					}
					results[i].Value = op.value
				}
				anyWrite = true

			case opDelete:
				if !exists {
					return retry.Unrecoverable(fmt.Errorf("%w: %s", ErrKeyNotFound, string(op.key)))
				}
				if err := txn.Delete(keyBytes); err != nil {
					return err
				}
				anyWrite = true
			}
		}

		if !anyWrite {
			return nil
		}

		cts := t.persist.captureCommitTS(txn)
		err = txn.Commit(t.ctx)
		if err == nil {
			for i := range t.ops {
				results[i].Version = int64(*cts)
			}
		}
		return err
	}, retry.AttemptAlways())

	if err != nil {
		return nil, err
	}
	return results, nil
}

// ============================================================
// In-memory implementation (for testing)
// ============================================================

type memEntry[V any] struct {
	value   V
	version int64
}

type memPersist[K comparable, V any] struct {
	data    map[K]*memEntry[V]
	nextVer int64
}

func NewOptimisticTxnMemoryPersist[K comparable, V any](marshaler Marshaler[V]) OptimisticTxnPersist[K, V] {
	return &memPersist[K, V]{
		data:    make(map[K]*memEntry[V]),
		nextVer: 1,
	}
}

func (p *memPersist[K, V]) Txn(ctx context.Context) Txn[K, V] {
	return &memTxn[K, V]{persist: p}
}

func (p *memPersist[K, V]) Scan(ctx context.Context, prefix K) ([]K, []V, []int64, error) {
	prefixStr := fmt.Sprintf("%v", prefix)
	var ks []K
	var vals []V
	var vers []int64
	for k, entry := range p.data {
		keyStr := fmt.Sprintf("%v", k)
		if strings.HasPrefix(keyStr, prefixStr) {
			ks = append(ks, k)
			vals = append(vals, entry.value)
			vers = append(vers, entry.version)
		}
	}
	return ks, vals, vers, nil
}

type memTxn[K comparable, V any] struct {
	persist *memPersist[K, V]
	ops     []txnOp[K, V]
}

func (t *memTxn[K, V]) Insert(key K, value V) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opInsert, key: key, value: value})
}

func (t *memTxn[K, V]) Update(key K, f UpdateFunc[V]) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opUpdate, key: key, updateFunc: f})
}

func (t *memTxn[K, V]) Upsert(key K, value V, f UpdateFunc[V]) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opUpsert, key: key, value: value, updateFunc: f})
}

func (t *memTxn[K, V]) Delete(key K) {
	t.ops = append(t.ops, txnOp[K, V]{kind: opDelete, key: key})
}

func (t *memTxn[K, V]) Commit() ([]TxnResult[V], error) {
	p := t.persist
	// Validate all ops first (don't partially apply).
	for _, op := range t.ops {
		switch op.kind {
		case opInsert:
			if _, ok := p.data[op.key]; ok {
				return nil, fmt.Errorf("%w: %v", ErrKeyAlreadyExists, op.key)
			}
		case opUpdate:
			if _, ok := p.data[op.key]; !ok {
				return nil, fmt.Errorf("%w: %v", ErrKeyNotFound, op.key)
			}
		case opDelete:
			if _, ok := p.data[op.key]; !ok {
				return nil, fmt.Errorf("%w: %v", ErrKeyNotFound, op.key)
			}
		case opUpsert:
			// always valid
		}
	}

	ver := p.nextVer
	p.nextVer++
	results := make([]TxnResult[V], len(t.ops))

	for i, op := range t.ops {
		switch op.kind {
		case opInsert:
			p.data[op.key] = &memEntry[V]{value: op.value, version: ver}
			results[i] = TxnResult[V]{Value: op.value, Version: ver}

		case opUpdate:
			entry := p.data[op.key]
			newV, shouldWrite := op.updateFunc(entry.value)
			if !shouldWrite {
				results[i] = TxnResult[V]{Value: entry.value, Version: entry.version}
				continue
			}
			p.data[op.key] = &memEntry[V]{value: newV, version: ver}
			results[i] = TxnResult[V]{Value: newV, Version: ver}

		case opUpsert:
			if entry, ok := p.data[op.key]; ok {
				newV, shouldWrite := op.updateFunc(entry.value)
				if !shouldWrite {
					results[i] = TxnResult[V]{Value: entry.value, Version: entry.version}
					continue
				}
				p.data[op.key] = &memEntry[V]{value: newV, version: ver}
				results[i] = TxnResult[V]{Value: newV, Version: ver}
			} else {
				p.data[op.key] = &memEntry[V]{value: op.value, version: ver}
				results[i] = TxnResult[V]{Value: op.value, Version: ver}
			}

		case opDelete:
			delete(p.data, op.key)
			results[i] = TxnResult[V]{Version: ver}
		}
	}

	return results, nil
}
