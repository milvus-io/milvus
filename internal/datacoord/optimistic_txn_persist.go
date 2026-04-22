package datacoord

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/retry"
)

var (
	ErrKeyAlreadyExists = fmt.Errorf("key already exists")
	ErrKeyNotFound      = fmt.Errorf("key not found")
	// ErrCASFailed is returned by Commit when an Update op's expectedVersion
	// doesn't match the backend's current ModRevision for that key. Callers
	// that want read-modify-write semantics retry: re-read the latest version,
	// re-mutate, re-commit.
	ErrCASFailed = fmt.Errorf("CAS precondition failed")
)

// OptimisticTxnPersist is a bytes-only persist layer with atomic multi-key
// transactions. Typed callers wrap this (see SegmentTxnWrapper) to add
// marshaling and domain logic.
type OptimisticTxnPersist interface {
	Txn(ctx context.Context) Txn
	// Scan reads all key-value pairs with the given prefix.
	Scan(ctx context.Context, prefix string) (keys []string, values [][]byte, versions []int64, err error)
}

// Txn collects operations and commits them atomically.
//
// Strict ops fail the commit if their precondition is violated:
//   - Insert: key must not exist.
//   - Update: key's ModRevision must equal expectedVersion (ErrCASFailed otherwise).
//   - Delete: key must exist.
//
// Unconditional ops always succeed:
//   - Put: create-or-overwrite.
//   - Remove: delete-if-exists.
//
// Every op in a Txn commits in a single atomic backend transaction.
type Txn interface {
	Insert(key string, value []byte)
	Update(key string, value []byte, expectedVersion int64)
	Delete(key string)
	Put(key string, value []byte)
	Remove(key string)
	Commit() ([]TxnResult, error)
}

type TxnResult struct {
	Value   []byte
	Version int64
}

// --- Segment key helpers ---

const segmentMetaPrefix = "datacoord-meta/s/"

func segmentKey(collectionID, partitionID, segmentID int64) string {
	return fmt.Sprintf("%s%d/%d/%d", segmentMetaPrefix, collectionID, partitionID, segmentID)
}

// ============================================================
// Internal op representation
// ============================================================

type opKind int

const (
	opInsert opKind = iota
	opUpdate
	opDelete
	opPut
	opRemove
)

type txnOp struct {
	kind            opKind
	key             string
	value           []byte // Insert, Update, Put
	expectedVersion int64  // Update
}

// ============================================================
// Etcd implementation
// ============================================================

type etcdPersist struct {
	cli *clientv3.Client
}

func NewOptimisticTxnEtcdPersist(cli *clientv3.Client) OptimisticTxnPersist {
	return &etcdPersist{cli: cli}
}

func (p *etcdPersist) Txn(ctx context.Context) Txn {
	return &etcdTxn{ctx: ctx, persist: p}
}

func (p *etcdPersist) Scan(ctx context.Context, prefix string) ([]string, [][]byte, []int64, error) {
	const batchSize int64 = 10000
	key := prefix
	end := clientv3.GetPrefixRangeEnd(prefix)

	var ks []string
	var vals [][]byte
	var vers []int64

	for {
		resp, err := p.cli.Get(ctx, key, clientv3.WithRange(end), clientv3.WithLimit(batchSize), clientv3.WithSerializable())
		if err != nil {
			return nil, nil, nil, err
		}
		for _, kv := range resp.Kvs {
			ks = append(ks, string(kv.Key))
			vals = append(vals, kv.Value)
			vers = append(vers, kv.ModRevision)
		}
		if !resp.More {
			break
		}
		key = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
	}
	return ks, vals, vers, nil
}

type etcdTxn struct {
	ctx     context.Context
	persist *etcdPersist
	ops     []txnOp
}

func (t *etcdTxn) Insert(key string, value []byte) {
	t.ops = append(t.ops, txnOp{kind: opInsert, key: key, value: value})
}

func (t *etcdTxn) Update(key string, value []byte, expectedVersion int64) {
	t.ops = append(t.ops, txnOp{kind: opUpdate, key: key, value: value, expectedVersion: expectedVersion})
}

func (t *etcdTxn) Delete(key string) {
	t.ops = append(t.ops, txnOp{kind: opDelete, key: key})
}

func (t *etcdTxn) Put(key string, value []byte) {
	t.ops = append(t.ops, txnOp{kind: opPut, key: key, value: value})
}

func (t *etcdTxn) Remove(key string) {
	t.ops = append(t.ops, txnOp{kind: opRemove, key: key})
}

func (t *etcdTxn) Commit() ([]TxnResult, error) {
	results := make([]TxnResult, len(t.ops))

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

func (t *etcdTxn) commitBatch(ops []txnOp, results []TxnResult) error {
	cmps := make([]clientv3.Cmp, 0, len(ops))
	puts := make([]clientv3.Op, 0, len(ops))

	for i, op := range ops {
		switch op.kind {
		case opInsert:
			cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(op.key), "=", 0))
			puts = append(puts, clientv3.OpPut(op.key, string(op.value)))
			results[i].Value = op.value

		case opUpdate:
			cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(op.key), "=", op.expectedVersion))
			puts = append(puts, clientv3.OpPut(op.key, string(op.value)))
			results[i].Value = op.value

		case opDelete:
			cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(op.key), "!=", 0))
			puts = append(puts, clientv3.OpDelete(op.key))

		case opPut:
			puts = append(puts, clientv3.OpPut(op.key, string(op.value)))
			results[i].Value = op.value

		case opRemove:
			puts = append(puts, clientv3.OpDelete(op.key))
		}
	}

	if len(puts) == 0 {
		return nil
	}

	var txnResp *clientv3.TxnResponse
	var err error
	if len(cmps) == 0 {
		txnResp, err = t.persist.cli.Txn(t.ctx).Then(puts...).Commit()
	} else {
		txnResp, err = t.persist.cli.Txn(t.ctx).If(cmps...).Then(puts...).Commit()
	}
	if err != nil {
		return err
	}
	if !txnResp.Succeeded {
		return classifyTxnFailure(ops)
	}

	rev := txnResp.Header.Revision
	for i := range ops {
		results[i].Version = rev
	}
	return nil
}

// classifyTxnFailure maps an etcd txn precondition failure to the most
// specific error we can infer from the staged ops. This is a best-effort
// classification; callers that need to distinguish (e.g. the retry loop for
// Update-CAS) check for ErrCASFailed specifically.
func classifyTxnFailure(ops []txnOp) error {
	hasUpdate, hasInsert, hasDelete := false, false, false
	for _, op := range ops {
		switch op.kind {
		case opUpdate:
			hasUpdate = true
		case opInsert:
			hasInsert = true
		case opDelete:
			hasDelete = true
		}
	}
	switch {
	case hasUpdate:
		return ErrCASFailed
	case hasInsert:
		return ErrKeyAlreadyExists
	case hasDelete:
		return ErrKeyNotFound
	}
	return fmt.Errorf("txn precondition failed")
}

// ============================================================
// TiKV implementation
// ============================================================

type tikvPersist struct {
	cli *txnkv.Client
}

func NewOptimisticTxnTiKVPersist(cli *txnkv.Client) OptimisticTxnPersist {
	return &tikvPersist{cli: cli}
}

func (p *tikvPersist) Txn(ctx context.Context) Txn {
	return &tikvTxn{ctx: ctx, persist: p}
}

func (p *tikvPersist) captureCommitTS(txn interface {
	SetCommitCallback(func(string, error))
},
) *uint64 {
	var cts uint64
	txn.SetCommitCallback(func(info string, err error) {
		if err != nil || info == "" {
			return
		}
		var txnInfo transaction.TxnInfo
		if e := json.Unmarshal([]byte(info), &txnInfo); e != nil {
			return
		}
		cts = txnInfo.CommitTS
	})
	return &cts
}

func (p *tikvPersist) Scan(ctx context.Context, prefix string) ([]string, [][]byte, []int64, error) {
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

	var ks []string
	var vals [][]byte
	var vers []int64
	for iter.Valid() {
		ks = append(ks, string(iter.Key()))
		v := append([]byte(nil), iter.Value()...)
		vals = append(vals, v)
		vers = append(vers, int64(txn.StartTS()))
		if err := iter.Next(); err != nil {
			return nil, nil, nil, err
		}
	}
	return ks, vals, vers, nil
}

type tikvTxn struct {
	ctx     context.Context
	persist *tikvPersist
	ops     []txnOp
}

func (t *tikvTxn) Insert(key string, value []byte) {
	t.ops = append(t.ops, txnOp{kind: opInsert, key: key, value: value})
}

func (t *tikvTxn) Update(key string, value []byte, expectedVersion int64) {
	t.ops = append(t.ops, txnOp{kind: opUpdate, key: key, value: value, expectedVersion: expectedVersion})
}

func (t *tikvTxn) Delete(key string) {
	t.ops = append(t.ops, txnOp{kind: opDelete, key: key})
}

func (t *tikvTxn) Put(key string, value []byte) {
	t.ops = append(t.ops, txnOp{kind: opPut, key: key, value: value})
}

func (t *tikvTxn) Remove(key string) {
	t.ops = append(t.ops, txnOp{kind: opRemove, key: key})
}

func (t *tikvTxn) Commit() ([]TxnResult, error) {
	results := make([]TxnResult, len(t.ops))

	err := retry.Do(t.ctx, func() error {
		txn, err := t.persist.cli.Begin()
		if err != nil {
			return err
		}
		defer txn.Rollback()

		anyWrite := false
		for i, op := range t.ops {
			keyBytes := []byte(op.key)
			switch op.kind {
			case opInsert:
				_, err := txn.Get(t.ctx, keyBytes)
				if err == nil {
					return retry.Unrecoverable(fmt.Errorf("%w: %s", ErrKeyAlreadyExists, op.key))
				}
				if err := txn.Set(keyBytes, op.value); err != nil {
					return err
				}
				results[i].Value = op.value
				anyWrite = true

			case opUpdate:
				// TiKV doesn't expose per-key ModRevision; the transaction
				// itself provides serializable-snapshot isolation, so a
				// concurrent writer that committed between our read and this
				// Set will cause the Commit to fail. expectedVersion is
				// accepted for API symmetry but not checked here.
				_ = op.expectedVersion
				if err := txn.Set(keyBytes, op.value); err != nil {
					return err
				}
				results[i].Value = op.value
				anyWrite = true

			case opDelete:
				if _, err := txn.Get(t.ctx, keyBytes); err != nil {
					return retry.Unrecoverable(fmt.Errorf("%w: %s", ErrKeyNotFound, op.key))
				}
				if err := txn.Delete(keyBytes); err != nil {
					return err
				}
				anyWrite = true

			case opPut:
				if err := txn.Set(keyBytes, op.value); err != nil {
					return err
				}
				results[i].Value = op.value
				anyWrite = true

			case opRemove:
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
// In-memory implementation (tests)
// ============================================================

type memEntry struct {
	value   []byte
	version int64
}

type memPersist struct {
	data    map[string]*memEntry
	nextVer int64
}

func NewOptimisticTxnMemoryPersist() OptimisticTxnPersist {
	return &memPersist{
		data:    make(map[string]*memEntry),
		nextVer: 1,
	}
}

func (p *memPersist) Txn(ctx context.Context) Txn {
	return &memTxn{persist: p}
}

func (p *memPersist) Scan(ctx context.Context, prefix string) ([]string, [][]byte, []int64, error) {
	var ks []string
	var vals [][]byte
	var vers []int64
	for k, entry := range p.data {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		ks = append(ks, k)
		vals = append(vals, entry.value)
		vers = append(vers, entry.version)
	}
	return ks, vals, vers, nil
}

type memTxn struct {
	persist *memPersist
	ops     []txnOp
}

func (t *memTxn) Insert(key string, value []byte) {
	t.ops = append(t.ops, txnOp{kind: opInsert, key: key, value: value})
}

func (t *memTxn) Update(key string, value []byte, expectedVersion int64) {
	t.ops = append(t.ops, txnOp{kind: opUpdate, key: key, value: value, expectedVersion: expectedVersion})
}

func (t *memTxn) Delete(key string) {
	t.ops = append(t.ops, txnOp{kind: opDelete, key: key})
}

func (t *memTxn) Put(key string, value []byte) {
	t.ops = append(t.ops, txnOp{kind: opPut, key: key, value: value})
}

func (t *memTxn) Remove(key string) {
	t.ops = append(t.ops, txnOp{kind: opRemove, key: key})
}

func (t *memTxn) Commit() ([]TxnResult, error) {
	p := t.persist
	// Validate all ops first so we don't partially apply.
	for _, op := range t.ops {
		switch op.kind {
		case opInsert:
			if _, ok := p.data[op.key]; ok {
				return nil, fmt.Errorf("%w: %s", ErrKeyAlreadyExists, op.key)
			}
		case opUpdate:
			entry, ok := p.data[op.key]
			if !ok {
				return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, op.key)
			}
			if entry.version != op.expectedVersion {
				return nil, ErrCASFailed
			}
		case opDelete:
			if _, ok := p.data[op.key]; !ok {
				return nil, fmt.Errorf("%w: %s", ErrKeyNotFound, op.key)
			}
		}
	}

	ver := p.nextVer
	p.nextVer++
	results := make([]TxnResult, len(t.ops))

	for i, op := range t.ops {
		switch op.kind {
		case opInsert, opUpdate, opPut:
			p.data[op.key] = &memEntry{value: op.value, version: ver}
			results[i] = TxnResult{Value: op.value, Version: ver}
		case opDelete, opRemove:
			delete(p.data, op.key)
			results[i] = TxnResult{Version: ver}
		}
	}
	return results, nil
}
