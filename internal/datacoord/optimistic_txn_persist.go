package datacoord

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	tikverr "github.com/tikv/client-go/v2/error"
	tikvkv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"github.com/tikv/client-go/v2/txnkv/txnsnapshot"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/kv/tikv"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

var (
	ErrKeyAlreadyExists = fmt.Errorf("key already exists")
	ErrKeyNotFound      = fmt.Errorf("key not found")
	// ErrVersionConflict reports that a versioned write (PutIfVersion /
	// DeleteIfVersion) found a different version in the store: the value the
	// write was built from is stale, and the write must be rebuilt.
	ErrVersionConflict = errors.New("optimistic txn: version conflict")
	// errCompareFailed reports that the read-modify-write compares of a batch
	// failed: a key changed between the read and the commit.
	errCompareFailed = errors.New("optimistic txn: compare failed")
)

// maxCompareAttempts bounds the attempts of a batch whose compares keep
// failing. Every other failure returns at once and is left to the caller.
const maxCompareAttempts = 20

// OptimisticTxnPersist is a persist layer that uses optimistic transactions.
type OptimisticTxnPersist[K comparable, V any] interface {
	// Txn creates a new transaction. Add operations via Insert/Update/Upsert/Delete,
	// then call Commit to execute them atomically.
	Txn(ctx context.Context) Txn[K, V]
	// Scan reads all key-value pairs with the given prefix.
	Scan(ctx context.Context, prefix K) (keys []K, values []V, versions []int64, err error)
	// Get reads the current value and version of each key. Absent keys are
	// omitted from the result.
	Get(ctx context.Context, keys []K) (map[K]TxnResult[V], error)
	// WalkRaw calls fn with every key and raw value under prefix: the
	// unversioned records composed next to the typed values (see Txn.Raw).
	// fn must not keep value after it returns.
	WalkRaw(ctx context.Context, prefix string, fn func(key string, value []byte) error) error
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
	// PutIfVersion writes value when the key's current version equals version;
	// version 0 means the key must not exist. The key is not read: the caller
	// supplies the version of the value it built the write from. A commit
	// marked op lands in the final batch when the transaction has to be split.
	PutIfVersion(key K, value V, version int64, commit bool)
	// DeleteIfVersion deletes the key when its current version equals version.
	DeleteIfVersion(key K, version int64)
	// Raw composes an unversioned write of another record into the same
	// transaction (e.g. a catalog-encoded DataView or channel tombstone).
	Raw(op RawOp)
	// Group starts a group: the ops added until the next Group call land in
	// one batch when the transaction has to be split, unless they exceed the
	// op limit on their own (e.g. a segment record and its binlog KVs). A
	// group whose raw ops land ahead of its versioned op keeps them under
	// that op's version (see planBatches).
	Group()
	// AtomicOnly makes Commit fail instead of splitting an over-limit
	// transaction into ordered batches.
	AtomicOnly()
	// Commit executes all operations atomically. Returns results in the order ops were added.
	// On CAS failure of a read-modify-write op, retries automatically (re-reads and
	// re-applies UpdateFuncs). A stale versioned op fails with ErrVersionConflict.
	// When a transaction split into ordered batches fails after its first batch,
	// the results come back with the error: the ops of the batches that landed
	// carry their versions, the others version 0.
	Commit() ([]TxnResult[V], error)
}

// RawOp is an unversioned put or delete composed into a transaction. Key is
// the full store key; DeletePrefix removes every key under it.
type RawOp struct {
	Key          string
	Value        string
	Delete       bool
	DeletePrefix bool
	Commit       bool
}

// storeErr marks a store rejection that no retry can heal (an invalid or
// oversized request) as unrecoverable, so a caller's retry loop does not keep
// resending it. Other failures stay retriable for the caller.
func storeErr(err error) error {
	if isPermanentStoreError(err) {
		return retry.Unrecoverable(err)
	}
	return err
}

func isPermanentStoreError(err error) bool {
	var txnTooLarge *tikverr.ErrTxnTooLarge
	if tikv.IsLocalMutationValidationErr(err) || errors.As(err, &txnTooLarge) {
		return true
	}
	// etcd rejects an invalid or oversized request with InvalidArgument, and
	// the client refuses to send one over its message limit with
	// ResourceExhausted, a code etcd also uses for throttling and a full
	// backend, which do recover.
	if errors.IsAny(err, rpctypes.ErrTooManyRequests, rpctypes.ErrGRPCRequestTooManyRequests, rpctypes.ErrNoSpace, rpctypes.ErrGRPCNoSpace) {
		return false
	}
	code := status.Code(err)
	var etcdErr rpctypes.EtcdError
	if errors.As(err, &etcdErr) {
		code = etcdErr.Code()
	}
	return code == codes.InvalidArgument || code == codes.ResourceExhausted
}

// requestContext bounds one store round trip like the catalog's MetaKv does.
func requestContext(ctx context.Context, timeout *paramtable.ParamItem) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, timeout.GetAsDuration(time.Millisecond))
}

func decodeStoredValue[V any](marshaler Marshaler[V], key string, data []byte) (V, error) {
	v, err := marshaler.Unmarshal(data)
	if err != nil {
		return v, merr.WrapErrDataIntegrity(err, "decode the persisted value of key %s", key)
	}
	return v, nil
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

// valueEqualer is implemented by marshalers whose values can be compared. A
// versioned write whose result was lost (e.g. a timeout after the store applied
// it) is recognized on retry by finding its own value in the store.
type valueEqualer[T any] interface {
	Equal(a, b T) bool
}

func storedValueEquals[V any](marshaler Marshaler[V], stored []byte, value V) bool {
	eq, ok := marshaler.(valueEqualer[V])
	if !ok {
		return false
	}
	current, err := marshaler.Unmarshal(stored)
	if err != nil {
		return false
	}
	return eq.Equal(current, value)
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

func (m *SegmentInfoMarshaler) Equal(a, b *datapb.SegmentInfo) bool {
	return proto.Equal(a, b)
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
	opPutIfVersion
	opDeleteIfVersion
	opRaw
)

type txnOp[K comparable, V any] struct {
	kind       opKind
	key        K
	value      V             // used by Insert, Upsert (insert case), PutIfVersion
	updateFunc UpdateFunc[V] // used by Update, Upsert (update case)
	version    int64         // used by PutIfVersion, DeleteIfVersion
	raw        RawOp         // used by Raw
	commit     bool
	group      int // see Txn.Group
}

func (op txnOp[K, V]) versioned() bool {
	return op.kind == opPutIfVersion || op.kind == opDeleteIfVersion
}

// txnOps is the op recorder shared by every backend.
type txnOps[K comparable, V any] struct {
	ops        []txnOp[K, V]
	group      int
	atomicOnly bool
}

func (t *txnOps[K, V]) add(op txnOp[K, V]) {
	op.group = t.group
	t.ops = append(t.ops, op)
}

func (t *txnOps[K, V]) Insert(key K, value V) {
	t.add(txnOp[K, V]{kind: opInsert, key: key, value: value})
}

func (t *txnOps[K, V]) Update(key K, f UpdateFunc[V]) {
	t.add(txnOp[K, V]{kind: opUpdate, key: key, updateFunc: f})
}

func (t *txnOps[K, V]) Upsert(key K, value V, f UpdateFunc[V]) {
	t.add(txnOp[K, V]{kind: opUpsert, key: key, value: value, updateFunc: f})
}

func (t *txnOps[K, V]) Delete(key K) {
	t.add(txnOp[K, V]{kind: opDelete, key: key})
}

func (t *txnOps[K, V]) PutIfVersion(key K, value V, version int64, commit bool) {
	t.add(txnOp[K, V]{kind: opPutIfVersion, key: key, value: value, version: version, commit: commit})
}

func (t *txnOps[K, V]) DeleteIfVersion(key K, version int64) {
	t.add(txnOp[K, V]{kind: opDeleteIfVersion, key: key, version: version})
}

func (t *txnOps[K, V]) Raw(op RawOp) {
	t.add(txnOp[K, V]{kind: opRaw, raw: op, commit: op.Commit})
}

func (t *txnOps[K, V]) Group() {
	t.group++
}

func (t *txnOps[K, V]) AtomicOnly() {
	t.atomicOnly = true
}

// txnBatch is one commit batch: the indexes of its ops, and its guards, the
// versioned ops whose keys it compares and touches (see planBatches).
type txnBatch struct {
	ops    []int
	guards []int
}

// planBatches splits op indexes into commit batches of at most limit ops,
// mirroring the metastore composite-write contract (kv/txn.Commit): a set that
// fits is one atomic batch in recorded order; otherwise the non-commit ops are
// flushed first in recorded order and every commit-marked op lands together in
// the final batch, which is the visibility marker of the whole write. The
// non-commit ops of a group (see Txn.Group) share a batch: a group that does
// not fit in the current batch starts the next one, and only a group over the
// limit on its own is split.
//
// A batch that lands raw ops of a group ahead of the group's versioned op (a
// group over the limit, or a commit-marked versioned op) is guarded by that
// op: it compares the op's key with the version the op expects, and puts the
// key back with its stored value (a touch), which takes a slot of the batch.
// The op then expects the version of the last touch. The raw ops thus land
// only on the version the op was built on, and once they did, a writer of the
// key built on that version conflicts. An absent key (version 0) is compared
// and not touched.
func (t *txnOps[K, V]) planBatches(limit int) ([]txnBatch, error) {
	if len(t.ops) == 0 {
		return nil, nil
	}
	all := make([]int, len(t.ops))
	for i := range t.ops {
		all[i] = i
	}
	if limit <= 0 || len(t.ops) <= limit {
		return []txnBatch{{ops: all}}, nil
	}
	if t.atomicOnly {
		return nil, merr.WrapErrServiceInternalMsg("atomic transaction needs %d ops but the limit is %d", len(t.ops), limit)
	}
	var batches []txnBatch
	var batch txnBatch
	var commit []int
	// size counts the ops of b with the touches of its guards.
	size := func(b txnBatch) int {
		n := len(b.ops)
		for _, guard := range b.guards {
			if t.ops[guard].version != 0 {
				n++
			}
		}
		return n
	}
	for start := 0; start < len(t.ops); {
		end := start + 1
		for end < len(t.ops) && t.ops[end].group == t.ops[start].group {
			end++
		}
		var group []int
		versioned := -1
		for i := start; i < end; i++ {
			if t.ops[i].versioned() {
				versioned = i
			}
			if t.ops[i].commit {
				commit = append(commit, i)
			} else {
				group = append(group, i)
			}
		}
		// place returns ops of the group as the batch that holds them does:
		// with the guard when they include raw ops that land ahead of
		// versioned, those recorded before it or, when it is commit-marked,
		// all of them.
		place := func(ops []int) txnBatch {
			b := txnBatch{ops: ops}
			if versioned >= 0 && !slices.Contains(ops, versioned) && slices.ContainsFunc(ops, func(i int) bool {
				return t.ops[i].kind == opRaw && (t.ops[versioned].commit || i < versioned)
			}) {
				b.guards = []int{versioned}
			}
			return b
		}
		part := place(group)
		if len(batch.ops) > 0 && size(batch)+size(part) > limit {
			batches = append(batches, batch)
			batch = txnBatch{}
		}
		for size(part) > limit {
			// A part takes at most one slot more than its ops: the touch.
			n := limit
			if size(place(group[:n])) > limit {
				n--
			}
			if n == 0 {
				return nil, merr.WrapErrServiceInternalMsg("transaction op limit %d leaves no room for a touch", limit)
			}
			batches = append(batches, place(group[:n]))
			group = group[n:]
			part = place(group)
		}
		batch.ops = append(batch.ops, part.ops...)
		batch.guards = append(batch.guards, part.guards...)
		start = end
	}
	if len(batch.ops) > 0 {
		batches = append(batches, batch)
	}
	if len(commit) > limit {
		return nil, merr.WrapErrServiceInternalMsg("transaction commit set of %d ops exceeds the limit %d", len(commit), limit)
	}
	if len(commit) > 0 {
		batches = append(batches, txnBatch{ops: commit})
	}
	mlog.Warn(context.TODO(), "optimistic txn exceeds atomic limit, falling back to ordered batches",
		mlog.Int("total", len(t.ops)), mlog.Int("limit", limit), mlog.Int("batches", len(batches)))
	return batches, nil
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

func (p *etcdPersist[K, V]) requestContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return requestContext(ctx, &paramtable.Get().EtcdCfg.RequestTimeout)
}

func (p *etcdPersist[K, V]) Scan(ctx context.Context, prefix K) ([]K, []V, []int64, error) {
	var ks []K
	var vals []V
	var vers []int64
	// Linearizable: recovery must see every segment the previous leader
	// committed, which a lagging follower may not have applied yet.
	err := etcdkv.WalkPrefix(
		ctx,
		p.cli,
		string(prefix),
		paramtable.Get().MetaStoreCfg.PaginationSize.GetAsInt(),
		0,
		func(keyValue *mvccpb.KeyValue) error {
			value, err := decodeStoredValue(p.marshaler, string(keyValue.Key), keyValue.Value)
			if err != nil {
				return err
			}
			ks = append(ks, K(keyValue.Key))
			vals = append(vals, value)
			vers = append(vers, keyValue.ModRevision)
			return nil
		},
	)
	if err != nil {
		return nil, nil, nil, err
	}
	return ks, vals, vers, nil
}

func (p *etcdPersist[K, V]) Get(ctx context.Context, keys []K) (map[K]TxnResult[V], error) {
	result := make(map[K]TxnResult[V], len(keys))
	for _, key := range keys {
		reqCtx, cancel := p.requestContext(ctx)
		resp, err := p.cli.Get(reqCtx, string(key))
		cancel()
		if err != nil {
			return nil, err
		}
		if len(resp.Kvs) == 0 {
			continue
		}
		v, err := decodeStoredValue(p.marshaler, string(key), resp.Kvs[0].Value)
		if err != nil {
			return nil, err
		}
		result[key] = TxnResult[V]{Value: v, Version: resp.Kvs[0].ModRevision}
	}
	return result, nil
}

func (p *etcdPersist[K, V]) WalkRaw(ctx context.Context, prefix string, fn func(key string, value []byte) error) error {
	return etcdkv.WalkPrefix(ctx, p.cli, prefix, paramtable.Get().MetaStoreCfg.PaginationSize.GetAsInt(), 0,
		func(keyValue *mvccpb.KeyValue) error {
			return fn(string(keyValue.Key), keyValue.Value)
		})
}

func etcdRawOp(op RawOp) clientv3.Op {
	switch {
	case op.DeletePrefix:
		return clientv3.OpDelete(op.Key, clientv3.WithPrefix())
	case op.Delete:
		return clientv3.OpDelete(op.Key)
	default:
		return clientv3.OpPut(op.Key, op.Value)
	}
}

type etcdTxn[K string, V any] struct {
	txnOps[K, V]
	ctx     context.Context
	persist *etcdPersist[K, V]
}

func (t *etcdTxn[K, V]) Commit() ([]TxnResult[V], error) {
	results := make([]TxnResult[V], len(t.ops))

	// Split ops into batches to avoid exceeding etcd txn op limits.
	batches, err := t.planBatches(paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt())
	if err != nil {
		return nil, retry.Unrecoverable(err)
	}
	for i, batch := range batches {
		if err := t.commitBatch(batch, results); err != nil {
			if i == 0 {
				return nil, err
			}
			// The batches before this one landed.
			return results, err
		}
	}
	return results, nil
}

func (t *etcdTxn[K, V]) commitBatch(batch txnBatch, results []TxnResult[V]) error {
	commitStart := time.Now()
	retryCount := 0

	err := retry.Do(t.ctx, func() error {
		retryCount++
		ctx, cancel := t.persist.requestContext(t.ctx)
		defer cancel()
		cmps := make([]clientv3.Cmp, 0, len(batch.ops)+len(batch.guards))
		putOps := make([]clientv3.Op, 0, len(batch.ops)+len(batch.guards))
		written := make(map[int]bool, len(batch.ops))
		hasVersioned := len(batch.guards) > 0
		for _, i := range batch.guards {
			op := t.ops[i]
			cmps = append(cmps, etcdVersionCmp(string(op.key), op.version))
			if op.version != 0 {
				// The compare fails on an absent key, so the put never
				// misses the value it keeps.
				putOps = append(putOps, clientv3.OpPut(string(op.key), "", clientv3.WithIgnoreValue()))
			}
		}

		// Phase 1: read existing values of read-modify-write ops from etcd
		getStart := time.Now()
		for _, i := range batch.ops {
			op := t.ops[i]
			keyStr := string(op.key)
			switch op.kind {
			case opInsert:
				resp, err := t.persist.cli.Get(ctx, keyStr, clientv3.WithSerializable(), clientv3.WithKeysOnly())
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
				resp, err := t.persist.cli.Get(ctx, keyStr, clientv3.WithSerializable())
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
				resp, err := t.persist.cli.Get(ctx, keyStr, clientv3.WithSerializable())
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
				resp, err := t.persist.cli.Get(ctx, keyStr, clientv3.WithSerializable(), clientv3.WithKeysOnly())
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

			case opPutIfVersion:
				hasVersioned = true
				cmps = append(cmps, etcdVersionCmp(keyStr, op.version))
				valBytes, err := t.persist.marshaler.Marshal(op.value)
				if err != nil {
					return retry.Unrecoverable(err)
				}
				putOps = append(putOps, clientv3.OpPut(keyStr, string(valBytes)))
				results[i].Value = op.value
				written[i] = true

			case opDeleteIfVersion:
				hasVersioned = true
				cmps = append(cmps, etcdVersionCmp(keyStr, op.version))
				putOps = append(putOps, clientv3.OpDelete(keyStr))
				written[i] = true

			case opRaw:
				putOps = append(putOps, etcdRawOp(op.raw))
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
			txnResp, err = t.persist.cli.Txn(ctx).Then(putOps...).Commit()
		} else {
			txnResp, err = t.persist.cli.Txn(ctx).If(cmps...).Then(putOps...).Commit()
		}
		txnDur := time.Since(txnStart)

		totalDur := time.Since(commitStart)
		if totalDur > 40*time.Millisecond {
			mlog.Info(t.ctx, "etcdTxn.Commit slow",
				mlog.Duration("total", totalDur),
				mlog.Duration("etcdGet", getDur),
				mlog.Duration("etcdTxn", txnDur),
				mlog.Int("numOps", len(batch.ops)),
				mlog.Int("numPuts", len(putOps)),
				mlog.Int("retryCount", retryCount))
		}

		if err != nil {
			if hasVersioned {
				// The outcome is unknown: the request may have landed. Read it
				// back, so a landed batch succeeds and one that did not land
				// leaves the versions its resend compares against.
				applied, resolveErr := t.resolveUnknownOutcome(batch, results)
				if resolveErr == nil && applied {
					return nil
				}
				if errors.Is(resolveErr, ErrVersionConflict) {
					return retry.Unrecoverable(resolveErr)
				}
			}
			return err
		}
		if !txnResp.Succeeded {
			if hasVersioned {
				applied, err := t.resolveVersionedFailure(ctx, batch, results)
				if err != nil {
					if errors.Is(err, ErrVersionConflict) {
						return retry.Unrecoverable(err)
					}
					return err
				}
				if applied {
					return nil
				}
			}
			return errCompareFailed
		}
		for i, w := range written {
			if w {
				results[i].Version = txnResp.Header.Revision
			}
		}
		for _, i := range batch.guards {
			if t.ops[i].version != 0 {
				// The op expects the version its last touch left.
				t.ops[i].version = txnResp.Header.Revision
			}
		}
		return nil
	}, retry.Attempts(maxCompareAttempts), retry.RetryErr(func(err error) bool { return errors.Is(err, errCompareFailed) }))
	if errors.Is(err, errCompareFailed) {
		return merr.WrapErrServiceUnavailableErr(err, "optimistic txn batch kept failing its compares after %d attempts", maxCompareAttempts)
	}
	return storeErr(err)
}

// resolveUnknownOutcome reads a batch whose commit outcome is unknown back
// until the store answers, or the transaction's context ends. A batch that
// landed unseen would fail a write that took effect, and leave the cache
// behind the store until a later write of its segments reads them back.
func (t *etcdTxn[K, V]) resolveUnknownOutcome(batch txnBatch, results []TxnResult[V]) (bool, error) {
	backoff := 10 * time.Millisecond
	for {
		ctx, cancel := t.persist.requestContext(context.WithoutCancel(t.ctx))
		applied, err := t.resolveVersionedFailure(ctx, batch, results)
		cancel()
		if err == nil || errors.Is(err, ErrVersionConflict) {
			return applied, err
		}
		select {
		case <-time.After(backoff):
		case <-t.ctx.Done():
			return applied, err
		}
		backoff = min(2*backoff, time.Second)
	}
}

// resolveVersionedFailure rereads the keys of the batch's versioned ops and
// guards after a failed compare. It reports ErrVersionConflict when one of
// them moved to a value this batch did not write, or a guard's key moved at
// all: a touch keeps the value, so another writer's write cannot be told from
// this batch's touch, and the write is built again from the store. It
// reports (false, nil) when all of them are still at their expected versions
// (the read-modify-write compares failed: retry), and (true, nil) when the
// whole batch is found already applied - an earlier attempt of this batch
// landed but its response was lost. In that case the results are filled from
// the store.
//
// A versioned op counts as applied when the stored value equals its value, and
// the batch only when its raw ops are found applied too: the catalog encoding
// keeps a segment's binlogs out of its record, so another writer may have
// stored an equal record with other binlogs.
func (t *etcdTxn[K, V]) resolveVersionedFailure(ctx context.Context, batch txnBatch, results []TxnResult[V]) (bool, error) {
	unchanged, applied := 0, 0
	for _, i := range batch.guards {
		op := t.ops[i]
		resp, err := t.persist.cli.Get(ctx, string(op.key), clientv3.WithKeysOnly())
		if err != nil {
			return false, err
		}
		current := int64(0)
		if len(resp.Kvs) > 0 {
			current = resp.Kvs[0].ModRevision
		}
		if current != op.version {
			return false, errors.Wrapf(ErrVersionConflict, "key %s: expected version %d, current %d", string(op.key), op.version, current)
		}
		unchanged++
	}
	revisions := make(map[int]int64, len(batch.ops))
	var rawOps []RawOp
	for _, i := range batch.ops {
		op := t.ops[i]
		switch {
		case op.kind == opRaw:
			rawOps = append(rawOps, op.raw)
			continue
		case !op.versioned():
			// Mixed batches cannot tell a lost success from a conflict.
			continue
		}
		resp, err := t.persist.cli.Get(ctx, string(op.key))
		if err != nil {
			return false, err
		}
		exists := len(resp.Kvs) > 0
		current := int64(0)
		if exists {
			current = resp.Kvs[0].ModRevision
		}
		switch {
		case current == op.version:
			unchanged++
		case op.kind == opDeleteIfVersion && !exists:
			applied++
		case op.kind == opPutIfVersion && exists && storedValueEquals(t.persist.marshaler, resp.Kvs[0].Value, op.value):
			applied++
			revisions[i] = current
		default:
			return false, errors.Wrapf(ErrVersionConflict, "key %s: expected version %d, current %d", string(op.key), op.version, current)
		}
	}
	if applied == 0 {
		return false, nil
	}
	if unchanged > 0 || applied+len(rawOps) != len(batch.ops) {
		return false, errors.Wrapf(ErrVersionConflict, "batch partially applied by another writer")
	}
	for _, op := range rawOps {
		landed, err := t.persist.rawOpLanded(ctx, op)
		if err != nil {
			return false, err
		}
		if !landed {
			return false, errors.Wrapf(ErrVersionConflict, "key %s: another writer stored the values of the batch's versioned ops", op.Key)
		}
	}
	for i, revision := range revisions {
		results[i].Value = t.ops[i].value
		results[i].Version = revision
	}
	mlog.Info(t.ctx, "etcdTxn.Commit found an earlier attempt applied", mlog.Int("numOps", len(batch.ops)))
	return true, nil
}

// rawOpLanded reports whether the store holds what op writes: its value, or
// no key where it deletes.
func (p *etcdPersist[K, V]) rawOpLanded(ctx context.Context, op RawOp) (bool, error) {
	if op.Delete || op.DeletePrefix {
		opts := []clientv3.OpOption{clientv3.WithCountOnly()}
		if op.DeletePrefix {
			opts = append(opts, clientv3.WithPrefix())
		}
		resp, err := p.cli.Get(ctx, op.Key, opts...)
		if err != nil {
			return false, err
		}
		return resp.Count == 0, nil
	}
	resp, err := p.cli.Get(ctx, op.Key)
	if err != nil {
		return false, err
	}
	return len(resp.Kvs) > 0 && string(resp.Kvs[0].Value) == op.Value, nil
}

func etcdVersionCmp(key string, version int64) clientv3.Cmp {
	if version == 0 {
		return clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
	}
	return clientv3.Compare(clientv3.ModRevision(key), "=", version)
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

func (p *tikvPersist[K, V]) requestContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return requestContext(ctx, &paramtable.Get().TiKVCfg.RequestTimeout)
}

// tikvScan calls fn with every key and value under prefix in txn.
func tikvScan(txn *transaction.KVTxn, prefix string, fn func(key, value []byte) error) error {
	iter, err := txn.Iter([]byte(prefix), tikvkv.PrefixNextKey([]byte(prefix)))
	if err != nil {
		return err
	}
	defer iter.Close()
	for iter.Valid() {
		if err := fn(iter.Key(), iter.Value()); err != nil {
			return err
		}
		if err := iter.Next(); err != nil {
			return err
		}
	}
	return nil
}

// isTiKVRetriable reports a commit that lost to a concurrent writer or whose
// outcome is unknown; a retry rereads the versions and recognizes its own
// landed write.
func isTiKVRetriable(err error) bool {
	var latch *tikverr.ErrWriteConflictInLatch
	return tikverr.IsErrWriteConflict(err) || errors.As(err, &latch) || tikverr.IsErrorUndetermined(err)
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
}, key []byte,
) ([]byte, bool, error) {
	entry, err := txn.Get(ctx, key)
	if err != nil {
		if tikverr.IsErrNotFound(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return entry.Value, true, nil
}

// tikvKeyWithCommitTS reads the key's latest value and its commit timestamp
// inside txn.
func (p *tikvPersist[K, V]) tikvKeyWithCommitTS(ctx context.Context, txn *transaction.KVTxn, key []byte) (tikvkv.ValueEntry, bool, error) {
	entry, err := txn.Get(ctx, key, tikvkv.WithReturnCommitTS())
	if err != nil {
		if tikverr.IsErrNotFound(err) {
			return tikvkv.ValueEntry{}, false, nil
		}
		return tikvkv.ValueEntry{}, false, err
	}
	return entry, true, nil
}

func (p *tikvPersist[K, V]) Scan(ctx context.Context, prefix K) ([]K, []V, []int64, error) {
	txn, err := p.cli.Begin()
	if err != nil {
		return nil, nil, nil, err
	}
	defer txn.Rollback()

	var ks []K
	var vals []V
	var vers []int64
	err = tikvScan(txn, string(prefix), func(key, value []byte) error {
		v, err := decodeStoredValue(p.marshaler, string(key), value)
		if err != nil {
			return err
		}
		ks = append(ks, K(key))
		vals = append(vals, v)
		vers = append(vers, int64(txn.StartTS()))
		return nil
	})
	if err != nil {
		return nil, nil, nil, err
	}
	return ks, vals, vers, nil
}

func (p *tikvPersist[K, V]) Get(ctx context.Context, keys []K) (map[K]TxnResult[V], error) {
	txn, err := p.cli.Begin()
	if err != nil {
		return nil, err
	}
	defer txn.Rollback()
	result := make(map[K]TxnResult[V], len(keys))
	for _, key := range keys {
		reqCtx, cancel := p.requestContext(ctx)
		val, exists, err := p.tikvKeyExists(reqCtx, txn, []byte(key))
		cancel()
		if err != nil {
			return nil, err
		}
		if !exists {
			continue
		}
		v, err := decodeStoredValue(p.marshaler, string(key), val)
		if err != nil {
			return nil, err
		}
		// Like Scan, the snapshot timestamp bounds the commit ts of every value read.
		result[key] = TxnResult[V]{Value: v, Version: int64(txn.StartTS())}
	}
	return result, nil
}

func (p *tikvPersist[K, V]) WalkRaw(ctx context.Context, prefix string, fn func(key string, value []byte) error) error {
	txn, err := p.cli.Begin()
	if err != nil {
		return err
	}
	defer txn.Rollback()
	return tikvScan(txn, prefix, func(key, value []byte) error {
		return fn(string(key), value)
	})
}

type tikvTxn[K string, V any] struct {
	txnOps[K, V]
	ctx     context.Context
	persist *tikvPersist[K, V]
}

func (t *tikvTxn[K, V]) Commit() ([]TxnResult[V], error) {
	results := make([]TxnResult[V], len(t.ops))

	err := retry.Do(t.ctx, func() error {
		ctx, cancel := t.persist.requestContext(t.ctx)
		defer cancel()
		txn, err := t.persist.cli.Begin()
		if err != nil {
			return err
		}
		defer txn.Rollback()

		anyWrite := false
		versionedWritten, versionedApplied := 0, 0
		for i, op := range t.ops {
			keyBytes := []byte(op.key)
			switch op.kind {
			case opRaw:
				switch {
				case op.raw.DeletePrefix:
					err = tikvDeletePrefix(txn, op.raw.Key)
				case op.raw.Delete:
					err = txn.Delete([]byte(op.raw.Key))
				default:
					err = txn.Set([]byte(op.raw.Key), []byte(op.raw.Value))
				}
				if err != nil {
					return err
				}
				anyWrite = true
				continue
			case opPutIfVersion, opDeleteIfVersion:
				// The versions handed out by this persist are snapshot or
				// commit timestamps, so a value committed after the caller's
				// version is newer than the copy the write was built from.
				entry, exists, err := t.persist.tikvKeyWithCommitTS(ctx, txn, keyBytes)
				if err != nil {
					return err
				}
				if (op.version == 0 && exists) ||
					(op.version != 0 && (!exists || (entry.CommitTS != 0 && int64(entry.CommitTS) > op.version))) {
					// An earlier attempt whose response was lost may have
					// written exactly this op already.
					if op.kind == opDeleteIfVersion && !exists {
						versionedApplied++
						continue
					}
					if op.kind == opPutIfVersion && exists && storedValueEquals(t.persist.marshaler, entry.Value, op.value) {
						versionedApplied++
						results[i].Value = op.value
						results[i].Version = int64(entry.CommitTS)
						continue
					}
					return retry.Unrecoverable(errors.Wrapf(ErrVersionConflict,
						"key %s: expected version %d, current commit ts %d", string(op.key), op.version, entry.CommitTS))
				}
				versionedWritten++
				if op.kind == opDeleteIfVersion {
					if err := txn.Delete(keyBytes); err != nil {
						return err
					}
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
				continue
			}

			val, exists, err := t.persist.tikvKeyExists(ctx, txn, keyBytes)
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

		if versionedApplied > 0 && versionedWritten > 0 {
			return retry.Unrecoverable(errors.Wrapf(ErrVersionConflict, "transaction partially applied by another writer"))
		}
		if versionedApplied > 0 {
			// The stored values equal what the versioned ops write: an earlier
			// attempt landed if the raw ops did too (see
			// etcdTxn.resolveVersionedFailure). Nothing is committed.
			for _, op := range t.ops {
				if op.kind != opRaw && !op.versioned() {
					return retry.Unrecoverable(errors.Wrapf(ErrVersionConflict, "transaction partially applied by another writer"))
				}
				if op.kind != opRaw {
					continue
				}
				landed, err := tikvRawOpLanded(ctx, txn.GetSnapshot(), op.raw)
				if err != nil {
					return err
				}
				if !landed {
					return retry.Unrecoverable(errors.Wrapf(ErrVersionConflict,
						"key %s: another writer stored the values of the transaction's versioned ops", op.raw.Key))
				}
			}
			return nil
		}

		if !anyWrite {
			return nil
		}

		cts := t.persist.captureCommitTS(txn)
		err = txn.Commit(ctx)
		if err == nil {
			for i := range t.ops {
				if results[i].Version == 0 {
					results[i].Version = int64(*cts)
				}
			}
		}
		return err
	}, retry.Attempts(maxCompareAttempts), retry.RetryErr(isTiKVRetriable))
	if isTiKVRetriable(err) {
		return nil, merr.WrapErrServiceUnavailableErr(err, "optimistic txn kept conflicting after %d attempts", maxCompareAttempts)
	}
	if err != nil {
		return nil, storeErr(err)
	}
	return results, nil
}

// tikvRawOpLanded reports whether snapshot holds what op writes: its value, or
// no key where it deletes.
func tikvRawOpLanded(ctx context.Context, snapshot *txnsnapshot.KVSnapshot, op RawOp) (bool, error) {
	if op.DeletePrefix {
		iter, err := snapshot.Iter([]byte(op.Key), tikvkv.PrefixNextKey([]byte(op.Key)))
		if err != nil {
			return false, err
		}
		defer iter.Close()
		return !iter.Valid(), nil
	}
	entry, err := snapshot.Get(ctx, []byte(op.Key))
	if tikverr.IsErrNotFound(err) {
		return op.Delete, nil
	}
	if err != nil {
		return false, err
	}
	return !op.Delete && string(entry.Value) == op.Value, nil
}

// tikvDeletePrefix deletes every key under prefix in txn.
func tikvDeletePrefix(txn *transaction.KVTxn, prefix string) error {
	var keys [][]byte
	err := tikvScan(txn, prefix, func(key, _ []byte) error {
		keys = append(keys, append([]byte(nil), key...))
		return nil
	})
	if err != nil {
		return err
	}
	for _, key := range keys {
		if err := txn.Delete(key); err != nil {
			return err
		}
	}
	return nil
}

// ============================================================
// In-memory implementation (for testing)
// ============================================================

type memEntry[V any] struct {
	value   V
	version int64
}

type memPersist[K comparable, V any] struct {
	// mu serializes transactions like the store would: meta commits writes of
	// different segments concurrently.
	mu        sync.RWMutex
	data      map[K]*memEntry[V]
	raw       map[string]string
	nextVer   int64
	marshaler Marshaler[V]
}

func NewOptimisticTxnMemoryPersist[K comparable, V any](marshaler Marshaler[V]) OptimisticTxnPersist[K, V] {
	return &memPersist[K, V]{
		data:      make(map[K]*memEntry[V]),
		raw:       make(map[string]string),
		nextVer:   1,
		marshaler: marshaler,
	}
}

func (p *memPersist[K, V]) clone(value V) (V, error) {
	data, err := p.marshaler.Marshal(value)
	if err != nil {
		var zero V
		return zero, err
	}
	return p.marshaler.Unmarshal(data)
}

func (p *memPersist[K, V]) Txn(ctx context.Context) Txn[K, V] {
	return &memTxn[K, V]{persist: p}
}

func (p *memPersist[K, V]) Scan(ctx context.Context, prefix K) ([]K, []V, []int64, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	prefixStr := fmt.Sprintf("%v", prefix)
	var ks []K
	var vals []V
	var vers []int64
	for k, entry := range p.data {
		keyStr := fmt.Sprintf("%v", k)
		if strings.HasPrefix(keyStr, prefixStr) {
			value, err := p.clone(entry.value)
			if err != nil {
				return nil, nil, nil, err
			}
			ks = append(ks, k)
			vals = append(vals, value)
			vers = append(vers, entry.version)
		}
	}
	return ks, vals, vers, nil
}

func (p *memPersist[K, V]) Get(ctx context.Context, keys []K) (map[K]TxnResult[V], error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make(map[K]TxnResult[V], len(keys))
	for _, key := range keys {
		if entry, ok := p.data[key]; ok {
			value, err := p.clone(entry.value)
			if err != nil {
				return nil, err
			}
			result[key] = TxnResult[V]{Value: value, Version: entry.version}
		}
	}
	return result, nil
}

func (p *memPersist[K, V]) WalkRaw(ctx context.Context, prefix string, fn func(key string, value []byte) error) error {
	p.mu.RLock()
	keys := make([]string, 0)
	for key := range p.raw {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	values := make([]string, len(keys))
	for i, key := range keys {
		values[i] = p.raw[key]
	}
	p.mu.RUnlock()
	for i, key := range keys {
		if err := fn(key, []byte(values[i])); err != nil {
			return err
		}
	}
	return nil
}

type memTxn[K comparable, V any] struct {
	txnOps[K, V]
	persist *memPersist[K, V]
}

func (t *memTxn[K, V]) Commit() ([]TxnResult[V], error) {
	p := t.persist
	p.mu.Lock()
	defer p.mu.Unlock()
	// Validate all ops first (don't partially apply).
	for _, op := range t.ops {
		switch op.kind {
		case opInsert:
			if _, ok := p.data[op.key]; ok {
				return nil, merr.Wrapf(ErrKeyAlreadyExists, "key %v", op.key)
			}
		case opUpdate:
			if _, ok := p.data[op.key]; !ok {
				return nil, merr.Wrapf(ErrKeyNotFound, "key %v", op.key)
			}
		case opDelete:
			if _, ok := p.data[op.key]; !ok {
				return nil, merr.Wrapf(ErrKeyNotFound, "key %v", op.key)
			}
		case opPutIfVersion, opDeleteIfVersion:
			current := int64(0)
			if entry, ok := p.data[op.key]; ok {
				current = entry.version
			}
			if current != op.version {
				return nil, errors.Wrapf(ErrVersionConflict, "key %v: expected version %d, current %d", op.key, op.version, current)
			}
		case opUpsert, opRaw:
			// always valid
		}
	}

	ver := p.nextVer
	p.nextVer++
	results := make([]TxnResult[V], len(t.ops))

	for i, op := range t.ops {
		switch op.kind {
		case opInsert, opPutIfVersion:
			stored, err := p.clone(op.value)
			if err != nil {
				return nil, err
			}
			p.data[op.key] = &memEntry[V]{value: stored, version: ver}
			results[i] = TxnResult[V]{Value: op.value, Version: ver}

		case opUpdate:
			entry := p.data[op.key]
			existing, err := p.clone(entry.value)
			if err != nil {
				return nil, err
			}
			newV, shouldWrite := op.updateFunc(existing)
			if !shouldWrite {
				results[i] = TxnResult[V]{Value: existing, Version: entry.version}
				continue
			}
			p.data[op.key] = &memEntry[V]{value: newV, version: ver}
			results[i] = TxnResult[V]{Value: newV, Version: ver}

		case opUpsert:
			if entry, ok := p.data[op.key]; ok {
				existing, err := p.clone(entry.value)
				if err != nil {
					return nil, err
				}
				newV, shouldWrite := op.updateFunc(existing)
				if !shouldWrite {
					results[i] = TxnResult[V]{Value: existing, Version: entry.version}
					continue
				}
				p.data[op.key] = &memEntry[V]{value: newV, version: ver}
				results[i] = TxnResult[V]{Value: newV, Version: ver}
			} else {
				stored, err := p.clone(op.value)
				if err != nil {
					return nil, err
				}
				p.data[op.key] = &memEntry[V]{value: stored, version: ver}
				results[i] = TxnResult[V]{Value: op.value, Version: ver}
			}

		case opDelete, opDeleteIfVersion:
			delete(p.data, op.key)
			results[i] = TxnResult[V]{Version: ver}

		case opRaw:
			switch {
			case op.raw.DeletePrefix:
				for key := range p.raw {
					if strings.HasPrefix(key, op.raw.Key) {
						delete(p.raw, key)
					}
				}
			case op.raw.Delete:
				delete(p.raw, op.raw.Key)
			default:
				p.raw[op.raw.Key] = op.raw.Value
			}
		}
	}

	return results, nil
}
