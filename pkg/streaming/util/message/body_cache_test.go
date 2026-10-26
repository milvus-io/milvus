package message

import (
	"context"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
)

// Keep state-machine fixtures compact; production obtains this length from
// decodePayload, and the wire-accounting tests below exercise that real path.
func (m *bodyCacheManager) getTestBody(ctx context.Context, slot *bodyCacheSlot, decode func() (proto.Message, error)) (proto.Message, error) {
	return m.get(ctx, slot, func() (proto.Message, int, error) {
		body, err := decode()
		size := 0
		if err == nil {
			size = proto.Size(body)
		}
		return body, size, err
	})
}

func TestBodyCacheConcurrentConstruction(t *testing.T) {
	m := newBodyCacheManager(1<<20, time.Hour, time.Hour)
	defer m.close()
	slot := &bodyCacheSlot{}
	started, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	decode := func() (proto.Message, error) {
		calls.Add(1)
		close(started)
		<-release
		return &msgpb.InsertRequest{NumRows: 7}, nil
	}
	const readers = 32
	bodies := make([]proto.Message, readers)
	errs := make([]error, readers)
	var wg sync.WaitGroup
	for i := range bodies {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			bodies[i], errs[i] = m.getTestBody(context.Background(), slot, decode)
		}(i)
	}
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	waiter := make(chan error, 1)
	go func() {
		_, err := m.getTestBody(ctx, slot, decode)
		waiter <- err
	}()
	cancel()
	require.ErrorIs(t, <-waiter, context.Canceled)
	close(release)
	wg.Wait()
	require.EqualValues(t, 1, calls.Load())
	for i := range bodies {
		require.NoError(t, errs[i])
		require.Same(t, bodies[0], bodies[i])
	}
}

func TestBodyCacheLoaderCancellation(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "cancel"
		if deadline {
			name = "deadline"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				m := newBodyCacheManager(1<<20, time.Hour, time.Hour)
				defer m.close()
				slot := &bodyCacheSlot{}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				var calls atomic.Int32
				loaderDone := make(chan error, 1)
				go func() {
					_, err := m.getTestBody(ctx, slot, func() (proto.Message, error) {
						calls.Add(1)
						<-ctx.Done()
						return nil, ctx.Err()
					})
					loaderDone <- err
				}()
				synctest.Wait()

				const readers = 16
				bodies := make([]proto.Message, readers)
				errs := make([]error, readers)
				var wg sync.WaitGroup
				for i := range bodies {
					wg.Go(func() {
						bodies[i], errs[i] = m.getTestBody(context.Background(), slot, func() (proto.Message, error) {
							calls.Add(1)
							return &msgpb.InsertRequest{NumRows: 7}, nil
						})
					})
				}
				// All readers must join the original attempt before it fails.
				waiterCtx, cancelWaiter := context.WithCancel(context.Background())
				defer cancelWaiter()
				waiterDone := make(chan error, 1)
				go func() {
					_, err := m.getTestBody(waiterCtx, slot, func() (proto.Message, error) {
						panic("canceled waiter must not decode")
					})
					waiterDone <- err
				}()
				synctest.Wait()
				cancelWaiter()
				require.ErrorIs(t, <-waiterDone, context.Canceled)
				require.EqualValues(t, 1, calls.Load(), "waiter cancellation must not affect the loader")
				if deadline {
					time.Sleep(time.Second)
				} else {
					cancel()
				}
				require.ErrorIs(t, <-loaderDone, ctx.Err())
				wg.Wait()
				for i := range bodies {
					require.NoError(t, errs[i])
					require.Same(t, bodies[0], bodies[i])
				}
				require.EqualValues(t, 2, calls.Load(), "waiters share a single replacement decode")
			})
		})
	}
}

func TestBodyCacheSharedDecodeError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := newBodyCacheManager(1<<20, time.Hour, time.Hour)
		defer m.close()
		slot := &bodyCacheSlot{}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		release := make(chan struct{})
		var calls atomic.Int32
		decode := func() (proto.Message, error) {
			calls.Add(1)
			<-release
			return nil, ErrMalformedBody
		}
		const readers = 16
		errs := make(chan error, readers+1)
		go func() {
			_, err := m.getTestBody(ctx, slot, decode)
			errs <- err
		}()
		synctest.Wait()
		for range readers {
			go func() {
				_, err := m.getTestBody(context.Background(), slot, decode)
				errs <- err
			}()
		}
		synctest.Wait()
		// An unrelated decoding failure must propagate even if its caller cancels.
		cancel()
		close(release)
		for range readers + 1 {
			require.ErrorIs(t, <-errs, ErrMalformedBody)
		}
		require.EqualValues(t, 1, calls.Load(), "ordinary errors must not trigger waiter retries")
	})
}

func TestBodyCacheRetryEvictionAndAdmission(t *testing.T) {
	m := newBodyCacheManager(800, time.Minute, time.Hour)
	defer m.close()
	slot := &bodyCacheSlot{}
	partial := &msgpb.InsertRequest{NumRows: 7}
	//nolint:staticcheck // Verify the existing decoder's nil-context compatibility.
	body, err := m.getTestBody(nil, slot, func() (proto.Message, error) { return partial, context.Canceled })
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, body)
	require.Nil(t, slot.body)
	decode := func() (proto.Message, error) { return &msgpb.InsertRequest{NumRows: 7}, nil }
	body, err = m.getTestBody(context.Background(), slot, decode)
	require.NoError(t, err)
	require.NotSame(t, partial, body)
	hit, err := m.getTestBody(context.Background(), slot, decode)
	require.NoError(t, err)
	require.Same(t, body, hit)
	m.recycle(time.Now())
	require.Same(t, body, slot.body)
	m.recycle(time.Now().Add(time.Minute))
	require.Zero(t, m.bytes.Load())
	require.Zero(t, m.shards[0].entries.Len())
	require.EqualValues(t, 7, body.(*msgpb.InsertRequest).NumRows, "eviction cannot reset readers' bodies")
	rebuilt, err := m.getTestBody(context.Background(), slot, decode)
	require.NoError(t, err)
	require.NotSame(t, body, rebuilt)
	require.True(t, proto.Equal(body, rebuilt))

	oversized := &bodyCacheSlot{}
	large := &msgpb.InsertRequest{RowIDs: make([]int64, 1024)}
	result, err := m.getTestBody(context.Background(), oversized, func() (proto.Message, error) { return large, nil })
	require.NoError(t, err)
	require.Same(t, large, result)
	require.Nil(t, oversized.body)
	require.Same(t, rebuilt, slot.body, "oversized bypass must not evict cached bodies")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = m.getTestBody(ctx, slot, decode)
	require.ErrorIs(t, err, context.Canceled, "hits must honor cancellation too")
}

func TestBodyCacheAdmissionEvictsLRU(t *testing.T) {
	m := newBodyCacheManager(800, time.Hour, time.Hour)
	defer m.close()
	calls := 0
	decode := func() (proto.Message, error) {
		calls++
		return &msgpb.InsertRequest{NumRows: 7}, nil
	}
	slots := []*bodyCacheSlot{{}, {}, {}}
	var retained proto.Message
	for i, slot := range slots {
		body, err := m.getTestBody(context.Background(), slot, decode)
		require.NoError(t, err)
		if i == 1 {
			retained = body
		}
	}
	_, err := m.getTestBody(context.Background(), slots[0], decode) // refresh the oldest entry
	require.NoError(t, err)
	slot := &bodyCacheSlot{}
	body, err := m.getTestBody(context.Background(), slot, decode)
	require.NoError(t, err)
	require.Nil(t, slots[1].body, "evict the least recently accessed entry")
	require.NotNil(t, slots[0].body)
	require.NotNil(t, slots[2].body)
	require.Same(t, body, slot.body, "admit the completed decode before returning")
	require.Equal(t, 3, m.shards[0].entries.Len())
	require.LessOrEqual(t, m.bytes.Load(), m.budget)
	hit, err := m.getTestBody(context.Background(), slot, decode)
	require.NoError(t, err)
	require.Same(t, body, hit)
	require.Equal(t, 4, calls, "capacity pressure must not discard the new decode")
	require.EqualValues(t, 7, retained.(*msgpb.InsertRequest).NumRows)
}

func TestBodyCacheIdleRecycler(t *testing.T) {
	idle := newBodyCacheManager(800, time.Millisecond, time.Millisecond)
	defer idle.close()
	_, err := idle.getTestBody(context.Background(), &bodyCacheSlot{}, func() (proto.Message, error) { return &msgpb.InsertRequest{}, nil })
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		idle.shards[0].mu.Lock()
		defer idle.shards[0].mu.Unlock()
		return idle.shards[0].entries.Len() == 0 && idle.bytes.Load() == 0
	}, time.Second, time.Millisecond)
}

func TestBodyCacheCloseDuringConstruction(t *testing.T) {
	m := newBodyCacheManager(1<<20, time.Hour, time.Hour)
	slot := &bodyCacheSlot{}
	started, release := make(chan struct{}), make(chan struct{})
	done := make(chan proto.Message, 1)
	go func() {
		body, _ := m.getTestBody(context.Background(), slot, func() (proto.Message, error) {
			close(started)
			<-release
			return &msgpb.InsertRequest{NumRows: 7}, nil
		})
		done <- body
	}()
	<-started
	m.close()
	m.close()
	close(release)
	require.EqualValues(t, 7, (<-done).(*msgpb.InsertRequest).NumRows)
	require.Nil(t, slot.body)
	require.Zero(t, m.shards[0].entries.Len())
}

func TestBodyCacheAdmissionEvictsMultipleEntries(t *testing.T) {
	m := newBodyCacheManager(1024, time.Hour, time.Hour)
	defer m.close()
	slots := []*bodyCacheSlot{{}, {}}
	for _, slot := range slots {
		_, err := m.getTestBody(context.Background(), slot, func() (proto.Message, error) { return &msgpb.InsertRequest{}, nil })
		require.NoError(t, err)
	}
	// Even a half-full cache may need to evict several entries for one body.
	slot := &bodyCacheSlot{}
	decode := func() (proto.Message, error) { return &msgpb.InsertRequest{RowIDs: make([]int64, 256)}, nil }
	body, err := m.getTestBody(context.Background(), slot, decode)
	require.NoError(t, err)
	for _, evicted := range slots {
		require.Nil(t, evicted.body)
	}
	require.Equal(t, 1, m.shards[0].entries.Len())
	require.Same(t, body, slot.body)
	require.LessOrEqual(t, m.bytes.Load(), m.budget)
}

func TestBodyCacheConcurrentEviction(t *testing.T) {
	m := newBodyCacheManager(1024, time.Millisecond, time.Millisecond)
	defer m.close()
	decode := func() (proto.Message, error) { return &msgpb.InsertRequest{RowIDs: []int64{1, 2, 3}}, nil }
	var wg sync.WaitGroup
	for range 8 {
		slot := &bodyCacheSlot{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				body, err := m.getTestBody(context.Background(), slot, decode)
				if err != nil {
					t.Errorf("get body: %v", err)
					return
				}
				m.recycle(time.Now().Add(time.Hour))
				require.Equal(t, []int64{1, 2, 3}, body.(*msgpb.InsertRequest).RowIDs)
			}
		}()
	}
	wg.Wait()
}

func TestImmutableBodyCacheIdentity(t *testing.T) {
	mutable := NewInsertMessageBuilderV1().WithVChannel("v1").
		WithHeader(&InsertMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.InsertRequest{NumRows: 7}).MustBuildMutable()
	raw := mutable.WithTimeTick(1).WithLastConfirmedUseMessageID().IntoImmutableMessage(nil)
	first := MustAsImmutableInsertMessageV1(raw).MustBody()
	require.Same(t, first, MustAsImmutableInsertMessageV1(raw).MustBody())
	clone := raw.(*immutableMessageImpl).clone()
	clone.overwriteTimeTick(2)
	require.Same(t, first, MustAsImmutableInsertMessageV1(clone).MustBody())
	require.EqualValues(t, 2, clone.TimeTick())
	reconstructed := NewImmutableMesasge(nil, raw.Payload(), raw.Properties().ToRawMap())
	other := MustAsImmutableInsertMessageV1(reconstructed).MustBody()
	require.NotSame(t, first, other)
	require.True(t, proto.Equal(first, other))
	private := MustAsMutableInsertMessageV1(mutable).MustBody()
	private.NumRows = 9
	require.EqualValues(t, 7, first.NumRows)
	require.EqualValues(t, 7, MustAsMutableInsertMessageV1(mutable).MustBody().NumRows)
}

func TestImmutableBodyCacheDecodeRetry(t *testing.T) {
	mutable := NewInsertMessageBuilderV1().WithVChannel("v1").
		WithHeader(&InsertMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.InsertRequest{NumRows: 7}).MustBuildMutable()
	raw := mutable.WithTimeTick(1).WithLastConfirmedUseMessageID().IntoImmutableMessage(nil)
	msg := MustAsImmutableInsertMessageV1(raw)
	var calls int
	patch := mockey.Mock((*messageImpl).decodePayload).To(func(m *messageImpl, _ context.Context) ([]byte, error) {
		calls++
		if calls == 1 {
			return nil, context.Canceled
		}
		return m.payload, nil
	}).Build()
	defer patch.UnPatch()
	body, err := msg.Body(context.Background())
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, body)
	body, err = msg.Body(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, 7, body.NumRows)
	require.Same(t, body, msg.MustBody())
	require.Equal(t, 2, calls, "failed decoding is retried; a successful decoding is reused")
	patch.UnPatch()
	corrupted := MustAsImmutableInsertMessageV1(NewImmutableMesasge(nil, []byte{0xff}, raw.Properties().ToRawMap()))
	body, err = corrupted.Body(context.Background())
	require.ErrorIs(t, err, ErrMalformedBody)
	require.Nil(t, body)
}

func TestImmutableBodyCacheDecryptsOnce(t *testing.T) {
	mutable := NewInsertMessageBuilderV1().WithVChannel("v1").
		WithHeader(&InsertMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.InsertRequest{NumRows: 7}).MustBuildMutable()
	properties := maps.Clone(mutable.Properties().ToRawMap())
	header, err := EncodeProto(&messagespb.CipherHeader{EzId: 1, CollectionId: 1})
	require.NoError(t, err)
	properties[messageCipherHeader] = header
	decrypt := mockey.Mock((*mockDecryptor).Decrypt).Return(mutable.Payload(), nil).Build()
	defer decrypt.UnPatch()
	getCipher := mockey.Mock(getCipher).Return(&mockCipher{}, nil).Build()
	defer getCipher.UnPatch()
	getDecryptor := mockey.Mock((*mockCipher).GetDecryptor).Return(&mockDecryptor{}, nil).Build()
	defer getDecryptor.UnPatch()
	raw := NewImmutableMesasge(nil, []byte("encrypted WAL payload"), properties)
	first := MustAsImmutableInsertMessageV1(raw).MustBody()
	require.EqualValues(t, 7, first.NumRows)
	require.Same(t, first, MustAsImmutableInsertMessageV1(raw).MustBody())
	require.Equal(t, 1, decrypt.Times())
	require.Equal(t, 1, getDecryptor.Times())
	slot := raw.(*immutableMessageImpl).bodyCache
	manager := globalBodyCache()
	manager.shards[slot.shard].mu.Lock()
	require.EqualValues(t, bodyCacheEntryOverhead+2*len(mutable.Payload()), slot.bytes, "charge plaintext, not ciphertext length")
	manager.shards[slot.shard].mu.Unlock()
}

func BenchmarkImmutableBodyCache(b *testing.B) {
	mutable := NewInsertMessageBuilderV1().WithVChannel("v1").
		WithHeader(&InsertMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.InsertRequest{RowIDs: make([]int64, 64*1024)}).MustBuildMutable()
	immutable := MustAsImmutableInsertMessageV1(mutable.WithTimeTick(1).WithLastConfirmedUseMessageID().IntoImmutableMessage(nil))
	immutable.MustBody()
	b.Run("cached", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			immutable.MustBody()
		}
	})
	b.Run("decode", func(b *testing.B) {
		msg := MustAsMutableInsertMessageV1(mutable)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			msg.MustBody()
		}
	})
}

func TestBodyCacheShardIsolation(t *testing.T) {
	for _, warm := range []bool{false, true} {
		m := newBodyCacheManager(4096, time.Hour, time.Hour)
		slot := &bodyCacheSlot{shard: 1}
		decode := func() (proto.Message, int, error) { return &msgpb.InsertRequest{NumRows: 7}, 2, nil }
		if warm {
			_, err := m.get(context.Background(), slot, decode)
			require.NoError(t, err)
		}
		m.shards[0].mu.Lock()
		done := make(chan error, 1)
		go func() {
			_, err := m.get(context.Background(), slot, decode)
			done <- err
		}()
		var err error
		var completed bool
		select {
		case err = <-done:
			completed = true
		case <-time.After(time.Second):
		}
		m.shards[0].mu.Unlock()
		if !completed {
			<-done
		}
		m.close()
		require.True(t, completed, "an unrelated shard must not block a hit or a cold publication with available capacity")
		require.NoError(t, err)
	}
}

func TestBodyCacheSharedBudgetAndLargeAdmission(t *testing.T) {
	const entries = bodyCacheShardCount * (bodyCacheEvictionBatch + 1)
	m := newBodyCacheManager(entries*bodyCacheEntryOverhead, time.Hour, time.Hour)
	defer m.close()
	slots := make([]*bodyCacheSlot, entries)
	decode := func() (proto.Message, int, error) { return &msgpb.InsertRequest{}, 0, nil }
	for i := range slots {
		slots[i] = newBodyCacheSlot()
		_, err := m.get(context.Background(), slots[i], decode)
		require.NoError(t, err)
	}
	require.Equal(t, m.budget, m.bytes.Load())
	// This entry needs substantially more than a single shard's share, and
	// requires multiple eviction batches across different shards.
	large := newBodyCacheSlot()
	wireSize := int(m.budget / 3)
	body, err := m.get(context.Background(), large, func() (proto.Message, int, error) {
		return &msgpb.InsertRequest{NumRows: 9}, wireSize, nil
	})
	require.NoError(t, err)
	require.Same(t, body, large.body)
	require.LessOrEqual(t, m.bytes.Load(), m.budget)
	var sum int64
	for i := range m.shards {
		shard := &m.shards[i]
		for e := shard.entries.Front(); e != nil; e = e.Next() {
			sum += e.Value.(*bodyCacheSlot).bytes
		}
	}
	require.Equal(t, sum, m.bytes.Load())
}

func TestBodyCacheBoundedRecycle(t *testing.T) {
	m := newBodyCacheManager(1<<20, time.Minute, time.Hour)
	defer m.close()
	for range bodyCacheEvictionBatch + 1 {
		_, err := m.get(context.Background(), &bodyCacheSlot{}, func() (proto.Message, int, error) {
			return &msgpb.InsertRequest{}, 0, nil
		})
		require.NoError(t, err)
	}
	now := time.Now().Add(time.Hour)
	require.True(t, m.recycleBatch(&m.shards[0], now))
	require.Equal(t, 1, m.shards[0].entries.Len(), "one lock hold must not drain an arbitrarily large expired prefix")
	m.recycle(now)
	require.Zero(t, m.bytes.Load())
}

func TestBodyCacheConcurrentSharedBudget(t *testing.T) {
	m := newBodyCacheManager(4096, time.Hour, time.Hour)
	var wg sync.WaitGroup
	for range bodyCacheShardCount * 2 {
		wg.Go(func() {
			for i := range 100 {
				slot := newBodyCacheSlot()
				body, err := m.get(context.Background(), slot, func() (proto.Message, int, error) {
					return &msgpb.InsertRequest{NumRows: 7}, 32 + i*8, nil
				})
				if err != nil || body.(*msgpb.InsertRequest).NumRows != 7 {
					t.Errorf("body unavailable under capacity pressure: %v", err)
					return
				}
				if used := m.bytes.Load(); used < 0 || used > m.budget {
					t.Errorf("invalid reserved bytes: %d", used)
				}
			}
		})
	}
	wg.Wait()
	m.close()
	require.Zero(t, m.bytes.Load())
}

func TestBodyCacheCloseRefundsUnpublishedReservation(t *testing.T) {
	m := newBodyCacheManager(4096, time.Hour, time.Hour)
	defer m.close()
	slot := newBodyCacheSlot()
	started, release := make(chan struct{}), make(chan struct{})
	result, closed := make(chan error, 1), make(chan struct{})
	go func() {
		_, err := m.get(context.Background(), slot, func() (proto.Message, int, error) {
			close(started)
			<-release
			return &msgpb.InsertRequest{}, 0, nil
		})
		result <- err
	}()
	<-started
	func() {
		shard := &m.shards[slot.shard]
		shard.mu.Lock()
		defer shard.mu.Unlock()
		close(release)
		// Decode and reserve can finish while the owning shard is locked,
		// but publication cannot. Close must refund this reservation.
		require.Eventually(t, func() bool { return m.bytes.Load() == bodyCacheEntryOverhead }, time.Second, time.Millisecond)
		go func() { m.close(); close(closed) }()
		require.Eventually(t, m.closed.Load, time.Second, time.Millisecond)
	}()
	require.NoError(t, <-result)
	<-closed
	require.Nil(t, slot.body)
	require.Zero(t, m.bytes.Load())
}

func TestImmutableBodyAccountsInputWireSize(t *testing.T) {
	mutable := NewInsertMessageBuilderV1().WithVChannel("v1").
		WithHeader(&InsertMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.InsertRequest{NumRows: 7}).MustBuildMutable()
	// Repeated singular fields are valid protobuf, but occupy more wire bytes
	// than proto.Size of the decoded message. Charge the bytes actually decoded.
	payload := append(append([]byte(nil), mutable.Payload()...), mutable.Payload()...)
	raw := NewImmutableMesasge(nil, payload, mutable.Properties().ToRawMap()).(*immutableMessageImpl)
	body := MustAsImmutableInsertMessageV1(raw).MustBody()
	require.EqualValues(t, 7, body.NumRows)
	m := globalBodyCache()
	m.shards[raw.bodyCache.shard].mu.Lock()
	require.EqualValues(t, bodyCacheEntryOverhead+2*len(payload), raw.bodyCache.bytes)
	m.shards[raw.bodyCache.shard].mu.Unlock()
	clone := raw.clone()
	clone.overwriteTimeTick(15)
	require.Same(t, raw.bodyCache, clone.bodyCache)
	require.Same(t, body, MustAsImmutableInsertMessageV1(clone).MustBody())
}
