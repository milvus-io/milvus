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
			bodies[i], errs[i] = m.get(context.Background(), slot, decode)
		}(i)
	}
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	waiter := make(chan error, 1)
	go func() {
		_, err := m.get(ctx, slot, decode)
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
					_, err := m.get(ctx, slot, func() (proto.Message, error) {
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
						bodies[i], errs[i] = m.get(context.Background(), slot, func() (proto.Message, error) {
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
					_, err := m.get(waiterCtx, slot, func() (proto.Message, error) {
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
			_, err := m.get(ctx, slot, decode)
			errs <- err
		}()
		synctest.Wait()
		for range readers {
			go func() {
				_, err := m.get(context.Background(), slot, decode)
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
	body, err := m.get(nil, slot, func() (proto.Message, error) { return partial, context.Canceled })
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, body)
	require.Nil(t, slot.body)
	decode := func() (proto.Message, error) { return &msgpb.InsertRequest{NumRows: 7}, nil }
	body, err = m.get(context.Background(), slot, decode)
	require.NoError(t, err)
	require.NotSame(t, partial, body)
	hit, err := m.get(context.Background(), slot, decode)
	require.NoError(t, err)
	require.Same(t, body, hit)
	m.recycle(time.Now())
	require.Same(t, body, slot.body)
	m.recycle(time.Now().Add(time.Minute))
	require.Zero(t, m.bytes)
	require.Zero(t, m.entries.Len())
	require.EqualValues(t, 7, body.(*msgpb.InsertRequest).NumRows, "eviction cannot reset readers' bodies")
	rebuilt, err := m.get(context.Background(), slot, decode)
	require.NoError(t, err)
	require.NotSame(t, body, rebuilt)
	require.True(t, proto.Equal(body, rebuilt))

	oversized := &bodyCacheSlot{}
	large := &msgpb.InsertRequest{RowIDs: make([]int64, 1024)}
	result, err := m.get(context.Background(), oversized, func() (proto.Message, error) { return large, nil })
	require.NoError(t, err)
	require.Same(t, large, result)
	require.Nil(t, oversized.body)
	require.Same(t, rebuilt, slot.body, "oversized bypass must not evict cached bodies")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = m.get(ctx, slot, decode)
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
		body, err := m.get(context.Background(), slot, decode)
		require.NoError(t, err)
		if i == 1 {
			retained = body
		}
	}
	_, err := m.get(context.Background(), slots[0], decode) // refresh the oldest entry
	require.NoError(t, err)
	slot := &bodyCacheSlot{}
	body, err := m.get(context.Background(), slot, decode)
	require.NoError(t, err)
	require.Nil(t, slots[1].body, "evict the least recently accessed entry")
	require.NotNil(t, slots[0].body)
	require.NotNil(t, slots[2].body)
	require.Same(t, body, slot.body, "admit the completed decode before returning")
	require.Equal(t, 3, m.entries.Len())
	require.LessOrEqual(t, m.bytes, m.budget)
	hit, err := m.get(context.Background(), slot, decode)
	require.NoError(t, err)
	require.Same(t, body, hit)
	require.Equal(t, 4, calls, "capacity pressure must not discard the new decode")
	require.EqualValues(t, 7, retained.(*msgpb.InsertRequest).NumRows)
}

func TestBodyCacheIdleRecycler(t *testing.T) {
	idle := newBodyCacheManager(800, time.Millisecond, time.Millisecond)
	defer idle.close()
	_, err := idle.get(context.Background(), &bodyCacheSlot{}, func() (proto.Message, error) { return &msgpb.InsertRequest{}, nil })
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		idle.mu.Lock()
		defer idle.mu.Unlock()
		return idle.entries.Len() == 0 && idle.bytes == 0
	}, time.Second, time.Millisecond)
}

func TestBodyCacheCloseDuringConstruction(t *testing.T) {
	m := newBodyCacheManager(1<<20, time.Hour, time.Hour)
	slot := &bodyCacheSlot{}
	started, release := make(chan struct{}), make(chan struct{})
	done := make(chan proto.Message, 1)
	go func() {
		body, _ := m.get(context.Background(), slot, func() (proto.Message, error) {
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
	require.Zero(t, m.entries.Len())
}

func TestBodyCacheAdmissionEvictsMultipleEntries(t *testing.T) {
	m := newBodyCacheManager(1024, time.Hour, time.Hour)
	defer m.close()
	slots := []*bodyCacheSlot{{}, {}}
	for _, slot := range slots {
		_, err := m.get(context.Background(), slot, func() (proto.Message, error) { return &msgpb.InsertRequest{}, nil })
		require.NoError(t, err)
	}
	// Even a half-full cache may need to evict several entries for one body.
	slot := &bodyCacheSlot{}
	decode := func() (proto.Message, error) { return &msgpb.InsertRequest{RowIDs: make([]int64, 256)}, nil }
	body, err := m.get(context.Background(), slot, decode)
	require.NoError(t, err)
	for _, evicted := range slots {
		require.Nil(t, evicted.body)
	}
	require.Equal(t, 1, m.entries.Len())
	require.Same(t, body, slot.body)
	require.LessOrEqual(t, m.bytes, m.budget)
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
				body, err := m.get(context.Background(), slot, decode)
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
