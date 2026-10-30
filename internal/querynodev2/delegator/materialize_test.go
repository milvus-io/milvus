package delegator

import (
	"bytes"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/materialize"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/resource"
)

// mres is one worker reply. A reply with a body always carries schema's
// fingerprint; sendSchema says whether it also carries the schema bytes.
func mres(src int64, schema string, sendSchema bool, raw int64, body ...string) *internalpb.MaterializeResults {
	entries := make([][]byte, len(body))
	for i, b := range body {
		entries[i] = []byte(b)
	}
	h := &internalpb.MaterializeHeader{
		Status: merr.Success(), BodyRawSize: raw,
		SourceId: src, AllRetrieveCount: int64(len(body)), ScannedTotalBytes: 10,
		CostAggregation: &internalpb.CostAggregation{TotalRelatedDataSize: 100},
	}
	if len(body) > 0 {
		h.SchemaFingerprint = materialize.SchemaFingerprint([]byte(schema))
		if sendSchema {
			h.BodySchema = []byte(schema)
		}
	}
	return &internalpb.MaterializeResults{Header: h, Body: entries}
}

func TestAggregateMaterializeResults_Relay(t *testing.T) {
	replies := []*internalpb.MaterializeResults{
		mres(1, "S", true, 50, "a"),
		mres(2, "", false, 0),
		mres(3, "S", true, 70, "b"),
	}
	for i, r := range replies {
		r.Header.FeatureBits = 1 << i
	}
	got, err := AggregateMaterializeResults(replies, 1, false, 0)
	require.NoError(t, err)
	h := got.GetHeader()
	require.Equal(t, uint64(0b111), h.GetFeatureBits())
	require.Equal(t, []byte("S"), h.GetBodySchema())
	require.Equal(t, materialize.SchemaFingerprint([]byte("S")), h.GetSchemaFingerprint())
	require.Equal(t, int64(120), h.GetBodyRawSize())
	require.Equal(t, int64(2), h.GetAllRetrieveCount())
	require.Equal(t, int64(30), h.GetScannedTotalBytes())
	require.Equal(t, int64(300), h.GetCostAggregation().GetTotalRelatedDataSize())
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, bodyOf(got))
}

// bodyOf takes msg's body and returns its entries, flattened.
func bodyOf(msg *internalpb.MaterializeResults) [][]byte {
	b := resource.TakeMaterializeBody(msg)
	defer b.Release()
	out := make([][]byte, b.Len())
	for i := range out {
		out[i] = bytes.Join(b.Entry(i), nil)
	}
	return out
}

// received is msg as the codec delivers it from a gRPC peer: unmarshaled from
// pooled 16 KB buffers, which are then freed, as gRPC does.
func received(t *testing.T, msg *internalpb.MaterializeResults) *internalpb.MaterializeResults {
	return receivedFrom(t, mem.DefaultBufferPool(), msg)
}

// receivedFrom is received with the buffers taken from pool.
func receivedFrom(t *testing.T, pool mem.BufferPool, msg *internalpb.MaterializeResults) *internalpb.MaterializeResults {
	t.Helper()
	wire, err := proto.Marshal(msg)
	require.NoError(t, err)
	var data mem.BufferSlice
	for b := wire; len(b) > 0; {
		n := min(16<<10, len(b))
		pb := pool.Get(n)
		copy(*pb, b[:n])
		data = append(data, mem.NewBuffer(pb, pool))
		b = b[n:]
	}
	out := &internalpb.MaterializeResults{}
	require.NoError(t, encoding.GetCodecV2("proto").Unmarshal(data, out))
	data.Free()
	return out
}

// countingPool is a mem.BufferPool that counts Get and Put and poisons every
// buffer it gets back (0xAA), so a read after Free shows up as corrupted
// bytes. Its buffers have a capacity above gRPC's pooling threshold, so
// mem.NewBuffer pools them.
type countingPool struct {
	gets, puts atomic.Int64
}

func (p *countingPool) Get(n int) *[]byte {
	p.gets.Add(1)
	b := make([]byte, n, max(n, 4096))
	return &b
}

func (p *countingPool) Put(b *[]byte) {
	p.puts.Add(1)
	full := (*b)[:cap(*b)]
	for i := range full {
		full[i] = 0xAA
	}
}

// outstanding is the number of buffers not yet returned.
func (p *countingPool) outstanding() int64 { return p.gets.Load() - p.puts.Load() }

// The relay copies no body byte: what the delegator allocates to aggregate
// and marshal eight received replies is bookkeeping per received frame (a
// view each), not per body byte.
func TestAggregateMaterializeResults_NoBodyCopy(t *testing.T) {
	codec := encoding.GetCodecV2("proto")
	relay := func(entry int) (uint64, int) {
		var in []*internalpb.MaterializeResults
		var want int
		for w := 0; w < 8; w++ {
			r := mres(int64(w), "S", w == 0, int64(entry), strings.Repeat("x", entry), strings.Repeat("y", entry/3))
			want += entry + entry/3
			in = append(in, received(t, r))
		}
		// A local worker's reply, a Go buffer, mixes in.
		in = append(in, mres(9, "S", false, 1, "local"))
		want += len("local")

		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		out, err := AggregateMaterializeResults(in, 0, true, 0)
		require.NoError(t, err)
		bs, err := codec.Marshal(out)
		require.NoError(t, err)
		runtime.ReadMemStats(&after)
		require.Greater(t, bs.Len(), want)
		got := &internalpb.MaterializeResults{}
		require.NoError(t, proto.Unmarshal(bs.Materialize(), got))
		require.Len(t, got.GetBody(), 17)
		require.Equal(t, []byte("local"), got.GetBody()[16])
		bs.Free()
		return after.TotalAlloc - before.TotalAlloc, want
	}
	small, smallBody := relay(1 << 10)
	large, largeBody := relay(1 << 20)
	t.Logf("allocated to relay: %d B for %d body bytes, %d B for %d", small, smallBody, large, largeBody)
	frames := uint64(largeBody/(16<<10) + 8)
	require.Less(t, large, small+frames*256, "more than 256 B per received frame")
	require.Less(t, large, uint64(largeBody)/100)
}

// On error, the bodies taken from the replies are released: every buffer
// the replies were received in goes back to the pool.
func TestAggregateMaterializeResults_ErrorReleases(t *testing.T) {
	cases := []struct {
		name    string
		in      func(pool *countingPool) []*internalpb.MaterializeResults
		wantErr error
	}{
		{"schemas differ", func(pool *countingPool) []*internalpb.MaterializeResults {
			return []*internalpb.MaterializeResults{
				receivedFrom(t, pool, mres(1, "S1", true, 1, strings.Repeat("a", 40000))),
				receivedFrom(t, pool, mres(2, "S2", true, 1, strings.Repeat("b", 40000))),
			}
		}, merr.ErrServiceUnimplemented},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := &countingPool{}
			in := tc.in(pool)
			require.NotZero(t, pool.outstanding(), "the replies hold their receive buffers")
			_, err := AggregateMaterializeResults(in, 1, false, 0)
			require.ErrorIs(t, err, tc.wantErr)
			require.Zero(t, pool.outstanding(), "every receive buffer is returned to the pool")
			for _, r := range in {
				require.Zero(t, resource.TakeMaterializeBody(r).Len(), "taken by the aggregate")
			}
		})
	}
}

func TestAggregateMaterializeResults_Schema(t *testing.T) {
	fp := materialize.SchemaFingerprint([]byte("S"))
	cases := []struct {
		name      string
		in        []*internalpb.MaterializeResults
		wantErr   error
		wantBytes []byte
		wantFP    uint64
	}{
		{"no worker sent bytes", []*internalpb.MaterializeResults{
			mres(1, "S", false, 1, "a"), mres(2, "S", false, 1, "b"),
		}, nil, nil, fp},
		{"bytes from the second worker", []*internalpb.MaterializeResults{
			mres(1, "S", false, 1, "a"), mres(2, "S", true, 1, "b"),
		}, nil, []byte("S"), fp},
		{"bytes from the first worker", []*internalpb.MaterializeResults{
			mres(1, "S", true, 1, "a"), mres(2, "S", false, 1, "b"),
		}, nil, []byte("S"), fp},
		{"all empty", []*internalpb.MaterializeResults{
			mres(1, "", false, 0), mres(2, "", false, 0),
		}, nil, nil, 0},
		{"fingerprints differ", []*internalpb.MaterializeResults{
			mres(1, "S1", true, 1, "a"), mres(2, "S2", true, 1, "b"),
		}, merr.ErrServiceUnimplemented, nil, 0},
		{"fingerprints differ, no bytes", []*internalpb.MaterializeResults{
			mres(1, "S1", false, 1, "a"), mres(2, "S2", false, 1, "b"),
		}, merr.ErrServiceUnimplemented, nil, 0},
		// The proxy checks the bytes against the fingerprint; the delegator
		// relays them as they are.
		{"bytes that do not match the fingerprint are relayed", func() []*internalpb.MaterializeResults {
			bad := mres(1, "S", true, 1, "a")
			bad.Header.BodySchema = []byte("X")
			return []*internalpb.MaterializeResults{bad, mres(2, "S", true, 1, "b")}
		}(), nil, []byte("X"), fp},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := AggregateMaterializeResults(c.in, 1, false, 0)
			if c.wantErr != nil {
				require.ErrorIs(t, err, c.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, c.wantBytes, got.GetHeader().GetBodySchema())
			require.Equal(t, c.wantFP, got.GetHeader().GetSchemaFingerprint())
		})
	}
}

// The delegator bounds the relayed reply by maxOutputSize on the sum of the
// workers' result_size (the reduce's measure of the rows each sent), not on
// the bodies' raw size; over the bound it fails and releases every body.
func TestAggregateMaterializeResults_OutputSizeLimit(t *testing.T) {
	sized := func(src, resultSize int64) *internalpb.MaterializeResults {
		r := mres(src, "S", true, 600, "body")
		r.Header.ResultSize = resultSize
		return r
	}
	cases := []struct {
		name    string
		limit   int64
		sizes   []int64
		wantErr bool
	}{
		{name: "raw sum over, result sum under", limit: 1000, sizes: []int64{400, 500}},
		{name: "at the limit", limit: 1000, sizes: []int64{400, 600}},
		{name: "sum over", limit: 1000, sizes: []int64{400, 601}, wantErr: true},
		{name: "one worker over", limit: 1000, sizes: []int64{1001}, wantErr: true},
		{name: "no limit", limit: 0, sizes: []int64{1 << 40, 1 << 40}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pool := &countingPool{}
			var in []*internalpb.MaterializeResults
			var sum int64
			for i, sz := range c.sizes {
				in = append(in, receivedFrom(t, pool, sized(int64(i), sz)))
				sum += sz
			}
			got, err := AggregateMaterializeResults(in, 1, false, c.limit)
			if c.wantErr {
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				require.Contains(t, err.Error(), "maxOutputSize")
				require.Zero(t, pool.outstanding(), "every receive buffer is returned to the pool")
				return
			}
			require.NoError(t, err)
			require.Equal(t, sum, got.GetHeader().GetResultSize())
			require.EqualValues(t, 600*len(c.sizes), got.GetHeader().GetBodyRawSize())
			require.Len(t, bodyOf(got), len(c.sizes))
			require.Zero(t, pool.outstanding())
		})
	}
}
