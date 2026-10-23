package dql

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querynodev2/delegator"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/materialize"
	"github.com/milvus-io/milvus/internal/util/queryutil"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	_ "github.com/milvus-io/milvus/pkg/v3/util/resource" // registers releaseCodec as "proto"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// This file measures the Query and Materialize paths of a request over S
// shards, each with N workers, from the workers' reduce output to the proxy's
// final result, with the production gRPC codec on both hops. Each step is
// timed on its own, so the benchmark reports both the summed CPU time (total)
// and a critical path. Shards run in parallel, so the critical path is the
// slowest shard's chain -- its slowest worker, its delegator's slowest single
// receive (each reply is received while the delegator still waits for the
// others), its delegator's serial part, and the proxy's handling of its reply
// on arrival (gRPC decodes each shard's reply on its own goroutine; for
// Materialize the load balancer's callback then decodes the body into the
// fill) -- followed by what the proxy does once the fan-out returned: for
// Query its cross-shard query pipeline, for Materialize the fill's finish and
// the single-result pipeline PostExecute still runs; for a requery shape,
// then organize. The workers and delegators run one after another on one
// goroutine; the proxy's per-shard receive (and, for Materialize, the add
// into the fill) runs on one goroutine per shard, concurrently, as the load
// balancer's callbacks do, so contention on the fill is measured.
//
// Two request shapes: a filter query (nq=0 in the name), and a search
// requery (nq>=1) whose requested ids are every row in a random "distance"
// order, split over nq queries; with nq>1 a quarter of each query's hits
// repeat another query's.
//
// The transport is modeled, not timed: a sent message keeps the codec's
// BufferSlice, as gRPC does, and reaches the receiver split into 16 KB
// buffers taken from a pool, as gRPC's HTTP/2 transport hands them over; the
// receiver frees them after Unmarshal, as gRPC does, and they return to the
// pool. With pbPoison set, a freed buffer is overwritten, so a reader still
// using it sees garbage.

const (
	pbFieldVec  = 101
	pbFieldText = 102
	pbFieldJSON = 103
	pbDim       = 768
	pbTS        = 100
)

// pbVocab is a 2500-word synthetic vocabulary, drawn Zipf-distributed by
// pbWord, so text compresses like natural text rather than best-case.
var pbVocab = func() []string {
	r := rand.New(rand.NewSource(42))
	words := make([]string, 2500)
	seen := make(map[string]bool, len(words))
	for i := range words {
		for {
			b := make([]byte, 3+r.Intn(8))
			for j := range b {
				b[j] = byte('a' + r.Intn(26))
			}
			if w := string(b); !seen[w] {
				seen[w] = true
				words[i] = w
				break
			}
		}
	}
	return words
}()

// pbSegments is how many segment records each worker's rows come from.
const pbSegments = 8

type pbShape struct {
	fields  string // vec768 | text | vec768+text
	shards  int
	workers int // per shard
	topK    int // per request, over all shards
	nq      int // 0: filter query; >= 1: search requery with nq queries
}

type pbWorker struct {
	ids   *schemapb.IDs
	ts    *schemapb.FieldData
	recs  []arrow.Record // one per segment
	pairs [][2]int64     // (segment, row) per output row, scattered
}

type pbShard struct {
	workers []pbWorker
}

type pbFixture struct {
	schema    *schemapb.CollectionSchema
	shards    []pbShard
	req       *querypb.QueryRequest
	known     materializeSchemaRef
	maxOut    int64
	outputIDs []int64
	// Requery only: the ids the requery asks for (every pk once, in search
	// order, as mergeIDsFunc builds them) and the search result's ids that
	// organize aligns the fields with (the nq queries' hits, concatenated).
	requested *schemapb.IDs
	searchIDs []*schemapb.IDs
}

// pbStats holds one run's time per role, its critical path, and wire bytes
// per hop. worker, deleg and proxy sum over workers, replies and shards; the
// byte counts sum over shards.
type pbStats struct {
	worker, deleg, proxy time.Duration
	critical             time.Duration
	w2d, d2p             int
}

// pbShardTimes are one shard's step times: work[i] is worker i, recv[i] the
// delegator's receive of its reply, serial the delegator's merge (or
// aggregate) and send, proxyRecv the proxy's receive of the shard's reply.
type pbShardTimes struct {
	work, recv        []time.Duration
	serial, proxyRecv time.Duration
}

// chain is the shard's critical path up to the end of the fan-out.
func (t pbShardTimes) chain() time.Duration {
	var maxWork, maxRecv time.Duration
	for i := range t.work {
		maxWork = max(maxWork, t.work[i])
		maxRecv = max(maxRecv, t.recv[i])
	}
	return maxWork + maxRecv + t.serial + t.proxyRecv
}

// finish fills the role totals and the critical path: the slowest shard's
// chain, then afterWall (what the proxy does after the fan-out, wall clock).
// afterCPU is the same work's CPU time, summed over shards.
func (st *pbStats) finish(shards []pbShardTimes, afterWall, afterCPU time.Duration) {
	var slowest time.Duration
	for _, t := range shards {
		for i := range t.work {
			st.worker += t.work[i]
			st.deleg += t.recv[i]
		}
		st.deleg += t.serial
		st.proxy += t.proxyRecv
		slowest = max(slowest, t.chain())
	}
	st.proxy += afterCPU
	st.critical = slowest + afterWall
}

func (fx *pbFixture) requery() bool { return fx.requested != nil }

func pbSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{
			FieldID: pbFieldVec, Name: "vec", DataType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: strconv.Itoa(pbDim)}},
		},
		{FieldID: pbFieldText, Name: "text", DataType: schemapb.DataType_VarChar},
		{FieldID: pbFieldJSON, Name: "meta", DataType: schemapb.DataType_JSON},
	}}
}

func pbWord(z *rand.Zipf) string { return pbVocab[z.Uint64()] }

func pbWords(z *rand.Zipf, minLen int) string {
	var sb strings.Builder
	for sb.Len() < minLen {
		if sb.Len() > 0 {
			sb.WriteByte(' ')
		}
		sb.WriteString(pbWord(z))
	}
	return sb.String()
}

// pbRecord builds one segment's Arrow record of the output columns the way
// segcore exports them: columns named by field id, the pk (an output field,
// as the proxy always adds it) as int64, FloatVector as FixedSizeBinary, JSON
// as Binary, and the milvus.field_order / milvus.valid_data_fields metadata,
// with the timestamp (kept in the header) first in field_order. pks holds the
// pk of every row.
func pbRecord(r *rand.Rand, z *rand.Zipf, fields string, pks []int64) arrow.Record {
	rows := len(pks)
	ids := []int64{100}
	if strings.Contains(fields, "vec768") {
		ids = append(ids, pbFieldVec)
	}
	if strings.Contains(fields, "text") {
		ids = append(ids, pbFieldText, pbFieldJSON)
	}
	order := []string{"1"}
	afs := make([]arrow.Field, 0, len(ids))
	for _, id := range ids {
		order = append(order, strconv.FormatInt(id, 10))
		var t arrow.DataType
		switch id {
		case 100:
			t = arrow.PrimitiveTypes.Int64
		case pbFieldVec:
			t = &arrow.FixedSizeBinaryType{ByteWidth: pbDim * 4}
		case pbFieldText:
			t = arrow.BinaryTypes.String
		case pbFieldJSON:
			t = arrow.BinaryTypes.Binary
		}
		afs = append(afs, arrow.Field{Name: strconv.FormatInt(id, 10), Type: t})
	}
	md := arrow.NewMetadata([]string{"milvus.field_order", "milvus.valid_data_fields"},
		[]string{strings.Join(order, ","), ""})
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrow.NewSchema(afs, &md))
	defer b.Release()
	vec := make([]float32, pbDim)
	for i, id := range ids {
		for row := 0; row < rows; row++ {
			switch id {
			case 100:
				b.Field(i).(*array.Int64Builder).Append(pks[row])
			case pbFieldVec:
				for j := range vec {
					vec[j] = r.Float32()
				}
				b.Field(i).(*array.FixedSizeBinaryBuilder).Append(arrow.Float32Traits.CastToBytes(vec))
			case pbFieldText:
				b.Field(i).(*array.StringBuilder).Append(pbWords(z, 300))
			case pbFieldJSON:
				js := fmt.Sprintf(`{"tag":%q,"n":%d,"note":%q}`, pbWord(z), r.Intn(1000), pbWords(z, 100))
				b.Field(i).(*array.BinaryBuilder).Append([]byte(js))
			}
		}
	}
	return b.NewRecord()
}

func newPBFixture(tb testing.TB, s pbShape) *pbFixture {
	paramtable.Init()
	r := rand.New(rand.NewSource(int64(s.shards*1000000 + s.workers*10000 + s.topK)))
	z := rand.NewZipf(r, 1.1, 1, uint64(len(pbVocab)-1))
	fx := &pbFixture{
		schema: pbSchema(),
		req:    &querypb.QueryRequest{Req: &internalpb.RetrieveRequest{Limit: typeutil.Unlimited}},
		maxOut: paramtable.Get().QuotaConfig.MaxOutputSize.GetAsInt64(),
	}
	fx.outputIDs = []int64{100}
	if strings.Contains(s.fields, "vec768") {
		fx.outputIDs = append(fx.outputIDs, pbFieldVec)
	}
	if strings.Contains(s.fields, "text") {
		fx.outputIDs = append(fx.outputIDs, pbFieldText, pbFieldJSON)
	}
	fx.shards = make([]pbShard, s.shards)
	for sh := range fx.shards {
		for w := 0; w < s.workers; w++ {
			// Disjoint, interleaved pks across shards and workers, sorted
			// within each worker.
			var pks []int64
			for i := sh + w*s.shards; i < s.topK; i += s.shards * s.workers {
				pks = append(pks, int64(i))
			}
			ts := make([]int64, len(pks))
			for i := range ts {
				ts[i] = pbTS
			}
			// The rows come from pbSegments segment records, each about three
			// times larger than its share, picked at random: the gather is
			// scattered across records and rows, as on a real worker.
			perSeg := 3*len(pks)/pbSegments + 1
			slots := r.Perm(pbSegments * perSeg)[:len(pks)]
			pairs := make([][2]int64, len(pks))
			segPKs := make([][]int64, pbSegments)
			for i := range segPKs {
				// Rows nobody selects get pks outside the request.
				segPKs[i] = make([]int64, perSeg)
				for j := range segPKs[i] {
					segPKs[i][j] = -1 - int64(i*perSeg+j)
				}
			}
			for i, slot := range slots {
				pairs[i] = [2]int64{int64(slot / perSeg), int64(slot % perSeg)}
				segPKs[slot/perSeg][slot%perSeg] = pks[i]
			}
			recs := make([]arrow.Record, pbSegments)
			for i := range recs {
				recs[i] = pbRecord(r, z, s.fields, segPKs[i])
			}
			fx.shards[sh].workers = append(fx.shards[sh].workers, pbWorker{
				ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: pks}}},
				ts: &schemapb.FieldData{FieldId: 1, Type: schemapb.DataType_Int64, Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: ts}}},
				}},
				recs:  recs,
				pairs: pairs,
			})
		}
	}
	if s.nq > 0 {
		fx.requested, fx.searchIDs = pbSearchIDs(r, s.topK, s.nq)
	}
	// The proxy's schema cache already holds the batch schema in the steady
	// state, so workers omit the bytes.
	w0 := &fx.shards[0].workers[0]
	rec, err := materialize.BuildBatch(w0.header(), w0.selection())
	require.NoError(tb, err)
	schemaMsg, _, err := materialize.EncodeBatch(rec, materialize.CodecNone)
	rec.Release()
	require.NoError(tb, err)
	parsed, err := materialize.ParseSchema(schemaMsg)
	require.NoError(tb, err)
	fx.known = materializeSchemaRef{fingerprint: materialize.SchemaFingerprint(schemaMsg), schema: parsed}
	tb.Cleanup(func() {
		for _, sh := range fx.shards {
			for _, w := range sh.workers {
				for _, rec := range w.recs {
					rec.Release()
				}
			}
		}
	})
	return fx
}

// pbSearchIDs draws a search result over the pks 0..topK-1: nq queries,
// each hitting topK/nq of them in a random "distance" order; with nq > 1 a
// quarter of each query's hits repeat hits of other queries, as overlapping
// queries do. It returns the requery's ids (every pk once, in first-hit
// order, as mergeIDsFunc builds them) and the search result's ids, which
// organize aligns the fields with: one IDs of every query's hits, as
// searchWithRequeryPipe's gen_ids passes them.
func pbSearchIDs(r *rand.Rand, topK, nq int) (*schemapb.IDs, []*schemapb.IDs) {
	perm := r.Perm(topK)
	per := topK / nq
	var hits []int64
	for q := 0; q < nq; q++ {
		own := perm[q*per : (q+1)*per]
		if q == nq-1 {
			own = perm[q*per:]
		}
		qh := make([]int64, 0, len(own)+len(own)/4)
		for _, pk := range own {
			qh = append(qh, int64(pk))
		}
		if nq > 1 {
			for _, i := range r.Perm(topK)[:len(own)/4] {
				qh = append(qh, int64(perm[i]))
			}
			r.Shuffle(len(qh), func(i, j int) { qh[i], qh[j] = qh[j], qh[i] })
			// A query hits a pk at most once.
			seen := make(map[int64]bool, len(qh))
			kept := qh[:0]
			for _, pk := range qh {
				if !seen[pk] {
					seen[pk] = true
					kept = append(kept, pk)
				}
			}
			qh = kept
		}
		hits = append(hits, qh...)
	}
	search := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: hits}}}
	merged, err := mergeIDsFunc(context.Background(), nil, []*milvuspb.SearchResults{{Results: &schemapb.SearchResultData{Ids: search}}})
	if err != nil {
		panic(err)
	}
	return merged[0].(*schemapb.IDs), []*schemapb.IDs{search}
}

// header is a fresh reduce-output header: the worker's encode replaces its
// FieldsData, so each run needs its own.
func (w *pbWorker) header() *segcorepb.RetrieveResults {
	return &segcorepb.RetrieveResults{Ids: w.ids, FieldsData: []*schemapb.FieldData{w.ts}}
}

func (w *pbWorker) selection() *queryutil.ArrowSelection {
	return &queryutil.ArrowSelection{Records: w.recs, Rows: queryutil.RowRefsForTest(w.pairs)}
}

var (
	pbZstdOnce sync.Once
	pbZstdEnc  *zstd.Encoder
	// pbZstdDecs are single-threaded decoders, one per receiving goroutine.
	pbZstdDecs = sync.Pool{New: func() any {
		dec, _ := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		return dec
	}}
)

// pbZstd builds the encoder the way grpcclient does (default level, 1MB
// window).
func pbZstd() *zstd.Encoder {
	pbZstdOnce.Do(func() {
		pbZstdEnc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithWindowSize(1<<20))
	})
	return pbZstdEnc
}

// pbChunk is the size of the buffers gRPC's transport hands the codec.
const pbChunk = 16 << 10

// pbWire is a sent message: the codec's BufferSlice (or, compressed, the
// zstd frame) as gRPC keeps it until the transport writes it.
type pbWire struct {
	bs mem.BufferSlice
}

func (w pbWire) Len() int { return w.bs.Len() }

// pbCompressBuf is the buffer grpcclient's zstdWriter keeps across messages.
var pbCompressBuf bytes.Buffer

// pbSend marshals msg with the registered gRPC codec. Uncompressed, the
// BufferSlice is kept as is. Compressed, it is buffered and compressed in
// one shot, as grpcclient's zstdWriter does.
func pbSend(msg proto.Message, compress bool) (pbWire, error) {
	bs, err := encoding.GetCodecV2("proto").Marshal(msg)
	if err != nil {
		return pbWire{}, err
	}
	if !compress {
		return pbWire{bs: bs}, nil
	}
	pbCompressBuf.Reset()
	for _, b := range bs {
		pbCompressBuf.Write(b.ReadOnlyData())
	}
	bs.Free()
	enc := pbZstd()
	frame := enc.EncodeAll(pbCompressBuf.Bytes(), nil)
	return pbWire{bs: mem.BufferSlice{mem.SliceBuffer(frame)}}, nil
}

// pbFramePool is the transport's receive-buffer pool. Its buffers are
// poisoned on Put when pbPoison is set, and gets and puts are counted.
type pbFramePool struct {
	pool       sync.Pool
	gets, puts atomic.Int64
}

var (
	pbFrames pbFramePool
	pbPoison atomic.Bool
)

func (p *pbFramePool) Get(n int) *[]byte {
	p.gets.Add(1)
	if b, ok := p.pool.Get().(*[]byte); ok && cap(*b) >= n {
		*b = (*b)[:n]
		return b
	}
	b := make([]byte, n, max(n, pbChunk))
	return &b
}

func (p *pbFramePool) Put(b *[]byte) {
	p.puts.Add(1)
	if pbPoison.Load() {
		full := (*b)[:cap(*b)]
		for i := range full {
			full[i] = 0xAA
		}
	}
	p.pool.Put(b)
}

func (p *pbFramePool) outstanding() int64 { return p.gets.Load() - p.puts.Load() }

// pbTransport moves a sent message to the receiver: it copies the bytes into
// pbChunk buffers from pbFrames, frees the sender's buffers (gRPC does once
// written) and returns the received buffers. It is not timed.
func pbTransport(w pbWire) mem.BufferSlice {
	out := make(mem.BufferSlice, 0, w.bs.Len()/pbChunk+1)
	var cur *[]byte
	for _, b := range w.bs {
		for d := b.ReadOnlyData(); len(d) > 0; {
			if cur == nil || len(*cur) == pbChunk {
				if cur != nil {
					out = append(out, mem.NewBuffer(cur, &pbFrames))
				}
				cur = pbFrames.Get(pbChunk)
				*cur = (*cur)[:0]
			}
			n := min(len(d), pbChunk-len(*cur))
			*cur = append(*cur, d[:n]...)
			d = d[n:]
		}
	}
	if cur != nil {
		out = append(out, mem.NewBuffer(cur, &pbFrames))
	}
	w.bs.Free()
	return out
}

// pbRecv decodes what the transport delivered, as gRPC's recvAndDecompress
// does: compressed, it streams the frame through the decoder into pooled
// buffers (behind a limit reader, so ReadAll reads in chunks), then
// Unmarshal; uncompressed, it hands the chunks to Unmarshal. Then it frees
// the received buffers, as gRPC does after Unmarshal.
func pbRecv(in mem.BufferSlice, compress bool, msg proto.Message) error {
	defer in.Free()
	if compress {
		// One decoder per receiving goroutine, as each gRPC stream has.
		dec := pbZstdDecs.Get().(*zstd.Decoder)
		defer pbZstdDecs.Put(dec)
		if err := dec.Reset(in.Reader()); err != nil {
			return err
		}
		out, err := mem.ReadAll(io.LimitReader(dec, math.MaxInt64), mem.DefaultBufferPool())
		if err != nil {
			out.Free()
			return err
		}
		in = out
		defer out.Free()
	}
	return encoding.GetCodecV2("proto").Unmarshal(in, msg)
}

// pbProxyRecv runs each shard's proxy-side receive on its own goroutine, as
// the load balancer's callbacks do, and records each one's time. recv is the
// shard's receive (and add); the transport ran before, untimed.
func pbProxyRecv(wires []pbWire, times []pbShardTimes, recv func(shard int, in mem.BufferSlice) error) error {
	ins := make([]mem.BufferSlice, len(wires))
	for i, w := range wires {
		ins[i] = pbTransport(w)
	}
	errs := make([]error, len(wires))
	var wg sync.WaitGroup
	for i := range wires {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t0 := time.Now()
			errs[i] = recv(i, ins[i])
			times[i].proxyRecv = time.Since(t0)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// runQueryShard: worker MaterializeArrowSelection + send, delegator receive +
// RunDelegatorQueryPipeline + send. It returns the delegator's reply on the
// wire; the proxy receives it in runQueryPath.
func runQueryShard(ctx context.Context, fx *pbFixture, sh *pbShard, compress bool, st *pbStats) (pbWire, pbShardTimes, error) {
	t := pbShardTimes{work: make([]time.Duration, len(sh.workers)), recv: make([]time.Duration, len(sh.workers))}
	wires := make([]pbWire, len(sh.workers))
	for i := range sh.workers {
		t0 := time.Now()
		w := &sh.workers[i]
		h := w.header()
		if err := queryutil.MaterializeArrowSelection(h, w.selection(), fx.schema); err != nil {
			return pbWire{}, t, err
		}
		reply := &internalpb.RetrieveResults{
			Status: merr.Success(), Ids: h.GetIds(), FieldsData: h.GetFieldsData(),
			CostAggregation: &internalpb.CostAggregation{}, AllRetrieveCount: int64(len(w.pairs)),
		}
		wire, err := pbSend(reply, compress)
		if err != nil {
			return pbWire{}, t, err
		}
		t.work[i] = time.Since(t0)
		wires[i] = wire
		st.w2d += wire.Len()
	}
	results := make([]*internalpb.RetrieveResults, len(wires))
	for i, wire := range wires {
		in := pbTransport(wire)
		t0 := time.Now()
		results[i] = &internalpb.RetrieveResults{}
		if err := pbRecv(in, compress, results[i]); err != nil {
			return pbWire{}, t, err
		}
		t.recv[i] = time.Since(t0)
	}
	t0 := time.Now()
	merged, err := segments.RunDelegatorQueryPipeline(ctx, fx.req, fx.schema, results)
	if err != nil {
		return pbWire{}, t, err
	}
	wire, err := pbSend(merged, compress)
	if err != nil {
		return pbWire{}, t, err
	}
	t.serial = time.Since(t0)
	st.d2p += wire.Len()
	return wire, t, nil
}

// runMaterializeShard: worker Encode + send (as buildMaterializeResult),
// delegator receive + AggregateMaterializeResults + send. It returns the delegator's reply on the wire; the proxy receives it
// in runMaterializePath. The body codec is the configured one (pbSetCodec);
// gRPC compression stays off on this path.
func runMaterializeShard(fx *pbFixture, sh *pbShard, st *pbStats) (pbWire, pbShardTimes, error) {
	t := pbShardTimes{work: make([]time.Duration, len(sh.workers)), recv: make([]time.Duration, len(sh.workers))}
	wires := make([]pbWire, len(sh.workers))
	for i := range sh.workers {
		t0 := time.Now()
		w := &sh.workers[i]
		hdr, sel := w.header(), w.selection()
		enc, err := materialize.Encode(hdr, sel)
		if err != nil {
			return pbWire{}, t, err
		}
		header := &internalpb.MaterializeHeader{
			Status: merr.Success(), AllRetrieveCount: int64(len(w.pairs)),
			CostAggregation:   &internalpb.CostAggregation{},
			SchemaFingerprint: materialize.SchemaFingerprint(enc.SchemaMsg),
			BodyRawSize:       enc.RawSize,
		}
		if header.SchemaFingerprint != fx.known.fingerprint {
			header.BodySchema = enc.SchemaMsg
		}
		wire, err := pbSend(&internalpb.MaterializeResults{Header: header, Body: [][]byte{enc.Entry}}, false)
		if err != nil {
			return pbWire{}, t, err
		}
		t.work[i] = time.Since(t0)
		wires[i] = wire
		st.w2d += wire.Len()
	}
	results := make([]*internalpb.MaterializeResults, len(wires))
	for i, wire := range wires {
		in := pbTransport(wire)
		t0 := time.Now()
		results[i] = &internalpb.MaterializeResults{}
		if err := pbRecv(in, false, results[i]); err != nil {
			return pbWire{}, t, err
		}
		t.recv[i] = time.Since(t0)
	}
	t0 := time.Now()
	relayed, err := delegator.AggregateMaterializeResults(results, 0, true, 0)
	if err != nil {
		return pbWire{}, t, err
	}
	wire, err := pbSend(relayed, false)
	if err != nil {
		return pbWire{}, t, err
	}
	t.serial = time.Since(t0)
	st.d2p += wire.Len()
	return wire, t, nil
}

// pbMaterializeRecv is the proxy's per-shard receive: decode the reply and
// add it to the fill (materializeShard's on-arrival add).
func pbMaterializeRecv(fx *pbFixture, fill *materializeFill, channel string, in mem.BufferSlice) error {
	res := &internalpb.MaterializeResults{}
	if err := pbRecv(in, false, res); err != nil {
		return err
	}
	err := fill.add(0, channel, res)
	return err
}

// pbCrossShard is the query pipeline QueryTask.PostExecute runs for a plain
// query: the cross-shard reduce for Query, a pass-through of the one result
// for Materialize.
func pbCrossShard(ctx context.Context, fx *pbFixture, shards []*internalpb.RetrieveResults) (*milvuspb.QueryResults, error) {
	p, err := NewQueryPipeline(fx.schema, typeutil.Unlimited, 0, reduce.IReduceNoOrder, nil, nil, nil, nil, fx.outputIDs)
	if err != nil {
		return nil, err
	}
	return p.Execute(ctx, shards)
}

// pbResult is what a path hands on: the query's result, and for a requery
// shape the organized fields.
type pbResult struct {
	query     *milvuspb.QueryResults
	organized [][]*schemapb.FieldData
	aligned   bool // the requery's result holds exactly the requested ids, in order
}

// pbOrganize is the search pipeline's organize node after the requery.
func pbOrganize(ctx context.Context, fx *pbFixture, fields []*schemapb.FieldData) ([][]*schemapb.FieldData, error) {
	op := &organizeOperator{
		traceCtx:           ctx,
		primaryFieldSchema: fx.schema.GetFields()[0],
		schema:             fx.schema,
	}
	out, err := op.run(ctx, nil, fields, fx.searchIDs)
	if err != nil {
		return nil, err
	}
	return out[0].([][]*schemapb.FieldData), nil
}

func runQueryPath(ctx context.Context, fx *pbFixture, compress bool) (pbResult, pbStats, error) {
	var st pbStats
	times := make([]pbShardTimes, len(fx.shards))
	wires := make([]pbWire, len(fx.shards))
	for i := range fx.shards {
		var err error
		if wires[i], times[i], err = runQueryShard(ctx, fx, &fx.shards[i], compress, &st); err != nil {
			return pbResult{}, st, err
		}
	}
	shards := make([]*internalpb.RetrieveResults, len(fx.shards))
	if err := pbProxyRecv(wires, times, func(i int, in mem.BufferSlice) error {
		shards[i] = &internalpb.RetrieveResults{}
		return pbRecv(in, compress, shards[i])
	}); err != nil {
		return pbResult{}, st, err
	}
	t0 := time.Now()
	var res pbResult
	var err error
	if res.query, err = pbCrossShard(ctx, fx, shards); err != nil {
		return res, st, err
	}
	if fx.requery() {
		res.aligned = resultHasIDs(res.query.GetFieldsData(), fx.schema.GetFields()[0], fx.requested)
		if res.organized, err = pbOrganize(ctx, fx, res.query.GetFieldsData()); err != nil {
			return res, st, err
		}
	}
	after := time.Since(t0)
	st.finish(times, after, after)
	return res, st, nil
}

func runMaterializePath(ctx context.Context, fx *pbFixture, compress bool) (pbResult, pbStats, error) {
	var st pbStats
	times := make([]pbShardTimes, len(fx.shards))
	helper, err := typeutil.CreateSchemaHelper(fx.schema)
	if err != nil {
		return pbResult{}, st, err
	}
	fill := newMaterializeFill(helper, fx.maxOut, fx.requested, fx.known, nil)
	defer fill.release()
	wires := make([]pbWire, len(fx.shards))
	for i := range fx.shards {
		var err error
		if wires[i], times[i], err = runMaterializeShard(fx, &fx.shards[i], &st); err != nil {
			return pbResult{}, st, err
		}
	}
	if err := pbProxyRecv(wires, times, func(i int, in mem.BufferSlice) error {
		return pbMaterializeRecv(fx, fill, strconv.Itoa(i), in)
	}); err != nil {
		return pbResult{}, st, err
	}
	t0 := time.Now()
	var res pbResult
	if _, err := fill.failure(); err != nil {
		return res, st, err
	}
	out, err := fill.finish()
	if err != nil {
		return res, st, err
	}
	if res.query, err = pbCrossShard(ctx, fx, []*internalpb.RetrieveResults{out}); err != nil {
		return res, st, err
	}
	if fx.requery() {
		res.aligned = resultHasIDs(res.query.GetFieldsData(), fx.schema.GetFields()[0], fx.requested)
		if res.organized, err = pbOrganize(ctx, fx, res.query.GetFieldsData()); err != nil {
			return res, st, err
		}
	}
	after := time.Since(t0)
	st.finish(times, after, after)
	return res, st, nil
}

// pbRowsByPK keys every row of a query result by its pk, each row the
// values of its columns in field id order.
func pbRowsByPK(t *testing.T, r *milvuspb.QueryResults) map[int64][]any {
	t.Helper()
	var pks []int64
	for _, fd := range r.GetFieldsData() {
		if fd.GetFieldId() == 100 {
			pks = fd.GetScalars().GetLongData().GetData()
		}
	}
	require.NotNil(t, pks, "no pk column")
	rows := make(map[int64][]any, len(pks))
	fields := append([]*schemapb.FieldData(nil), r.GetFieldsData()...)
	slices.SortFunc(fields, func(a, b *schemapb.FieldData) int { return int(a.GetFieldId() - b.GetFieldId()) })
	for i, pk := range pks {
		row := make([]any, 0, len(fields))
		for _, fd := range fields {
			switch fd.GetFieldId() {
			case 100:
				row = append(row, fd.GetScalars().GetLongData().GetData()[i])
			case pbFieldVec:
				row = append(row, fd.GetVectors().GetFloatVector().GetData()[i*pbDim:(i+1)*pbDim])
			case pbFieldText:
				row = append(row, fd.GetScalars().GetStringData().GetData()[i])
			case pbFieldJSON:
				row = append(row, fd.GetScalars().GetJsonData().GetData()[i])
			}
		}
		rows[pk] = row
	}
	return rows
}

// pbHeaders are a result's columns without their payloads.
func pbHeaders(r *milvuspb.QueryResults) []string {
	var out []string
	for _, fd := range r.GetFieldsData() {
		out = append(out, fmt.Sprintf("%d/%s/%s/%v", fd.GetFieldId(), fd.GetFieldName(), fd.GetType(), fd.GetIsDynamic()))
	}
	slices.Sort(out)
	return out
}

// TestMaterializePipelineEquivalence guards the benchmark and the ordering
// contract. A filter query returns the same row set either way, but not in
// the same order: Query sorts by pk, Materialize keeps arrival order. A
// requery organizes to exactly the same fields either way.
func TestMaterializePipelineEquivalence(t *testing.T) {
	ctx := context.Background()
	// Receive buffers are poisoned once freed: a result still reading one
	// (the relay's views, the fill's entries) would differ.
	pbPoison.Store(true)
	defer pbPoison.Store(false)
	defer func() {
		require.Zero(t, pbFrames.outstanding(), "every receive buffer is back in the pool")
	}()
	for _, shards := range []int{1, 4} {
		for _, fields := range []string{"vec768", "text", "vec768+text"} {
			for _, nq := range []int{0, 1, 4} {
				for _, compress := range []bool{false, true} {
					t.Run(fmt.Sprintf("S=%d/%s/nq=%d/zstd=%v", shards, fields, nq, compress), func(t *testing.T) {
						pbSetCodec(t, compress)
						fx := newPBFixture(t, pbShape{fields: fields, shards: shards, workers: 4, topK: 100, nq: nq})
						q, _, err := runQueryPath(ctx, fx, compress)
						require.NoError(t, err)
						m, _, err := runMaterializePath(ctx, fx, compress)
						require.NoError(t, err)

						require.Equal(t, pbHeaders(q.query), pbHeaders(m.query))
						qRows, mRows := pbRowsByPK(t, q.query), pbRowsByPK(t, m.query)
						require.Len(t, qRows, 100)
						require.Equal(t, qRows, mRows)
						if nq == 0 {
							return
						}
						require.True(t, m.aligned)
						// One query: organize's fast path hands the requery's
						// fields on as they are. Several queries repeat ids, so
						// organize still gathers.
						fast := &m.organized[0][0] == &m.query.GetFieldsData()[0]
						require.Equal(t, nq == 1, fast)
						require.Len(t, m.organized, 1)
						require.Equal(t, len(q.organized[0]), len(m.organized[0]))
						for i := range q.organized[0] {
							require.True(t, proto.Equal(q.organized[0][i], m.organized[0][i]), "organized field %d differs", q.organized[0][i].GetFieldId())
						}
					})
				}
			}
		}
	}
}

// Materialize does not sort a filter query's rows by pk: they come in
// arrival order (design doc 20261019 §7).
func TestMaterializePipelineArrivalOrder(t *testing.T) {
	ctx := context.Background()
	fx := newPBFixture(t, pbShape{fields: "text", shards: 4, workers: 4, topK: 100})
	q, _, err := runQueryPath(ctx, fx, false)
	require.NoError(t, err)
	m, _, err := runMaterializePath(ctx, fx, false)
	require.NoError(t, err)
	pkCol := func(r *milvuspb.QueryResults) []int64 {
		for _, fd := range r.GetFieldsData() {
			if fd.GetFieldId() == 100 {
				return fd.GetScalars().GetLongData().GetData()
			}
		}
		return nil
	}
	require.True(t, slices.IsSorted(pkCol(q.query)))
	// Shards arrive in any order; within a shard's reply, worker by worker,
	// each worker's rows in pk order.
	got := pkCol(m.query)
	shardOf := make(map[int64]int)
	for s, sh := range fx.shards {
		for _, w := range sh.workers {
			for _, pk := range w.ids.GetIntId().GetData() {
				shardOf[pk] = s
			}
		}
	}
	for s, sh := range fx.shards {
		var want, inShard []int64
		for _, w := range sh.workers {
			want = append(want, w.ids.GetIntId().GetData()...)
		}
		for _, pk := range got {
			if shardOf[pk] == s {
				inShard = append(inShard, pk)
			}
		}
		require.Equal(t, want, inShard, "shard %d", s)
	}
	require.Len(t, got, len(shardOf))
	require.False(t, slices.IsSorted(got))
}

// BenchmarkMaterializePipeline reports, besides ns/op for the whole chain
// (which includes the untimed transport model), the critical path
// (critical-µs), the summed CPU time (total-µs), each role's share
// (worker-us, deleg-us, proxy-us per op) and the wire bytes of the
// worker->delegator hop and the delegator->proxy hop, summed over shards.
// pbSetCodec configures the workers' body codec for the run: zstd when
// compress (gRPC zstd on the Query path), none otherwise.
func pbSetCodec(tb testing.TB, compress bool) {
	pt := paramtable.Get()
	codec := string(materialize.CodecNone)
	if compress {
		codec = string(materialize.CodecZstd)
	}
	pt.Save(pt.QueryNodeCfg.MaterializeCodec.Key, codec)
	tb.Cleanup(func() { pt.Reset(pt.QueryNodeCfg.MaterializeCodec.Key) })
}

func BenchmarkMaterializePipeline(b *testing.B) {
	ctx := context.Background()
	type pathFn func(context.Context, *pbFixture, bool) (pbResult, pbStats, error)
	paths := []struct {
		name string
		run  pathFn
	}{{"query", runQueryPath}, {"materialize", runMaterializePath}}
	for _, fields := range []string{"vec768", "text", "vec768+text"} {
		for _, s := range []int{1, 4} {
			for _, n := range []int{1, 8} {
				for _, topK := range []int{100, 1000} {
					for _, nq := range []int{0, 1, 4} {
						fx := newPBFixture(b, pbShape{fields: fields, shards: s, workers: n, topK: topK, nq: nq})
						for _, compress := range []bool{false, true} {
							codec := "none"
							if compress {
								codec = "zstd"
							}
							for _, p := range paths {
								name := fmt.Sprintf("fields=%s/S=%d/N=%d/topK=%d/nq=%d/codec=%s/path=%s", fields, s, n, topK, nq, codec, p.name)
								b.Run(name, func(b *testing.B) {
									pbSetCodec(b, compress)
									var sum pbStats
									b.ReportAllocs()
									b.ResetTimer()
									for i := 0; i < b.N; i++ {
										_, st, err := p.run(ctx, fx, compress)
										if err != nil {
											b.Fatal(err)
										}
										sum.worker += st.worker
										sum.deleg += st.deleg
										sum.proxy += st.proxy
										sum.critical += st.critical
										sum.w2d, sum.d2p = st.w2d, st.d2p
									}
									per := func(d time.Duration) float64 { return float64(d) / 1e3 / float64(b.N) }
									b.ReportMetric(per(sum.critical), "critical-µs")
									b.ReportMetric(per(sum.worker+sum.deleg+sum.proxy), "total-µs")
									b.ReportMetric(per(sum.worker), "worker-us/op")
									b.ReportMetric(per(sum.deleg), "deleg-us/op")
									b.ReportMetric(per(sum.proxy), "proxy-us/op")
									b.ReportMetric(float64(sum.w2d), "w2d-B")
									b.ReportMetric(float64(sum.d2p), "d2p-B")
								})
							}
						}
					}
				}
			}
		}
	}
}
