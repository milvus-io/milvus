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

// Package-level plumbing shared by the gRPC compressors.
//
// Every codec follows the same shape, one file each in compression_zstd.go,
// compression_snappy.go and compression_s2.go:
//
//   - compression runs inline on the gRPC goroutine handling the RPC; no codec
//     starts a goroutine of its own,
//   - gRPC builds a compressor per outgoing message and a decompressor per
//     incoming one, so both are taken from a pool and reset rather than
//     constructed, and
//   - the settings are read once from paramtable on first use and are not
//     hot-reloadable.
package grpcclient

import (
	"io"
	"runtime"
	"sync"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
	"google.golang.org/grpc/encoding"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	None   = ""
	Zstd   = "zstd"
	Snappy = "snappy"
	S2     = "s2"
)

func init() {
	encoding.RegisterCompressor(zstdCompressor{})
	encoding.RegisterCompressor(snappyCompressor{})
	encoding.RegisterCompressor(s2Compressor{})
}

var (
	configOnce       sync.Once
	codecLevel       zstd.EncoderLevel
	codecConcurrency int
)

// encoderStateBudget caps the memory the zstd encoder state pool may hold.
//
// zstd builds every state eagerly on the first EncodeAll and keeps them for the
// life of the process, and a state grows sharply with the level. Without a
// budget a high level on a many-core node reserves gigabytes on the first
// compressed RPC: 16 cores x the default multiplier of 2 x ~36MB at best is
// over 1GB, and the maximum multiplier takes that past 4GB -- enough to OOM a
// memory-capped container from one config typo.
const encoderStateBudget = 256 << 20

// zstdEncoderStateBytes turns encoderStateBudget into a state count. The values
// are rounded up from the resident cost measured with rpcWindowSize, so that a
// library version growing its tables shrinks the pool rather than overrunning
// the budget.
func zstdEncoderStateBytes(level zstd.EncoderLevel) int {
	switch level {
	case zstd.SpeedFastest:
		return 3 << 20
	case zstd.SpeedBetterCompression:
		return 8 << 20
	case zstd.SpeedBestCompression:
		return 40 << 20
	default:
		return 4 << 20
	}
}

// initCompressionConfig lazily reads the codec settings shared by all
// compressors. They are read once and are not hot-reloadable.
//
// The settings come from ProxyGrpcClientCfg rather than from the caller's role,
// because a single process registers one set of codecs for every role it hosts
// -- standalone runs proxy, querynode and datanode together -- so there is no
// per-role codec to configure. That works because all four keys are global
// (bare grpc.client.*, with no p.Domain fallback, unlike ClientMaxSendSize). If
// a per-role override is ever added for them, this read has to change with it,
// or a QueryNode would encode at the Proxy's level while ClientBase advertises
// the algorithm from its own config.
func initCompressionConfig() {
	configOnce.Do(func() {
		cfg := &paramtable.Get().ProxyGrpcClientCfg
		codecLevel, _ = parseLevel(cfg.CompressionLevel.GetValue())

		multiplier := cfg.CompressionConcurrency.GetAsInt()
		if multiplier < 1 {
			// A paramtable that predates the item returns 0; zstd rejects a
			// concurrency below 1 and would fall back to an unconfigured codec.
			multiplier = paramtable.DefaultCompressionConcurrency
		}
		// GOMAXPROCS is read here rather than at package init so it reflects the
		// cgroup CPU limit that automaxprocs applies during startup.
		codecConcurrency = multiplier * runtime.GOMAXPROCS(0)
		if budget := encoderStateBudget / zstdEncoderStateBytes(codecLevel); codecConcurrency > budget {
			log.Warn("grpc zstd encoder concurrency clamped to stay within the state memory budget",
				zap.String("compressionLevel", cfg.CompressionLevel.GetValue()),
				zap.Int("requested", codecConcurrency),
				zap.Int("allowed", budget))
			codecConcurrency = budget
		}
	})
}

// parseLevel converts the config string ("fastest"|"default"|"better"|"best")
// into a klauspost encoder level. The ok flag is false for unrecognized values.
func parseLevel(s string) (zstd.EncoderLevel, bool) {
	ok, level := zstd.EncoderLevelFromString(s)
	if !ok {
		return zstd.SpeedDefault, false
	}
	return level, true
}

// codecPools holds the per-message compressors and decompressors of one codec.
//
// There is one set per frame format rather than one shared set, because a
// pooled object is not interchangeable between formats: Reset swaps the
// underlying reader or writer and leaves the framing and the buffer geometry
// alone. A snappy writer is fixed at snappy framing with 64KB blocks and an s2
// writer at s2 framing with rpcBlockSize, and a snappy reader is capped at a
// 64KB block, so reading an s2 stream with it fails outright.
type codecPools struct {
	once    sync.Once
	writers chan resettableWriter
	readers chan resettableReader
}

// init sizes the free lists on first use, once the compression settings have
// been read. Capacity is compressionConcurrency x GOMAXPROCS, the same ceiling
// the zstd encoder state pool uses and already more than the machine can
// (de)compress at once.
//
// A channel rather than a sync.Pool, because the capacity is the bound: a
// sync.Pool parks whatever a burst hands it until the next GC, and a pooled
// codec object is not small -- an s2 writer carries an rpcBlockSize input
// buffer plus a MaxEncodedLen output buffer, and a zstd decoder carries a
// history sized from the frames it has seen -- while gRPC puts no ceiling on
// concurrent streams by default, so a burst alone decides how many exist. The
// trade is that a free list does not hand memory back when traffic drops; what
// it holds is capped instead of reclaimed.
func (p *codecPools) init() {
	p.once.Do(func() {
		initCompressionConfig()
		p.writers = make(chan resettableWriter, codecConcurrency)
		p.readers = make(chan resettableReader, codecConcurrency)
	})
}

// resettableWriter is what a pooled compressor has to offer: write the message,
// finish the frame on Close, and start over against a new destination.
type resettableWriter interface {
	io.WriteCloser
	Reset(io.Writer)
}

// resettableReader is the decompressor counterpart.
type resettableReader interface {
	io.Reader
	Reset(io.Reader)
}

func (p *codecPools) getWriter(w io.Writer, newWriter func() resettableWriter) io.WriteCloser {
	p.init()
	var cw resettableWriter
	select {
	case cw = <-p.writers:
	default:
		cw = newWriter()
	}
	cw.Reset(w)
	return &pooledWriter{w: cw, pool: p.writers}
}

func (p *codecPools) getReader(r io.Reader, newReader func() resettableReader) io.ReadCloser {
	p.init()
	var cr resettableReader
	select {
	case cr = <-p.readers:
	default:
		cr = newReader()
	}
	cr.Reset(r)
	return &pooledReader{r: cr, pool: p.readers}
}

// pooledWriter hands its compressor back once gRPC closes it. gRPC closes every
// compressor it opens on the success path; one abandoned on an error path is
// simply collected instead of reused.
type pooledWriter struct {
	w    resettableWriter
	pool chan resettableWriter
}

func (p *pooledWriter) Write(b []byte) (int, error) {
	if p.w == nil {
		return 0, io.ErrClosedPipe
	}
	return p.w.Write(b)
}

func (p *pooledWriter) Close() error {
	if p.w == nil {
		return nil
	}
	w := p.w
	p.w = nil
	err := w.Close()
	// Reset clears whatever state Close left behind and drops the reference to
	// gRPC's buffer, while keeping the writer's own buffers for the next use.
	w.Reset(nil)
	select {
	case p.pool <- w:
	default:
		// Free list full: a burst built more writers than the ceiling allows,
		// so let the GC take this one.
	}
	return err
}

// pooledReader hands its decompressor back as soon as the message is drained.
// gRPC closes a decompressor that implements io.Closer on every path, success
// and failure alike (rpc_util.go, "if closer, ok := dcReader.(io.Closer)"), so
// Close is what returns this one; the Read path also returns it on EOF, which
// is what an older gRPC without that close would rely on. A reader that somehow
// sees neither is collected instead of reused.
type pooledReader struct {
	r    resettableReader
	pool chan resettableReader
	err  error
}

func (p *pooledReader) Read(b []byte) (int, error) {
	if p.r == nil {
		if p.err != nil {
			return 0, p.err
		}
		return 0, io.EOF
	}
	n, err := p.r.Read(b)
	if err != nil {
		p.err = err
		p.release()
	}
	return n, err
}

func (p *pooledReader) Close() error {
	p.release()
	return nil
}

func (p *pooledReader) release() {
	if p.r == nil {
		return
	}
	r := p.r
	p.r = nil
	// Drop the reference to gRPC's buffer; the decompressor's own buffers stay.
	r.Reset(nil)
	select {
	case p.pool <- r:
	default:
		// Free list full; let the GC take this one.
	}
}

// s2LevelOption maps the compression level to the corresponding s2 writer
// option. s2 has three levels rather than four, so fastest and default both map
// to its default; nil means no option. Shared by the s2 and snappy codecs, which
// are both built on the s2 writer.
func s2LevelOption(level zstd.EncoderLevel) s2.WriterOption {
	switch level {
	case zstd.SpeedBetterCompression:
		return s2.WriterBetterCompression()
	case zstd.SpeedBestCompression:
		return s2.WriterBestCompression()
	default:
		return nil
	}
}

// s2WriterOptions builds the options for a per-message s2 writer.
//
// Concurrency is pinned to 1 so the message is compressed inline on the gRPC
// goroutine that is already handling the RPC. Above 1, s2 spawns a goroutine
// per block plus an ordered-output channel per writer, which is a second
// scheduling layer under gRPC's own; concurrency 1 takes the writeSync fast
// path and skips both. Parallelism across RPCs comes from gRPC running many
// handlers at once, not from the codec.
func s2WriterOptions(format s2.WriterOption) []s2.WriterOption {
	opts := []s2.WriterOption{format}
	if opt := s2LevelOption(codecLevel); opt != nil {
		opts = append(opts, opt)
	}
	return append(opts, s2.WriterConcurrency(1))
}

// compressedBufs reuses the byte slice the zstd encoder compresses into.
//
// A free list rather than a sync.Pool, for the reason the codec pools use one:
// the capacity is the bound. It is sized and capped the same way -- at most
// compressionConcurrency x GOMAXPROCS slots, each holding at most
// compressedBufMaxRetain -- so the worst case is a number you can compute from
// the config rather than whatever a burst handed a sync.Pool. Being a typed
// channel it also drops the *[]byte boxing a sync.Pool needs to avoid
// allocating on every Put.
//
// The trade is the one a free list always makes: buffers a large message grew
// are held rather than reclaimed when traffic drops. compressedBufMaxRetain is
// what keeps that bounded.
var (
	compressedBufOnce sync.Once
	compressedBufs    chan []byte
)

func initCompressedBufs() {
	compressedBufOnce.Do(func() {
		initCompressionConfig()
		compressedBufs = make(chan []byte, codecConcurrency)
	})
}

const (
	// compressedBufMinSize is the floor for a freshly allocated buffer. Sizing
	// from the message keeps a 100 byte heartbeat from pulling a buffer sized
	// for a Search result.
	compressedBufMinSize = 4 << 10

	// compressedBufMaxRetain caps what goes back into the pool.
	// grpc.client.clientMaxSendSize defaults to 256MB, so without a ceiling one
	// large Insert would park a buffer that size in the pool.
	compressedBufMaxRetain = 1 << 20
)

func getCompressedBuf(srcLen int) []byte {
	initCompressedBufs()
	select {
	case b := <-compressedBufs:
		return b[:0]
	default:
	}
	size := srcLen
	if size < compressedBufMinSize {
		size = compressedBufMinSize
	}
	if size > compressedBufMaxRetain {
		size = compressedBufMaxRetain
	}
	// The buffer grows when a message needs more, and the grown buffer is what
	// comes back to the free list, so it converges on the working set instead
	// of on a fixed guess.
	return make([]byte, 0, size)
}

func putCompressedBuf(b []byte) {
	if cap(b) > compressedBufMaxRetain {
		// Grown by an outsized message; let the GC reclaim it.
		return
	}
	initCompressedBufs()
	select {
	case compressedBufs <- b:
	default:
		// Free list full; let the GC take this one.
	}
}
