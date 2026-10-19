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

package grpcclient

import (
	"bytes"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// zstdCompressor is the default codec and the only one every Milvus version can
// decode.
//
// The two directions are not symmetric. Encoding buffers the message and hands
// it to EncodeAll on one shared encoder, because gRPC hands the compressor an
// io.Writer while EncodeAll wants the whole input at once; EncodeAll spawns no
// goroutine -- it borrows a state from a pre-built pool and compresses on the
// caller's goroutine -- so the pool size is only the ceiling on how many RPCs
// may encode at once, and a caller that finds it empty blocks. Decoding streams
// through a pooled per-message decoder, like snappy and s2.
type zstdCompressor struct{}

var (
	encoderOnce sync.Once
	encoder     *zstd.Encoder

	zstdPools codecPools
)

// initEncoder builds the shared encoder on first use, after paramtable has been
// initialized. The level and CRC are read once; they are not hot-reloadable.
// Decoding needs no such step: it depends on what the peer sent, not on this
// node's settings.
func initEncoder() {
	encoderOnce.Do(func() {
		initCompressionConfig()
		cfg := &paramtable.Get().ProxyGrpcClientCfg
		encoder = newEncoder(codecLevel, codecConcurrency, cfg.CompressionCRC.GetAsBool())
	})
}

// rpcWindowSize caps the zstd match window.
//
// The window dominates an encoder state: fastBase.ensureHist allocates
// 2 x windowSize the first time a state encodes a message larger than a
// 128KB block and never releases it, which at the 8MB default costs ~17.6MB per
// state at level default -- ten times the hash tables alone. Dropping the window
// to 1MB brings that to ~3.6MB. It is free in compression terms for RPC traffic:
// measured over mixed vector-plus-metadata payloads from 4KB to 32MB the ratio
// moves by at most 0.03%, and on a highly repetitive 8MB corpus, the case a long
// window should help most, it does not move at all. A smaller window is also
// strictly easier on the peer's decoder, and this node still decodes frames a
// peer wrote with a larger one.
const rpcWindowSize = 1 << 20

// rpcMaxDecodeWindow caps the match window a peer may declare, which is what
// bounds the history buffer a pooled decoder allocates and keeps.
//
// 8MB covers every encoder that can reach this node: klauspost's EncodeAll
// declares min(nextPow2(len), its own window), so at most 8MB for a build that
// predates rpcWindowSize and at most 1MB after it, and libzstd stays at or
// below 8MB through level 19.
const rpcMaxDecodeWindow = 8 << 20

// newEncoder builds the shared zstd encoder.
//
// Encoder concurrency is the number of encoder states allocated eagerly on
// first use, not parallelism within a single EncodeAll. Each state carries a
// history buffer plus hash tables whose size grows with the level (measured with
// rpcWindowSize: ~2.6MB at fastest, ~3.6MB at default, ~6.5MB at better, ~36MB
// at best), so raising compressionConcurrency at a high level is expensive; it
// is deliberately a separate setting rather than something derived from the
// level.
func newEncoder(level zstd.EncoderLevel, concurrency int, crc bool) *zstd.Encoder {
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(level),
		zstd.WithEncoderConcurrency(concurrency),
		zstd.WithWindowSize(rpcWindowSize),
		zstd.WithEncoderCRC(crc),
	)
	if err != nil {
		// The fallback has to keep the window: zstd.NewWriter(nil) defaults to
		// 8MB, which takes a state from ~3.6MB to ~17.6MB at this level, times
		// the whole pool.
		log.Warn("failed to build the configured zstd encoder, falling back", zap.Error(err))
		enc, _ = zstd.NewWriter(nil, zstd.WithWindowSize(rpcWindowSize))
	}
	return enc
}

// newZstdReader builds one pooled streaming decoder.
//
// Concurrency is pinned to 1 for two reasons. A pooled decoder handles exactly
// one message at a time, and above 1 the library decodes stream blocks on
// goroutines of its own -- a second scheduling layer under gRPC's, which is why
// the s2 writer is pinned the same way. More to the point, above 1 a decoder
// holds a goroutine that only Close releases, and a pooled decoder is never
// closed, so anything above 1 leaks one per message. At 1 the library takes its
// sync path and starts nothing.
//
// The window is capped because gRPC's io.LimitReader does not reach it. That
// limiter bounds the decoded output, which is what makes streaming safe on peer
// input where DecodeAll is not -- but the history buffer is sized from the
// window the frame header declares and allocated before any output exists
// (framedec.go sets allocFrameBuffer, history.ensureBlock allocates it), so a
// few hundred bytes of hostile frame can ask for the library's own
// zstd.MaxWindowSize of 512MB, and the decoder then carries that buffer back
// into the free list. rpcMaxDecodeWindow is the bound the limiter cannot
// provide.
//
// This is reachable without credentials on the Proxy: the compressor registry
// is process-global, so the external port accepts these encodings too, and gRPC
// decompresses in recvAndDecompress before it dispatches through the
// interceptor chain, so before authentication.
func newZstdReader() resettableReader {
	dec, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxWindow(rpcMaxDecodeWindow),
	)
	if err != nil {
		// The fallback has to keep concurrency at 1: above it a decoder holds a
		// goroutine that only Close releases, and a pooled decoder is never
		// closed, so it would leak one per message.
		log.Warn("failed to build the configured zstd decoder, falling back", zap.Error(err))
		dec, _ = zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	}
	return &zstdReader{dec: dec}
}

// zstdReader adapts *zstd.Decoder to resettableReader, whose Reset returns
// nothing. Reset only fails on a decoder that has been Closed, which a pooled
// decoder never is, and Reset(nil) parks one without dropping the buffers it
// has grown -- which is exactly what returning it to the pool wants.
type zstdReader struct {
	dec *zstd.Decoder
}

func (z *zstdReader) Read(p []byte) (int, error) { return z.dec.Read(p) }

func (z *zstdReader) Reset(r io.Reader) { _ = z.dec.Reset(r) }

func (zstdCompressor) Name() string {
	return Zstd
}

func (zstdCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	initEncoder()
	return zstdPools.getWriter(w, func() resettableWriter { return &zstdWriter{} }), nil
}

// Decompress streams rather than calling DecodeAll, which saves two full-size
// copies and one large allocation per message. DecodeAll would have to pull the
// whole compressed frame out of gRPC's buffer, materialize the decoded message
// in one allocation it cannot pool because gRPC takes ownership of it, and then
// let gRPC copy that back into its own pooled buffers. Streaming decodes
// straight into those buffers. It also puts the decode under the io.LimitReader
// gRPC wraps this in, so a frame that expands past MaxCallRecvMsgSize is cut off
// rather than materialized in full first -- DecodeAll on peer input is the shape
// of CVE-2024-36129.
func (zstdCompressor) Decompress(r io.Reader) (io.Reader, error) {
	return zstdPools.getReader(r, newZstdReader), nil
}

// zstdWriter buffers the message and compresses it in one shot on Close. The
// buffer is retained across messages by the pool, so only the first message
// through a given writer pays for it.
type zstdWriter struct {
	buf    bytes.Buffer
	writer io.Writer
}

func (z *zstdWriter) Reset(w io.Writer) {
	// bytes.Buffer.Reset truncates the length but keeps the backing array, and
	// gRPC writes the whole uncompressed message in here before Close. The free
	// list caps how many writers exist, not how many bytes each one holds, and
	// unlike a sync.Pool nothing reclaims them -- so without this a single large
	// message parks its full size, up to MaxCallSendMsgSize, for the life of the
	// process. Same ceiling the compressed side applies in putCompressedBuf.
	if z.buf.Cap() > compressedBufMaxRetain {
		z.buf = bytes.Buffer{}
	} else {
		z.buf.Reset()
	}
	z.writer = w
}

func (z *zstdWriter) Write(p []byte) (int, error) {
	return z.buf.Write(p)
}

func (z *zstdWriter) Close() error {
	if z.writer == nil {
		return nil
	}
	src := z.buf.Bytes()
	compressed := encoder.EncodeAll(src, getCompressedBuf(len(src)))
	// gRPC's writer copies what it is handed (mem.writer.Write -> mem.Copy), so
	// the buffer is free to go back to the pool once Write returns.
	_, err := z.writer.Write(compressed)
	putCompressedBuf(compressed)
	return err
}
