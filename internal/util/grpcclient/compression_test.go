// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package grpcclient

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
)

func TestGrpcEncoder(t *testing.T) {
	data := "hello zstd algorithm!"
	var buf bytes.Buffer

	compressor := encoding.GetCompressor(Zstd)
	writer, err := compressor.Compress(&buf)
	assert.NoError(t, err)
	written, err := writer.Write([]byte(data))
	assert.NoError(t, err)
	assert.Equal(t, written, len(data))
	err = writer.Close()
	assert.NoError(t, err)

	reader, err := compressor.Decompress(bytes.NewReader(buf.Bytes()))
	assert.NoError(t, err)
	// Read to EOF rather than a single Read: the zstd decompressor streams, so
	// one Read fills whatever the decoder has decoded so far, not the message.
	result, err := io.ReadAll(reader)
	assert.NoError(t, err)
	assert.Equal(t, data, string(result))
}

func TestS2Compressor(t *testing.T) {
	data := "hello s2 algorithm! hello s2 algorithm! hello s2 algorithm!"
	var buf bytes.Buffer

	compressor := encoding.GetCompressor(S2)
	assert.NotNil(t, compressor)
	writer, err := compressor.Compress(&buf)
	assert.NoError(t, err)
	written, err := writer.Write([]byte(data))
	assert.NoError(t, err)
	assert.Equal(t, written, len(data))
	err = writer.Close()
	assert.NoError(t, err)

	reader, err := compressor.Decompress(bytes.NewReader(buf.Bytes()))
	assert.NoError(t, err)
	result, err := io.ReadAll(reader)
	assert.NoError(t, err)
	assert.Equal(t, data, string(result))
}

// Block size is not a wire constraint, so verify both directions: our own
// multi-block stream, and a stream a peer produced with a different block size.
func TestS2BlockSizeCompatibility(t *testing.T) {
	// several times rpcBlockSize so the stream really spans several blocks
	data := bytes.Repeat([]byte("milvus grpc compression payload "), (4*rpcBlockSize)/32)
	compressor := encoding.GetCompressor(S2)
	assert.NotNil(t, compressor)

	t.Run("multi block round trip", func(t *testing.T) {
		var buf bytes.Buffer
		writer, err := compressor.Compress(&buf)
		assert.NoError(t, err)
		_, err = writer.Write(data)
		assert.NoError(t, err)
		assert.NoError(t, writer.Close())

		reader, err := compressor.Decompress(bytes.NewReader(buf.Bytes()))
		assert.NoError(t, err)
		result, err := io.ReadAll(reader)
		assert.NoError(t, err)
		assert.Equal(t, data, result)
	})

	t.Run("decodes a peer using a larger block than ours", func(t *testing.T) {
		var buf bytes.Buffer
		// 4MB is s2's maximum, so this is the widest a peer can go.
		peer := s2.NewWriter(&buf, s2.WriterBlockSize(4<<20))
		_, err := peer.Write(data)
		assert.NoError(t, err)
		assert.NoError(t, peer.Close())

		reader, err := compressor.Decompress(bytes.NewReader(buf.Bytes()))
		assert.NoError(t, err)
		result, err := io.ReadAll(reader)
		assert.NoError(t, err)
		assert.Equal(t, data, result)
	})
}

func TestCompressedBufSizing(t *testing.T) {
	// These assert on the allocation path, so the free list has to be empty.
	// It is a channel, not a sync.Pool, so what an earlier test parked stays
	// parked rather than being cleared by the next GC.
	drain := func() {
		initCompressedBufs()
		for len(compressedBufs) > 0 {
			<-compressedBufs
		}
	}
	drain()
	t.Cleanup(drain)

	t.Run("small message gets a small buffer", func(t *testing.T) {
		assert.Equal(t, compressedBufMinSize, cap(getCompressedBuf(100)))
	})

	t.Run("buffer is sized from the message", func(t *testing.T) {
		assert.Equal(t, 64<<10, cap(getCompressedBuf(64<<10)))
	})

	t.Run("a huge message does not pull a huge buffer", func(t *testing.T) {
		assert.Equal(t, compressedBufMaxRetain, cap(getCompressedBuf(256<<20)))
	})

	t.Run("oversized buffers are dropped instead of pooled", func(t *testing.T) {
		putCompressedBuf(make([]byte, 0, compressedBufMaxRetain+1))
		// nothing retained, so the next get allocates from the size hint
		assert.Equal(t, compressedBufMinSize, cap(getCompressedBuf(1)))
	})

	t.Run("buffers within the ceiling are reused", func(t *testing.T) {
		putCompressedBuf(make([]byte, 0, compressedBufMaxRetain))
		assert.Equal(t, compressedBufMaxRetain, cap(getCompressedBuf(1)))
	})
}

// The reader must survive gRPC's access patterns: reads past EOF, an explicit
// Close (newer grpc releases do this), and Close after the stream is drained.
func TestPooledReaderReleaseIsIdempotent(t *testing.T) {
	msg := []byte("hello pooled reader")
	compressor := encoding.GetCompressor(S2)

	var buf bytes.Buffer
	w, err := compressor.Compress(&buf)
	assert.NoError(t, err)
	_, err = w.Write(msg)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	r, err := compressor.Decompress(bytes.NewReader(buf.Bytes()))
	assert.NoError(t, err)

	got, err := io.ReadAll(r)
	assert.NoError(t, err)
	assert.Equal(t, msg, got)

	// reading past EOF stays at EOF rather than panicking on the released reader
	n, err := r.Read(make([]byte, 1))
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)

	closer, ok := r.(io.Closer)
	assert.True(t, ok, "grpc closes the decompressor when it implements io.Closer")
	assert.NoError(t, closer.Close())
	assert.NoError(t, closer.Close())
}

// A decompressor goes back to its pool the moment a message is drained, and a
// message that fails to decode drains through the error path. Whatever the
// codec left behind has to be cleared on the way back, or one bad frame
// poisons a pooled object and every later message that draws it fails too.
// The free list hands objects back in order, so a poisoned one is drawn on the
// very next message rather than eventually; each case still runs several rounds
// so a codec that only fails on reuse has to show it.
func TestPooledDecompressorSurvivesBadFrames(t *testing.T) {
	data := bytes.Repeat([]byte("milvus grpc payload chunk "), (1<<20)/26)

	for _, name := range []string{Zstd, Snappy, S2} {
		t.Run(name, func(t *testing.T) {
			compressor := encoding.GetCompressor(name)
			require.NotNil(t, compressor)

			var good bytes.Buffer
			w, err := compressor.Compress(&good)
			require.NoError(t, err)
			_, err = w.Write(data)
			require.NoError(t, err)
			require.NoError(t, w.Close())
			frame := good.Bytes()

			readBack := func(t *testing.T, b []byte) ([]byte, error) {
				t.Helper()
				reader, err := compressor.Decompress(bytes.NewReader(b))
				if err != nil {
					return nil, err
				}
				return io.ReadAll(reader)
			}

			corrupt := append([]byte(nil), frame...)
			// Past the frame header, so the damage surfaces while decoding
			// rather than while identifying the format.
			for i := len(corrupt) / 2; i < len(corrupt)/2+64 && i < len(corrupt); i++ {
				corrupt[i] ^= 0xff
			}
			truncated := frame[:len(frame)/2]

			for _, bad := range [][]byte{corrupt, truncated} {
				for i := 0; i < 8; i++ {
					_, err := readBack(t, bad)
					require.Error(t, err, "bad frame decoded clean; the test is no longer exercising the error path")
					got, err := readBack(t, frame)
					require.NoError(t, err, "good frame failed after a bad one")
					require.Equal(t, data, got)
				}
			}
		})
	}
}

// Every encoder that reaches this node today calls EncodeAll, which emits a
// single-segment frame carrying the decoded size. A peer that streams instead
// emits neither, and declares a real match window the decoder has to honor --
// a different path through the decoder, so decode one of those too.
func TestZstdDecodesStreamEncodedPeer(t *testing.T) {
	data := bytes.Repeat([]byte("milvus grpc payload chunk "), (1<<20)/26)
	compressor := encoding.GetCompressor(Zstd)
	require.NotNil(t, compressor)

	for _, window := range []int{1 << 20, 8 << 20} {
		t.Run(fmt.Sprintf("%dMB window", window>>20), func(t *testing.T) {
			var frame bytes.Buffer
			peer, err := zstd.NewWriter(&frame, zstd.WithWindowSize(window))
			require.NoError(t, err)
			_, err = peer.Write(data)
			require.NoError(t, err)
			require.NoError(t, peer.Close())

			reader, err := compressor.Decompress(bytes.NewReader(frame.Bytes()))
			require.NoError(t, err)
			got, err := io.ReadAll(reader)
			require.NoError(t, err)
			require.Equal(t, data, got)
		})
	}
}

// maxBenchRecvSize mirrors milvus.yaml's clientMaxRecvSize, the limit gRPC
// wraps every decompressor in.
const maxBenchRecvSize = 268435456

func BenchmarkDecompress(b *testing.B) {
	pool := mem.DefaultBufferPool()
	for _, name := range []string{Zstd, Snappy, S2} {
		compressor := encoding.GetCompressor(name)
		for _, size := range []int{4 << 10, 64 << 10, 1 << 20, 8 << 20} {
			data := bytes.Repeat([]byte("milvus grpc payload chunk "), size/26)
			var buf bytes.Buffer
			w, err := compressor.Compress(&buf)
			require.NoError(b, err)
			_, err = w.Write(data)
			require.NoError(b, err)
			require.NoError(b, w.Close())
			frame := buf.Bytes()

			b.Run(fmt.Sprintf("%s/%dKB", name, size>>10), func(b *testing.B) {
				b.SetBytes(int64(len(data)))
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					reader, err := compressor.Decompress(bytes.NewReader(frame))
					if err != nil {
						b.Fatal(err)
					}
					// Exactly what grpc's decompress() does with the reader.
					// Consuming it any other way lets a reader that hands back
					// one big slice skip the copy into gRPC's buffers, which a
					// streaming one cannot, and the comparison stops meaning
					// anything.
					out, err := mem.ReadAll(io.LimitReader(reader, maxBenchRecvSize), pool)
					if err != nil {
						b.Fatal(err)
					}
					if out.Len() != len(data) {
						b.Fatalf("decoded %d bytes, want %d", out.Len(), len(data))
					}
					out.Free()
				}
			})
		}
	}
}

// The free lists are bounded: a burst wider than the ceiling has to keep
// working, handing the surplus objects to the GC rather than blocking on a full
// channel or growing without limit. gRPC sets no ceiling on concurrent streams
// by default, so the burst width here is deliberately well past the cap.
func TestCodecPoolsAreBounded(t *testing.T) {
	data := bytes.Repeat([]byte("milvus grpc payload chunk "), (64<<10)/26)
	initCompressionConfig()
	ceiling := codecConcurrency

	for _, name := range []string{Zstd, Snappy, S2} {
		t.Run(name, func(t *testing.T) {
			compressor := encoding.GetCompressor(name)
			require.NotNil(t, compressor)

			var wg sync.WaitGroup
			for i := 0; i < 8*ceiling; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					var buf bytes.Buffer
					w, err := compressor.Compress(&buf)
					require.NoError(t, err)
					_, err = w.Write(data)
					require.NoError(t, err)
					require.NoError(t, w.Close())

					r, err := compressor.Decompress(bytes.NewReader(buf.Bytes()))
					require.NoError(t, err)
					got, err := io.ReadAll(r)
					require.NoError(t, err)
					require.Equal(t, data, got)
				}()
			}
			wg.Wait()
		})
	}

	// Whatever the burst did, the free lists are still sized from the config
	// and hold no more than that.
	for _, pools := range []*codecPools{&zstdPools, &snappyPools, &s2Pools} {
		require.Equal(t, ceiling, cap(pools.writers))
		require.Equal(t, ceiling, cap(pools.readers))
		require.LessOrEqual(t, len(pools.writers), ceiling)
		require.LessOrEqual(t, len(pools.readers), ceiling)
	}
	require.Equal(t, ceiling, cap(compressedBufs))
	require.LessOrEqual(t, len(compressedBufs), ceiling)
}

// A pooled zstd writer buffers the whole uncompressed message before it
// compresses, so returning it to the free list has to drop a buffer that one
// large message grew. The free list caps how many writers exist, not how many
// bytes each holds, and unlike a sync.Pool nothing reclaims them.
func TestZstdWriterDropsOversizedBuffer(t *testing.T) {
	big := bytes.Repeat([]byte("milvus grpc payload chunk "), (8<<20)/25)
	compressor := encoding.GetCompressor(Zstd)
	require.NotNil(t, compressor)

	// Drain first so the writer inspected below is the one this test parked.
	zstdPools.init()
	for len(zstdPools.writers) > 0 {
		<-zstdPools.writers
	}

	var out bytes.Buffer
	w, err := compressor.Compress(&out)
	require.NoError(t, err)
	_, err = w.Write(big)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	select {
	case parked := <-zstdPools.writers:
		zw, ok := parked.(*zstdWriter)
		require.True(t, ok)
		require.LessOrEqual(t, zw.buf.Cap(), compressedBufMaxRetain,
			"an 8MB message must not park its input buffer in the free list")
	default:
		t.Fatal("writer was not returned to the free list")
	}
}

// The decoder's history buffer is sized from the window a frame declares and
// allocated before any output, so gRPC's io.LimitReader cannot bound it. The cap
// has to reject a window no real peer would send while still accepting the 8MB
// one that every build predating rpcWindowSize used.
func TestZstdRejectsOversizedDeclaredWindow(t *testing.T) {
	compressor := encoding.GetCompressor(Zstd)
	require.NotNil(t, compressor)
	// Past the encoder's 128KB block buffer, so the frame really streams and
	// declares the configured window rather than collapsing to EncodeAll.
	data := bytes.Repeat([]byte("milvus grpc payload chunk "), (256<<10)/26)

	frameWithWindow := func(t *testing.T, window int) []byte {
		t.Helper()
		var buf bytes.Buffer
		peer, err := zstd.NewWriter(&buf, zstd.WithWindowSize(window))
		require.NoError(t, err)
		_, err = peer.Write(data)
		require.NoError(t, err)
		require.NoError(t, peer.Close())
		return buf.Bytes()
	}

	t.Run("accepts the 8MB window older peers use", func(t *testing.T) {
		r, err := compressor.Decompress(bytes.NewReader(frameWithWindow(t, rpcMaxDecodeWindow)))
		require.NoError(t, err)
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.Equal(t, data, got)
	})

	t.Run("rejects a window past the cap", func(t *testing.T) {
		r, err := compressor.Decompress(bytes.NewReader(frameWithWindow(t, 2*rpcMaxDecodeWindow)))
		require.NoError(t, err)
		_, err = io.ReadAll(r)
		require.Error(t, err, "a frame declaring more than rpcMaxDecodeWindow must not be decoded")
	})
}
