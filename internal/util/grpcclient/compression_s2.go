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
	"io"

	"github.com/klauspost/compress/s2"
)

// rpcBlockSize is the block size of the streaming s2 codec, kept at s2's own
// default because anything smaller takes away the only reason to run s2 rather
// than snappy.
//
// s2's gain over snappy is a repeat-offset code and the longer matches it
// allows, and neither pays off inside a small block: measured on protobuf-ish
// metadata, a 64KB block gives s2 exactly snappy's ratio (4.38x) while costing
// ~6% more encode time, a 128KB block gives 4.58x, and 1MB gives 4.77x --
// decoding is ~5% faster at 1MB too. snappy is pinned at 64KB by its format, so
// below roughly 128KB messages s2 is strictly worse than snappy and above it
// this is where the difference lives.
//
// The cost is buffers: a pooled s2 writer holds an input buffer of this size
// plus an output buffer of MaxEncodedLen(blockSize), and a reader a frame
// buffer of ~1.17x. That is what the free lists cap -- at most
// compressionConcurrency x GOMAXPROCS of each. Block size is not a wire
// constraint in either direction: any block below the decoder's maximum
// decodes, and a reader grows its buffer for a peer that sends bigger ones.
const rpcBlockSize = 1 << 20

// s2ReaderStartBlock is only the size an s2 reader allocates up front, and is
// deliberately not rpcBlockSize.
//
// ReaderAllocBlock is eager: the reader allocates MaxEncodedLen of it at
// construction, whatever it will actually decode. Tying that to the block size
// this node writes would make every pooled reader carry ~1.17MB from the moment
// it is built, when what a reader needs is set by the peer, not by us. It grows
// on demand instead -- ensureBufferSize reallocates for a bigger block, capped
// at s2's 4MB maximum -- so starting at snappy's block size keeps the common
// case cheap and costs one realloc against a peer that really does send 1MB
// blocks.
const s2ReaderStartBlock = 64 << 10

// s2Compressor emits the s2 stream format. It is faster and denser than snappy,
// but s2 output cannot be read by a snappy decoder, so it is only usable between
// peers that both register this codec.
type s2Compressor struct{}

var s2Pools codecPools

func (s2Compressor) Name() string {
	return S2
}

func (s2Compressor) Compress(w io.Writer) (io.WriteCloser, error) {
	initCompressionConfig()
	return s2Pools.getWriter(w, func() resettableWriter {
		return s2.NewWriter(nil, s2WriterOptions(s2.WriterBlockSize(rpcBlockSize))...)
	}), nil
}

func (s2Compressor) Decompress(r io.Reader) (io.Reader, error) {
	initCompressionConfig()
	return s2Pools.getReader(r, func() resettableReader {
		// ReaderAllocBlock only sets the initial buffer; the maximum decodable
		// block stays at the s2 default, so frames from a peer using bigger
		// blocks still decode and the buffer grows on demand.
		return s2.NewReader(nil, s2.ReaderAllocBlock(s2ReaderStartBlock))
	}), nil
}
