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
// grpc zstd implementation from https://github.com/cortexproject/cortex

package grpcclient

import (
	"io"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc/encoding"
)

// grpcCompressor is now thread-safe: each call to Compress or Decompress
// creates a fresh encoder/decoder instance, so no shared state is used.
type zstdCompressor struct{}

func init() {
	encoding.RegisterCompressor(&zstdCompressor{})
}

// Compress returns a new zstd.Encoder that writes compressed data directly to w.
// Because each call to NewWriter allocates its own Encoder, this is safe for concurrent use.
func (c *zstdCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return zstd.NewWriter(w)
}

// Decompress returns a new zstd.Decoder that reads from r.
// Each call to NewReader returns a separate Decoder, making this safe to call from multiple goroutines.
func (c *zstdCompressor) Decompress(r io.Reader) (io.Reader, error) {
	dec, err := zstd.NewReader(r)
	if err != nil {
		return nil, err
	}
	return dec, nil
}

func (c *zstdCompressor) Name() string {
	return "zstd"
}
