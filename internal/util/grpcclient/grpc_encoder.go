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
	"bytes"
	"io"

	"github.com/DataDog/zstd" // Import Datadog's zstd package
	"google.golang.org/grpc/encoding"
)

const (
	None = ""
	Zstd = "zstd"
)

type grpcCompressor struct{}

func init() {
	c := &grpcCompressor{}
	encoding.RegisterCompressor(c)
}

func (c *grpcCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return &zstdWriteCloser{
		writer: w,
	}, nil
}

type zstdWriteCloser struct {
	writer io.Writer    // Compressed data will be written here.
	buf    bytes.Buffer // Buffer uncompressed data here, compress on Close.
}

func (z *zstdWriteCloser) Write(p []byte) (int, error) {
	return z.buf.Write(p)
}

func (z *zstdWriteCloser) Close() error {
	// prefer faster compression decompression rather than compression ratio
	compressed, err := zstd.CompressLevel(nil, z.buf.Bytes(), 3)
	if err != nil {
		return err
	}
	_, err = z.writer.Write(compressed)
	return err
}

func (c *grpcCompressor) Decompress(r io.Reader) (io.Reader, error) {
	compressed, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	// Use Datadog's API to decompress data
	uncompressed, err := zstd.Decompress(nil, compressed)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(uncompressed), nil
}

func (c *grpcCompressor) Name() string {
	return Zstd
}
