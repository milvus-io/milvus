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

	"github.com/klauspost/compress/zstd"
	"google.golang.org/grpc/encoding"
)

const (
	None = ""
	Zstd = "zstd"
)

type grpcCompressor struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

func init() {
	enc, _ := zstd.NewWriter(nil)
	dec, _ := zstd.NewReader(nil)
	c := &grpcCompressor{
		encoder: enc,
		decoder: dec,
	}
	encoding.RegisterCompressor(c)
}

func (c *grpcCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return &zstdWriteCloser{
		enc:    c.encoder,
		writer: w,
	}, nil
}

type zstdWriteCloser struct {
	enc    *zstd.Encoder
	writer io.Writer    // Compressed data will be written here.
	buf    bytes.Buffer // Buffer uncompressed data here, compress on Close.
}

func (z *zstdWriteCloser) Write(p []byte) (int, error) {
	return z.buf.Write(p)
}

func (z *zstdWriteCloser) Close() error {
	compressed := z.enc.EncodeAll(z.buf.Bytes(), nil)
	_, err := io.Copy(z.writer, bytes.NewReader(compressed))
	return err
}

func (c *grpcCompressor) Decompress(r io.Reader) (io.Reader, error) {
	compressed, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	uncompressed, err := c.decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(uncompressed), nil
}

func (c *grpcCompressor) Name() string {
	return Zstd
}
