// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// From github.com/apache/arrow/go/v17/parquet/compress/ztsd.go
// to implement a new codec to limit the concurrency of zstd compression.

package compress

import (
	"io"
	"sync"

	"github.com/apache/arrow/go/v17/parquet/compress"
	"github.com/klauspost/compress/zstd"

	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type zstdCodec struct{}

type zstdcloser struct {
	*zstd.Decoder
}

var (
	enc         *zstd.Encoder
	dec         *zstd.Decoder
	initEncoder sync.Once
	initDecoder sync.Once
)

func getencoder() *zstd.Encoder {
	initEncoder.Do(func() {
		enc, _ = zstd.NewWriter(nil, zstd.WithZeroFrames(true))
	})
	return enc
}

func getdecoder() *zstd.Decoder {
	initDecoder.Do(func() {
		dec, _ = zstd.NewReader(nil)
	})
	return dec
}

func (zstdCodec) Decode(dst, src []byte) []byte {
	dst, err := getdecoder().DecodeAll(src, dst[:0])
	if err != nil {
		panic(err)
	}
	return dst
}

func (z *zstdcloser) Close() error {
	z.Decoder.Close()
	return nil
}

func (zstdCodec) NewReader(r io.Reader) io.ReadCloser {
	ret, _ := zstd.NewReader(r)
	return &zstdcloser{ret}
}

func (zstdCodec) NewWriter(w io.Writer) io.WriteCloser {
	ret, _ := zstd.NewWriter(w)
	return ret
}

func (z zstdCodec) NewWriterLevel(w io.Writer, level int) (io.WriteCloser, error) {
	var compressLevel zstd.EncoderLevel
	if level == compress.DefaultCompressionLevel {
		compressLevel = zstd.SpeedDefault
	} else {
		compressLevel = zstd.EncoderLevelFromZstd(level)
	}
	return zstd.NewWriter(w, zstd.WithEncoderLevel(compressLevel), zstd.WithEncoderConcurrency(z.getConcurrency()))
}

func (z zstdCodec) Encode(dst, src []byte) []byte {
	return getencoder().EncodeAll(src, dst[:0])
}

func (z zstdCodec) EncodeLevel(dst, src []byte, level int) []byte {
	compressLevel := zstd.EncoderLevelFromZstd(level)
	if level == compress.DefaultCompressionLevel {
		compressLevel = zstd.SpeedDefault
	}
	enc, _ := zstd.NewWriter(nil, zstd.WithZeroFrames(true), zstd.WithEncoderLevel(compressLevel), zstd.WithEncoderConcurrency(z.getConcurrency()))
	return enc.EncodeAll(src, dst[:0])
}

// from zstd.h, ZSTD_COMPRESSBOUND
func (zstdCodec) CompressBound(len int64) int64 {
	extra := ((128 << 10) - len) >> 11
	if len >= (128 << 10) {
		extra = 0
	}
	return len + (len >> 8) + extra
}

// getConcurrency returns the concurrency for zstd compression.
func (zstdCodec) getConcurrency() int {
	// Every concurrent zstd compress thread will use additional memory, for double fast, 8MB is required.
	// So on a 8-core machine, we will use 64MB memory for zstd compression.
	// And binlog generation can be done in high concurrency, milvus may be OOM if the cpu is throttled.
	// But most of the time, we only serialize 16MB data for one binlog generation.
	// So 1 is enough for most cases to avoid to use too much memory.
	concurrent := paramtable.Get().CommonCfg.StorageZstdConcurrency.GetAsInt()
	if concurrent <= 0 {
		return hardware.GetCPUNum()
	}
	return concurrent
}

func init() {
	compress.RegisterCodec(compress.Codecs.Zstd, zstdCodec{})
}
