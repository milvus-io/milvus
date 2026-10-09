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
	"github.com/klauspost/compress/snappy"
)

// snappyCompressor emits the snappy stream format, so its output can be read by
// any snappy decoder as well as by s2. It is built on the s2 writer in
// snappy-compatible mode, which caps the block size at 64KB as the format
// requires.
type snappyCompressor struct{}

var snappyPools codecPools

func (snappyCompressor) Name() string {
	return Snappy
}

func (snappyCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	initCompressionConfig()
	return snappyPools.getWriter(w, func() resettableWriter {
		return s2.NewWriter(nil, s2WriterOptions(s2.WriterSnappyCompat())...)
	}), nil
}

func (snappyCompressor) Decompress(r io.Reader) (io.Reader, error) {
	initCompressionConfig()
	return snappyPools.getReader(r, func() resettableReader {
		// snappy.NewReader is an s2 reader capped at the snappy block size.
		return snappy.NewReader(nil)
	}), nil
}
