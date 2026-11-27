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

package numpy

import (
	"bytes"
	"context"
	"io"
	"math"
	"testing"

	"github.com/sbinet/npyio"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/mocks"
)

// positionSensitiveReader models minio-go's Object: ReadAt moves the read
// position and leaves the sequential stream consumed; only Seek resets it.
type positionSensitiveReader struct {
	data     []byte
	off      int
	consumed bool
}

func (r *positionSensitiveReader) Read(p []byte) (int, error) {
	if r.consumed {
		// minio-go converts the exhausted ranged body into ErrUnexpectedEOF
		// because the object size is non-zero.
		return 0, io.ErrUnexpectedEOF
	}
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.off:])
	r.off += n
	return n, nil
}

func (r *positionSensitiveReader) ReadAt(p []byte, off int64) (int, error) {
	r.consumed = true
	r.off = int(off)
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	return copy(p, r.data[off:]), nil
}

func (r *positionSensitiveReader) Seek(off int64, _ int) (int64, error) {
	r.off = int(off)
	r.consumed = false
	return off, nil
}

func (r *positionSensitiveReader) Close() error { return nil }

func (r *positionSensitiveReader) Size() (int64, error) { return int64(len(r.data)), nil }

// TestCreateReadersRewindsAfterHeaderProbe pins the minio-go fix: the header
// pre-check reads through ReadAt, which moves a minio object's own offset, so
// the sequential npyio parse must still start at byte 0. CreateReaders rewinds
// the reader after the probe.
func TestCreateReadersRewindsAfterHeaderProbe(t *testing.T) {
	buf := new(bytes.Buffer)
	require.NoError(t, npyio.Write(buf, []int64{1, 2, 3}))

	fr := &positionSensitiveReader{data: buf.Bytes()}
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().Reader(mock.Anything, "vec.npy").Return(fr, nil).Once()

	r, err := NewReader(context.Background(), cm, numpySchema("vec"), []string{"vec.npy"}, math.MaxInt)
	require.NoError(t, err)
	require.False(t, fr.consumed,
		"the sequential reader must be rewound after the header probe")
	require.NotNil(t, r)
}
