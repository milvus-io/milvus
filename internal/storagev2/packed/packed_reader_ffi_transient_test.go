// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package packed

import (
	"fmt"
	"io"
	"syscall"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type fakeTransientStreamReader struct {
	err error
}

func (f fakeTransientStreamReader) Read() (arrow.Record, error) {
	return nil, f.err
}

func TestReadNextMapsRetryableStreamErrno(t *testing.T) {
	r := &FFIPackedReader{recordReader: fakeTransientStreamReader{
		err: fmt.Errorf("%w: StorageTransientThrottling", syscall.EAGAIN),
	}}

	_, err := r.ReadNext()
	require.Error(t, err)
	require.ErrorIs(t, err, ErrLoonTransient)
}

func TestReadNextKeepsPermanentStreamErrorAsStorage(t *testing.T) {
	r := &FFIPackedReader{recordReader: fakeTransientStreamReader{
		err: fmt.Errorf("%w: AwsErrorNotFound", syscall.EIO),
	}}

	_, err := r.ReadNext()
	require.Error(t, err)
	require.ErrorIs(t, err, merr.ErrStorage)
	require.False(t, errors.Is(err, ErrLoonTransient))
}

func TestReadNextPassesEOF(t *testing.T) {
	r := &FFIPackedReader{recordReader: fakeTransientStreamReader{err: io.EOF}}

	_, err := r.ReadNext()
	require.ErrorIs(t, err, io.EOF)
}
