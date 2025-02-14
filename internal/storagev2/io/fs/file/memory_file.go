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

package file

import (
	"io"

	"github.com/cockroachdb/errors"
)

var errInvalid = errors.New("invalid argument")

type MemoryFile struct {
	b []byte
	i int
}

func (f *MemoryFile) Close() error {
	return nil
}

func (f *MemoryFile) Read(p []byte) (n int, err error) {
	if f.i >= len(f.b) {
		return 0, io.EOF
	}
	n = copy(p, f.b[f.i:])
	f.i += n
	return n, nil
}

func (f *MemoryFile) Write(b []byte) (int, error) {
	n, err := f.writeAt(b, int64(f.i))
	f.i += n
	return n, err
}

func (f *MemoryFile) writeAt(b []byte, off int64) (int, error) {
	if off < 0 || int64(int(off)) < off {
		return 0, errInvalid
	}
	if off > int64(len(f.b)) {
		f.truncate(off)
	}
	n := copy(f.b[off:], b)
	f.b = append(f.b, b[n:]...)
	return len(b), nil
}

func (f *MemoryFile) truncate(n int64) error {
	switch {
	case n < 0 || int64(int(n)) < n:
		return errInvalid
	case n <= int64(len(f.b)):
		f.b = f.b[:n]
		return nil
	default:
		f.b = append(f.b, make([]byte, int(n)-len(f.b))...)
		return nil
	}
}

func (f *MemoryFile) ReadAt(b []byte, off int64) (n int, err error) {
	if off < 0 || int64(int(off)) < off {
		return 0, errInvalid
	}
	if off > int64(len(f.b)) {
		return 0, io.EOF
	}
	n = copy(b, f.b[off:])
	f.i += n
	if n < len(b) {
		return n, io.EOF
	}
	return n, nil
}

func (f *MemoryFile) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = int64(f.i) + offset
	case io.SeekEnd:
		abs = int64(len(f.b)) + offset
	default:
		return 0, errInvalid
	}
	if abs < 0 {
		return 0, errInvalid
	}
	f.i = int(abs)
	return abs, nil
}

func (f *MemoryFile) Bytes() []byte {
	return f.b
}

func NewMemoryFile(b []byte) *MemoryFile {
	return &MemoryFile{
		b: b,
	}
}
