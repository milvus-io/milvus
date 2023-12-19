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

package storage

import (
	"io"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
)

var errInvalid = errors.New("invalid argument")

// MemoryFile implements the FileReader interface
type MemoryFile struct {
	data     []byte
	position int
}

// NewMemoryFile creates a new instance of MemoryFile
func NewMemoryFile(data []byte) *MemoryFile {
	return &MemoryFile{data: data}
}

// ReadAt implements the ReadAt method of the io.ReaderAt interface
func (mf *MemoryFile) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 || int64(int(off)) < off {
		return 0, errInvalid
	}
	if off > int64(len(mf.data)) {
		return 0, io.EOF
	}
	n = copy(p, mf.data[off:])
	mf.position += n
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// Seek implements the Seek method of the io.Seeker interface
func (mf *MemoryFile) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = int64(mf.position) + offset
	case io.SeekEnd:
		newOffset = int64(len(mf.data)) + offset
	default:
		return 0, errInvalid
	}
	if newOffset < 0 {
		return 0, errInvalid
	}
	mf.position = int(newOffset)
	return newOffset, nil
}

// Read implements the Read method of the io.Reader interface
func (mf *MemoryFile) Read(p []byte) (n int, err error) {
	if mf.position >= len(mf.data) {
		return 0, io.EOF
	}
	n = copy(p, mf.data[mf.position:])
	mf.position += n
	return n, nil
}

// Write implements the Write method of the io.Writer interface
func (mf *MemoryFile) Write(p []byte) (n int, err error) {
	// Write data to memory
	mf.data = append(mf.data, p...)
	return len(p), nil
}

// Close implements the Close method of the io.Closer interface
func (mf *MemoryFile) Close() error {
	// Memory file does not need a close operation
	return nil
}

type AzureFile struct {
	*MemoryFile
}

func NewAzureFile(body io.ReadCloser) *AzureFile {
	data, err := io.ReadAll(body)
	defer body.Close()
	if err != nil && err != io.EOF {
		log.Warn("create azure file failed, read data failed", zap.Error(err))
		return &AzureFile{
			NewMemoryFile(nil),
		}
	}

	return &AzureFile{
		NewMemoryFile(data),
	}
}
