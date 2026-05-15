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

package common

import (
	"io"
	"strings"

	"github.com/milvus-io/milvus/internal/storage"
)

// mockFileReader is a mock implementation of storage.FileReader for testing.
type mockFileReader struct {
	io.Reader
	io.Closer
	io.ReaderAt
	io.Seeker
	size int64

	err      error
	errCount int
}

func NewMockReader(content string) storage.FileReader {
	reader := strings.NewReader(content)
	return &mockFileReader{
		Reader:   reader,
		Closer:   io.NopCloser(reader),
		ReaderAt: reader,
		Seeker:   reader,
		size:     int64(len(content)),
		err:      nil,
		errCount: 0,
	}
}

func CustomMockReader(reader io.Reader) storage.FileReader {
	return &mockFileReader{
		Reader: reader,
		Closer: io.NopCloser(reader),
		size:   0,
	}
}

type prematureEOFReader struct {
	*strings.Reader
	limit        int
	read         int
	reportedSize int64
}

// NewPrematureEOFReader creates a test reader that returns EOF before the reported object size is reached.
func NewPrematureEOFReader(content string, limit int) storage.FileReader {
	return NewPrematureEOFReaderWithSize(content, limit, int64(len(content)))
}

// NewPrematureEOFReaderWithSize creates a test reader that reports a custom object size.
func NewPrematureEOFReaderWithSize(content string, limit int, reportedSize int64) storage.FileReader {
	reader := strings.NewReader(content)
	return &prematureEOFReader{
		Reader:       reader,
		limit:        limit,
		reportedSize: reportedSize,
	}
}

func (r *prematureEOFReader) Read(p []byte) (int, error) {
	if r.read >= r.limit {
		return 0, io.EOF
	}
	if remaining := r.limit - r.read; len(p) > remaining {
		p = p[:remaining]
	}
	n, err := r.Reader.Read(p)
	r.read += n
	return n, err
}

func (r *prematureEOFReader) Close() error {
	return nil
}

func (r *prematureEOFReader) Size() (int64, error) {
	return r.reportedSize, nil
}

func newErrorMockReader(content string, err error, errCount int) storage.FileReader {
	reader := strings.NewReader(content)
	return &mockFileReader{
		Reader:   reader,
		Closer:   io.NopCloser(reader),
		ReaderAt: reader,
		Seeker:   reader,
		size:     int64(len(content)),
		err:      err,
		errCount: errCount,
	}
}

func (m *mockFileReader) Read(p []byte) (int, error) {
	if m.errCount > 0 {
		m.errCount--
		return 0, m.err
	}
	return m.Reader.Read(p)
}

func (m *mockFileReader) Size() (int64, error) {
	return m.size, nil
}
