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
	"sync"

	"github.com/milvus-io/milvus/internal/storageprofile"
)

type instrumentedFileReader struct {
	inner     FileReader
	operation storageprofile.OperationRecorder
	classify  func(error) storageprofile.ErrorCategory

	firstByteOnce sync.Once
	finishOnce    sync.Once
}

func newInstrumentedFileReader(inner FileReader, operation storageprofile.OperationRecorder, classifiers ...func(error) storageprofile.ErrorCategory) FileReader {
	reader := &instrumentedFileReader{inner: inner, operation: operation}
	if len(classifiers) > 0 {
		reader.classify = classifiers[0]
	}
	return reader
}

// DetachProfiledFileReader transfers ownership of an instrumented reader to a
// higher-level logical operation (for example, a retrying reader). The nested
// observation is ignored so reopen attempts do not double count.
func DetachProfiledFileReader(reader FileReader) FileReader {
	profiled, ok := reader.(*instrumentedFileReader)
	if !ok {
		return reader
	}
	profiled.finish(storageprofile.OperationResult{Ignored: true})
	return profiled.inner
}

func (r *instrumentedFileReader) Read(buffer []byte) (int, error) {
	n, err := r.inner.Read(buffer)
	r.observeRead(n)
	r.observeResult(err)
	return n, err
}

func (r *instrumentedFileReader) ReadAt(buffer []byte, offset int64) (int, error) {
	n, err := r.inner.ReadAt(buffer, offset)
	r.observeRead(n)
	r.observeResult(err)
	return n, err
}

func (r *instrumentedFileReader) Seek(offset int64, whence int) (int64, error) {
	position, err := r.inner.Seek(offset, whence)
	if err != nil {
		r.finish(r.operationResult(err, true))
	}
	return position, err
}

func (r *instrumentedFileReader) Size() (int64, error) {
	size, err := r.inner.Size()
	if err != nil {
		r.finish(r.operationResult(err, true))
	}
	return size, err
}

func (r *instrumentedFileReader) Close() error {
	err := r.inner.Close()
	r.finish(r.operationResult(err, true))
	return err
}

func (r *instrumentedFileReader) observeRead(n int) {
	if n <= 0 {
		return
	}
	r.operation.AddCompletedBytes(uint64(n))
	r.firstByteOnce.Do(r.operation.FirstByte)
}

func (r *instrumentedFileReader) observeResult(err error) {
	switch err {
	case nil:
		return
	case io.EOF:
		r.finish(storageprofile.OperationResult{SizeKnown: true})
	default:
		r.finish(r.operationResult(err, true))
	}
}

func (r *instrumentedFileReader) operationResult(err error, sizeKnown bool) storageprofile.OperationResult {
	result := storageprofile.OperationResult{Err: err, SizeKnown: sizeKnown}
	if err != nil && r.classify != nil {
		result.Category = r.classify(err)
	}
	return result
}

func (r *instrumentedFileReader) finish(result storageprofile.OperationResult) {
	r.finishOnce.Do(func() { r.operation.Finish(result) })
}
