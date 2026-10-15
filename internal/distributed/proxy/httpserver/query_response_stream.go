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

package httpserver

import (
	"context"
	"errors"
	"io"

	"github.com/gin-gonic/gin"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// writeQueryResponse writes one response while owning only one encoded batch.
// The caller must discard buffered output or abort a committed transport on
// error. An error envelope can replace output only before commitment.
// Metadata must exclude data; this function owns the data array's framing.
func writeQueryResponse(ctx context.Context, w io.Writer, metadata gin.H, rows *queryResponseRows, batchRows int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if metadata == nil {
		metadata = gin.H{}
	}
	envelope, err := json.Marshal(metadata)
	if canceled := ctx.Err(); canceled != nil {
		return canceled
	}
	if err != nil {
		return err
	}
	if len(envelope) < 2 || envelope[0] != '{' || envelope[len(envelope)-1] != '}' {
		return merr.WrapErrServiceInternalMsg("response metadata did not encode as an object")
	}
	envelope = envelope[:len(envelope)-1]
	if len(envelope) > 1 {
		envelope = append(envelope, ',')
	}
	envelope = append(envelope, `"data":[`...)

	// Preflight the metadata and first batch before the first write. The helper
	// releases its map slice before returning the encoded batch to the writer.
	batch, err := marshalQueryResponseBatch(ctx, rows, batchRows)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if err := writeQueryResponseBytes(ctx, w, envelope); err != nil {
		return err
	}
	if len(batch) > 0 {
		if err := writeQueryResponseBytes(ctx, w, batch); err != nil {
			return err
		}
	}
	if err := flushQueryResponse(w); err != nil {
		return err
	}
	for err == nil {
		// Do not retain the preceding encoded batch while producing another.
		batch = nil
		batch, err = marshalQueryResponseBatch(ctx, rows, batchRows)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if err := writeQueryResponseBytes(ctx, w, []byte(",")); err != nil {
			return err
		}
		if err := writeQueryResponseBytes(ctx, w, batch); err != nil {
			return err
		}
		if err := flushQueryResponse(w); err != nil {
			return err
		}
	}
	return writeQueryResponseBytes(ctx, w, []byte("]}\n"))
}

func flushQueryResponse(w io.Writer) error {
	if flusher, ok := w.(interface{ FlushError() error }); ok {
		return flusher.FlushError()
	}
	return nil
}

func marshalQueryResponseBatch(ctx context.Context, rows *queryResponseRows, batchRows int) ([]byte, error) {
	batch, err := rows.Next(ctx, batchRows)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(batch)
	if canceled := ctx.Err(); canceled != nil {
		return nil, canceled
	}
	if err != nil {
		return nil, err
	}
	// Only strip delimiters verified on bytes produced by our array encoder.
	if len(encoded) < 2 || encoded[0] != '[' || encoded[len(encoded)-1] != ']' {
		return nil, merr.WrapErrServiceInternalMsg("response batch did not encode as an array")
	}
	return encoded[1 : len(encoded)-1], nil
}

func writeQueryResponseBytes(ctx context.Context, w io.Writer, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n, err := w.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return ctx.Err()
}
