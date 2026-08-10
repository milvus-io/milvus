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
	"bufio"
	"context"
	"io"
	"net/http"
	"sort"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/render"

	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var jsonContentType = []string{"application/json; charset=utf-8"}

const (
	streamingJSONBufferSize = 64 * 1024
)

type deferredRenderWriter interface {
	DeferRender(render.Render) error
}

type jsonRowSource interface {
	Len() int64
	Row(index int64) (map[string]interface{}, error)
}

type jsonStreamSource interface {
	WriteJSON(context.Context, io.Writer) error
}

type streamIdleProgressWriter struct {
	writer         io.Writer
	deadlineWriter *requestDeadlineWriter
	controller     *streamIdleController
}

func (w *streamIdleProgressWriter) Write(data []byte) (int, error) {
	written := 0
	for len(data) > 0 {
		chunk := data
		if len(chunk) > streamingJSONBufferSize {
			chunk = chunk[:streamingJSONBufferSize]
		}

		n, err := w.writer.Write(chunk)
		if n < 0 || n > len(chunk) {
			return written, &responseTransportError{cause: io.ErrShortWrite}
		}
		written += n
		if err != nil {
			return written, err
		}
		if n != len(chunk) {
			return written, &responseTransportError{cause: io.ErrShortWrite}
		}
		deadline := w.controller.arm()
		if w.deadlineWriter != nil {
			if err := w.deadlineWriter.setStreamIdleDeadline(deadline); err != nil {
				return written, err
			}
		}
		data = data[n:]
	}
	return written, nil
}

type jsonRowStreamSource struct {
	rows     jsonRowSource
	rowCount int64
}

type encodedJSONField struct {
	key    []byte
	value  []byte
	stream jsonStreamSource
}

type streamingJSONRender struct {
	ctx    context.Context
	fields []encodedJSONField
}

type jsonRender struct {
	Context context.Context
	Data    gin.H
}

// Render writes data with custom ContentType.
func (r jsonRender) Render(w http.ResponseWriter) error {
	r.WriteContentType(w)
	ctx := renderContext(r.Context)
	streamSource, ok, err := getJSONStreamSource(r.Data[HTTPReturnData])
	if err != nil {
		return err
	}
	if ok {
		streamRender, err := newStreamingJSONRender(ctx, r.Data, streamSource)
		if err != nil {
			return err
		}
		if deferredWriter, ok := w.(deferredRenderWriter); ok {
			if err := responseContextError(ctx); err != nil {
				return err
			}
			if err := deferredWriter.DeferRender(streamRender); err != nil {
				// A timeout may close the recorder between preflight and renderer
				// registration. Preserve the request cancellation in that race
				// instead of misclassifying the late registration as an invalid
				// search result.
				if ctxErr := responseContextError(ctx); ctxErr != nil {
					return ctxErr
				}
				return err
			}
			return responseContextError(ctx)
		}
		return streamRender.Render(w)
	}

	// Only cooperative stream sources are deferred. Generic JSON encoding may
	// spend substantial CPU time before its first Write, so keep that work in
	// the handler goroutine where timeout arbitration still applies.
	deadlineWriter := newRequestDeadlineWriter(ctx, w)
	defer deadlineWriter.stopDeadlineInterrupt()
	encoder := json.NewEncoder(deadlineWriter)
	return encoder.Encode(r.Data)
}

func renderContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func getJSONStreamSource(data interface{}) (jsonStreamSource, bool, error) {
	if source, ok := data.(jsonStreamSource); ok {
		return source, true, nil
	}
	rows, ok := data.(jsonRowSource)
	if !ok {
		return nil, false, nil
	}
	rowCount := rows.Len()
	if rowCount < 0 {
		return nil, false, merr.WrapErrServiceInternalMsg("JSON row source returned negative row count %d", rowCount)
	}
	return &jsonRowStreamSource{rows: rows, rowCount: rowCount}, true, nil
}

func newStreamingJSONRender(ctx context.Context, data gin.H, streamSource jsonStreamSource) (*streamingJSONRender, error) {
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	streamRender := &streamingJSONRender{
		ctx:    ctx,
		fields: make([]encodedJSONField, 0, len(keys)),
	}
	for _, key := range keys {
		if err := responseContextError(ctx); err != nil {
			return nil, err
		}
		encodedKey, err := json.Marshal(key)
		if err != nil {
			return nil, err
		}
		field := encodedJSONField{key: encodedKey}
		if key == HTTPReturnData {
			field.stream = streamSource
		} else {
			field.value, err = json.Marshal(data[key])
			if err != nil {
				return nil, err
			}
		}
		streamRender.fields = append(streamRender.fields, field)
	}
	return streamRender, nil
}

func (r *streamingJSONRender) Render(w http.ResponseWriter) error {
	r.WriteContentType(w)
	if err := responseContextError(r.ctx); err != nil {
		return err
	}

	deadlineWriter := newRequestDeadlineWriter(r.ctx, w)
	defer deadlineWriter.stopDeadlineInterrupt()
	outputWriter := io.Writer(deadlineWriter)
	if controller := streamIdleControllerFromContext(r.ctx); controller != nil {
		controller.arm()
		defer controller.stop()
		outputWriter = &streamIdleProgressWriter{
			writer:         deadlineWriter,
			deadlineWriter: deadlineWriter,
			controller:     controller,
		}
	}
	bufferedWriter := bufio.NewWriterSize(outputWriter, streamingJSONBufferSize)
	if err := bufferedWriter.WriteByte('{'); err != nil {
		return err
	}
	for index, field := range r.fields {
		if index > 0 {
			if err := bufferedWriter.WriteByte(','); err != nil {
				return err
			}
		}
		if _, err := bufferedWriter.Write(field.key); err != nil {
			return err
		}
		if err := bufferedWriter.WriteByte(':'); err != nil {
			return err
		}

		if field.stream == nil {
			if _, err := bufferedWriter.Write(field.value); err != nil {
				return err
			}
			continue
		}

		// Keep small responses uncommitted while encoding; large responses
		// commit naturally when the bounded buffer fills.
		if err := field.stream.WriteJSON(r.ctx, bufferedWriter); err != nil {
			if ctxErr := responseContextError(r.ctx); ctxErr != nil {
				return ctxErr
			}
			return err
		}
	}
	if err := bufferedWriter.WriteByte('}'); err != nil {
		return err
	}
	if err := bufferedWriter.WriteByte('\n'); err != nil {
		return err
	}
	return bufferedWriter.Flush()
}

func (source *jsonRowStreamSource) WriteJSON(ctx context.Context, w io.Writer) error {
	if _, err := w.Write([]byte{'['}); err != nil {
		return err
	}
	rowEncoder := json.NewEncoder(w)
	for rowIndex := int64(0); rowIndex < source.rowCount; rowIndex++ {
		if err := responseContextError(ctx); err != nil {
			return err
		}
		if rowIndex > 0 {
			if _, err := w.Write([]byte{','}); err != nil {
				return err
			}
		}
		row, err := source.rows.Row(rowIndex)
		if err != nil {
			if ctxErr := responseContextError(ctx); ctxErr != nil {
				return ctxErr
			}
			return err
		}
		// Reuse one streaming encoder. Sonic still materializes one encoded row
		// in scratch, but avoids an additional owned []byte and copy per row.
		if err := rowEncoder.Encode(row); err != nil {
			return err
		}
	}
	_, err := w.Write([]byte{']'})
	return err
}

// WriteContentType writes JSON ContentType.
func (r *streamingJSONRender) WriteContentType(w http.ResponseWriter) {
	writeJSONContentType(w)
}

// WriteContentType writes JSON ContentType.
func (r jsonRender) WriteContentType(w http.ResponseWriter) {
	writeJSONContentType(w)
}

func writeJSONContentType(w http.ResponseWriter) {
	header := w.Header()
	if val := header["Content-Type"]; len(val) == 0 {
		header["Content-Type"] = jsonContentType
	}
}
