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
	"io"
	"sort"

	"github.com/bytedance/sonic"
	"github.com/bytedance/sonic/encoder"
)

const dqlJSONBatchRows = 256

// writeDQLJSON keeps the existing ConfigStd wire representation, but reuses
// request-owned encoding and escaping buffers between batches. Write must finish
// consuming its argument before returning (the io.Writer contract).
// Row count bounds batching, not bytes: one large row can still grow the buffers.
func writeDQLJSON(ctx context.Context, w io.Writer, result map[string]interface{}, batchRows int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rows, ok := result[HTTPReturnData].([]map[string]interface{})
	if !ok || len(rows) == 0 {
		return sonic.ConfigStd.NewEncoder(w).Encode(result)
	}
	// Encode small envelope fields before committing anything. Keep sorted keys,
	// including fields after data; an encoding error here must not send partial JSON.
	keys := make([]string, 0, len(result))
	for key := range result {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	prefix, suffix := []byte{'{'}, []byte{}
	dst := &prefix
	for i, key := range keys {
		if i != 0 {
			*dst = append(*dst, ',')
		}
		name, err := sonic.ConfigStd.Marshal(key)
		if err != nil {
			return err
		}
		*dst = append(*dst, name...)
		*dst = append(*dst, ':')
		if key == "data" {
			*dst = append(*dst, '[')
			suffix = append(suffix, ']')
			dst = &suffix
			continue
		}
		value, err := sonic.ConfigStd.Marshal(result[key])
		if err != nil {
			return err
		}
		*dst = append(*dst, value...)
	}
	suffix = append(suffix, '}', '\n')
	write := func(p []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := w.Write(p)
		if err == nil && n != len(p) {
			return io.ErrShortWrite
		}
		return err
	}
	// EscapeHTML inside EncodeInto allocates a fresh destination. Escape explicitly
	// into a second reusable buffer instead, preserving the existing HTML policy.
	// The fallback encoder otherwise appends a newline after the closing bracket.
	options := encoder.SortMapKeys | encoder.CompactMarshaler | encoder.ValidateString | encoder.NoEncoderNewline
	raw, escaped := make([]byte, 0, 4096), make([]byte, 0, 4096)
	for start := 0; start < len(rows); start += batchRows {
		if err := ctx.Err(); err != nil {
			return err
		}
		raw = raw[:0]
		if err := encoder.EncodeInto(&raw, rows[start:min(start+batchRows, len(rows))], options); err != nil {
			return err
		}
		escaped = encoder.HTMLEscape(escaped[:0], raw)
		if start == 0 {
			if err := write(prefix); err != nil {
				return err
			}
		} else if err := write([]byte{','}); err != nil {
			return err
		}
		// Each batch is an array; join its elements into the single response array.
		if err := write(escaped[1 : len(escaped)-1]); err != nil {
			return err
		}
	}
	return write(suffix)
}
