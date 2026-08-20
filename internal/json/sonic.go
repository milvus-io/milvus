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

package json

import (
	gojson "encoding/json"
	"io"

	"github.com/bytedance/sonic"
)

var json = sonic.ConfigStd

// sonic JIT-compiles a dedicated decoder/encoder for every type on first use.
// The compilation registers a synthetic runtime module concurrently with
// plugin.Open and can crash the process (see gate.go). Every entry point below
// therefore takes the reader side of the gate, so JIT registration never
// overlaps with plugin loading.

// Marshal returns the JSON encoding bytes of v.
func Marshal(v any) ([]byte, error) {
	acquireRead()
	defer releaseRead()
	return json.Marshal(v)
}

// MarshalIndent returns the JSON encoding bytes of v with prefix and indent.
func MarshalIndent(v any, prefix, indent string) ([]byte, error) {
	acquireRead()
	defer releaseRead()
	return json.MarshalIndent(v, prefix, indent)
}

// Unmarshal parses the JSON-encoded data and stores the result in the value
// pointed to by v.
func Unmarshal(data []byte, v any) error {
	acquireRead()
	defer releaseRead()
	return json.Unmarshal(data, v)
}

// Valid reports whether data is a valid JSON encoding.
func Valid(data []byte) bool {
	acquireRead()
	defer releaseRead()
	return json.Valid(data)
}

// NewDecoder returns a decoder that reads from r. The returned Decoder holds
// the reader side of the gate for the whole decode operation.
func NewDecoder(r io.Reader) *Decoder {
	acquireRead()
	defer releaseRead()
	return &Decoder{decoder: json.NewDecoder(r)}
}

// NewEncoder returns an encoder that writes to w. The returned Encoder holds
// the reader side of the gate for the whole encode operation.
func NewEncoder(w io.Writer) *Encoder {
	acquireRead()
	defer releaseRead()
	return &Encoder{encoder: json.NewEncoder(w)}
}

// Decoder reads and decodes JSON values from an input stream.
type Decoder struct {
	decoder sonic.Decoder
}

// Decode reads the next JSON-encoded value from its input and stores it in the
// value pointed to by v.
func (d *Decoder) Decode(v any) error {
	acquireRead()
	defer releaseRead()
	return d.decoder.Decode(v)
}

// Buffered returns a reader of the data remaining in the Decoder's buffer.
func (d *Decoder) Buffered() io.Reader {
	return d.decoder.Buffered()
}

// DisallowUnknownFields causes the Decoder to return an error when the
// destination is a struct and the input contains object keys which do not match
// any non-ignored, exported fields in the destination.
func (d *Decoder) DisallowUnknownFields() {
	d.decoder.DisallowUnknownFields()
}

// More reports whether there is another element in the current array or object
// being parsed.
func (d *Decoder) More() bool {
	return d.decoder.More()
}

// UseNumber causes the Decoder to unmarshal a number into an interface{} as a
// Number instead of as a float64.
func (d *Decoder) UseNumber() {
	d.decoder.UseNumber()
}

// Encoder writes JSON values to an output stream.
type Encoder struct {
	encoder sonic.Encoder
}

// Encode writes the JSON encoding of v to the stream, followed by a newline.
func (e *Encoder) Encode(v any) error {
	acquireRead()
	defer releaseRead()
	return e.encoder.Encode(v)
}

// SetEscapeHTML specifies whether problematic HTML characters should be
// escaped inside JSON quoted strings.
func (e *Encoder) SetEscapeHTML(on bool) {
	e.encoder.SetEscapeHTML(on)
}

// SetIndent instructs the encoder to format each subsequent encoded value.
func (e *Encoder) SetIndent(prefix, indent string) {
	e.encoder.SetIndent(prefix, indent)
}

type (
	Delim      = gojson.Delim
	Number     = gojson.Number
	RawMessage = gojson.RawMessage
)
