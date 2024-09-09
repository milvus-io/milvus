// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright (c) 2016 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package log

import (
	"encoding/base64"
	"fmt"
	"math"
	"sync"
	"time"
	"unicode/utf8"

	jsoniter "github.com/json-iterator/go"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

// DefaultTimeEncoder serializes time.Time to a human-readable formatted string
func DefaultTimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	s := t.Format("2006/01/02 15:04:05.000 -07:00")
	if e, ok := enc.(*textEncoder); ok {
		for _, c := range []byte(s) {
			e.buf.AppendByte(c)
		}
		return
	}
	enc.AppendString(s)
}

// ShortCallerEncoder serializes a caller in file:line format.
func ShortCallerEncoder(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(caller.TrimmedPath())
}

// For JSON-escaping; see textEncoder.safeAddString below.
const _hex = "0123456789abcdef"

var _textPool = sync.Pool{New: func() interface{} {
	return &textEncoder{}
}}

var (
	_pool = buffer.NewPool()
	// Get retrieves a buffer from the pool, creating one if necessary.
	Get = _pool.Get
)

func getTextEncoder() *textEncoder {
	return _textPool.Get().(*textEncoder)
}

func putTextEncoder(enc *textEncoder) {
	if enc.reflectBuf != nil {
		enc.reflectBuf.Free()
	}
	enc.EncoderConfig = nil
	enc.buf = nil
	enc.spaced = false
	enc.openNamespaces = 0
	enc.reflectBuf = nil
	enc.reflectEnc = nil
	_textPool.Put(enc)
}

type textEncoder struct {
	*zapcore.EncoderConfig
	buf                 *buffer.Buffer
	spaced              bool // include spaces after colons and commas
	openNamespaces      int
	disableErrorVerbose bool

	// for encoding generic values by reflection
	reflectBuf *buffer.Buffer
	reflectEnc *jsoniter.Encoder
}

func NewTextEncoder(encoderConfig *zapcore.EncoderConfig, spaced bool, disableErrorVerbose bool) zapcore.Encoder {
	return &textEncoder{
		EncoderConfig:       encoderConfig,
		buf:                 _pool.Get(),
		spaced:              spaced,
		disableErrorVerbose: disableErrorVerbose,
	}
}

// NewTextEncoderByConfig creates a fast, low-allocation Text encoder with config. The encoder
// appropriately escapes all field keys and values.
func NewTextEncoderByConfig(cfg *Config) zapcore.Encoder {
	cc := zapcore.EncoderConfig{
		// Keys can be anything except the empty string.
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "name",
		CallerKey:      "caller",
		MessageKey:     "message",
		StacktraceKey:  "stack",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     DefaultTimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   ShortCallerEncoder,
	}
	if cfg.DisableTimestamp {
		cc.TimeKey = ""
	}
	switch cfg.Format {
	case "text", "":
		return &textEncoder{
			EncoderConfig:       &cc,
			buf:                 _pool.Get(),
			spaced:              false,
			disableErrorVerbose: cfg.DisableErrorVerbose,
		}
	case "json":
		return zapcore.NewJSONEncoder(cc)
	default:
		panic(fmt.Sprintf("unsupport log format: %s", cfg.Format))
	}
}

func (enc *textEncoder) AddArray(key string, arr zapcore.ArrayMarshaler) error {
	enc.addKey(key)
	return enc.AppendArray(arr)
}

func (enc *textEncoder) AddObject(key string, obj zapcore.ObjectMarshaler) error {
	enc.addKey(key)
	return enc.AppendObject(obj)
}

func (enc *textEncoder) AddBinary(key string, val []byte) {
	enc.AddString(key, base64.StdEncoding.EncodeToString(val))
}

func (enc *textEncoder) AddByteString(key string, val []byte) {
	enc.addKey(key)
	enc.AppendByteString(val)
}

func (enc *textEncoder) AddBool(key string, val bool) {
	enc.addKey(key)
	enc.AppendBool(val)
}

func (enc *textEncoder) AddComplex128(key string, val complex128) {
	enc.addKey(key)
	enc.AppendComplex128(val)
}

func (enc *textEncoder) AddDuration(key string, val time.Duration) {
	enc.addKey(key)
	enc.AppendDuration(val)
}

func (enc *textEncoder) AddFloat64(key string, val float64) {
	enc.addKey(key)
	enc.AppendFloat64(val)
}

func (enc *textEncoder) AddInt64(key string, val int64) {
	enc.addKey(key)
	enc.AppendInt64(val)
}

func (enc *textEncoder) resetReflectBuf() {
	if enc.reflectBuf == nil {
		enc.reflectBuf = _pool.Get()
		enc.reflectEnc = jsoniter.NewEncoder(enc.reflectBuf)
	} else {
		enc.reflectBuf.Reset()
	}
}

func (enc *textEncoder) AddReflected(key string, obj interface{}) error {
	enc.resetReflectBuf()
	err := enc.reflectEnc.Encode(obj)
	if err != nil {
		return err
	}
	enc.reflectBuf.TrimNewline()
	enc.addKey(key)
	enc.AppendByteString(enc.reflectBuf.Bytes())
	return nil
}

func (enc *textEncoder) OpenNamespace(key string) {
	enc.addKey(key)
	enc.buf.AppendByte('{')
	enc.openNamespaces++
}

func (enc *textEncoder) AddString(key, val string) {
	enc.addKey(key)
	enc.AppendString(val)
}

func (enc *textEncoder) AddTime(key string, val time.Time) {
	enc.addKey(key)
	enc.AppendTime(val)
}

func (enc *textEncoder) AddUint64(key string, val uint64) {
	enc.addKey(key)
	enc.AppendUint64(val)
}

func (enc *textEncoder) AppendArray(arr zapcore.ArrayMarshaler) error {
	enc.addElementSeparator()
	ne := enc.cloned()
	ne.buf.AppendByte('[')
	err := arr.MarshalLogArray(ne)
	ne.buf.AppendByte(']')
	enc.AppendByteString(ne.buf.Bytes())
	ne.buf.Free()
	putTextEncoder(ne)
	return err
}

func (enc *textEncoder) AppendObject(obj zapcore.ObjectMarshaler) error {
	enc.addElementSeparator()
	ne := enc.cloned()
	ne.buf.AppendByte('{')
	err := obj.MarshalLogObject(ne)
	ne.buf.AppendByte('}')
	enc.AppendByteString(ne.buf.Bytes())
	ne.buf.Free()
	putTextEncoder(ne)
	return err
}

func (enc *textEncoder) AppendBool(val bool) {
	enc.addElementSeparator()
	enc.buf.AppendBool(val)
}

func (enc *textEncoder) AppendByteString(val []byte) {
	enc.addElementSeparator()
	if !enc.needDoubleQuotes(string(val)) {
		enc.safeAddByteString(val)
		return
	}
	enc.buf.AppendByte('"')
	enc.safeAddByteString(val)
	enc.buf.AppendByte('"')
}

func (enc *textEncoder) AppendComplex128(val complex128) {
	enc.addElementSeparator()
	// Cast to a platform-independent, fixed-size type.
	r, i := real(val), imag(val)
	enc.buf.AppendFloat(r, 64)
	enc.buf.AppendByte('+')
	enc.buf.AppendFloat(i, 64)
	enc.buf.AppendByte('i')
}

func (enc *textEncoder) AppendDuration(val time.Duration) {
	cur := enc.buf.Len()
	enc.EncodeDuration(val, enc)
	if cur == enc.buf.Len() {
		// User-supplied EncodeDuration is a no-op. Fall back to nanoseconds to keep
		// JSON valid.
		enc.AppendInt64(int64(val))
	}
}

func (enc *textEncoder) AppendInt64(val int64) {
	enc.addElementSeparator()
	enc.buf.AppendInt(val)
}

func (enc *textEncoder) AppendReflected(val interface{}) error {
	enc.resetReflectBuf()
	err := enc.reflectEnc.Encode(val)
	if err != nil {
		return err
	}
	enc.reflectBuf.TrimNewline()
	enc.AppendByteString(enc.reflectBuf.Bytes())
	return nil
}

func (enc *textEncoder) AppendString(val string) {
	enc.addElementSeparator()
	enc.safeAddStringWithQuote(val)
}

func (enc *textEncoder) AppendTime(val time.Time) {
	cur := enc.buf.Len()
	enc.EncodeTime(val, enc)
	if cur == enc.buf.Len() {
		// User-supplied EncodeTime is a no-op. Fall back to nanos since epoch to keep
		// output JSON valid.
		enc.AppendInt64(val.UnixNano())
	}
}

func (enc *textEncoder) beginQuoteFiled() {
	if enc.buf.Len() > 0 {
		enc.buf.AppendByte(' ')
	}
	enc.buf.AppendByte('[')
}

func (enc *textEncoder) endQuoteFiled() {
	enc.buf.AppendByte(']')
}

func (enc *textEncoder) AppendUint64(val uint64) {
	enc.addElementSeparator()
	enc.buf.AppendUint(val)
}

func (enc *textEncoder) AddComplex64(k string, v complex64) { enc.AddComplex128(k, complex128(v)) }

func (enc *textEncoder) AddFloat32(k string, v float32) { enc.AddFloat64(k, float64(v)) }

func (enc *textEncoder) AddInt(k string, v int) { enc.AddInt64(k, int64(v)) }

func (enc *textEncoder) AddInt32(k string, v int32) { enc.AddInt64(k, int64(v)) }

func (enc *textEncoder) AddInt16(k string, v int16) { enc.AddInt64(k, int64(v)) }

func (enc *textEncoder) AddInt8(k string, v int8) { enc.AddInt64(k, int64(v)) }

func (enc *textEncoder) AddUint(k string, v uint) { enc.AddUint64(k, uint64(v)) }

func (enc *textEncoder) AddUint32(k string, v uint32) { enc.AddUint64(k, uint64(v)) }

func (enc *textEncoder) AddUint16(k string, v uint16) { enc.AddUint64(k, uint64(v)) }

func (enc *textEncoder) AddUint8(k string, v uint8) { enc.AddUint64(k, uint64(v)) }

func (enc *textEncoder) AddUintptr(k string, v uintptr) { enc.AddUint64(k, uint64(v)) }

func (enc *textEncoder) AppendComplex64(v complex64) { enc.AppendComplex128(complex128(v)) }

func (enc *textEncoder) AppendFloat64(v float64) { enc.appendFloat(v, 64) }

func (enc *textEncoder) AppendFloat32(v float32) { enc.appendFloat(float64(v), 32) }

func (enc *textEncoder) AppendInt(v int) { enc.AppendInt64(int64(v)) }

func (enc *textEncoder) AppendInt32(v int32) { enc.AppendInt64(int64(v)) }

func (enc *textEncoder) AppendInt16(v int16) { enc.AppendInt64(int64(v)) }

func (enc *textEncoder) AppendInt8(v int8) { enc.AppendInt64(int64(v)) }

func (enc *textEncoder) AppendUint(v uint) { enc.AppendUint64(uint64(v)) }

func (enc *textEncoder) AppendUint32(v uint32) { enc.AppendUint64(uint64(v)) }

func (enc *textEncoder) AppendUint16(v uint16) { enc.AppendUint64(uint64(v)) }

func (enc *textEncoder) AppendUint8(v uint8) { enc.AppendUint64(uint64(v)) }

func (enc *textEncoder) AppendUintptr(v uintptr) { enc.AppendUint64(uint64(v)) }

func (enc *textEncoder) Clone() zapcore.Encoder {
	clone := enc.cloned()
	clone.buf.Write(enc.buf.Bytes())
	return clone
}

func (enc *textEncoder) cloned() *textEncoder {
	clone := getTextEncoder()
	clone.EncoderConfig = enc.EncoderConfig
	clone.spaced = enc.spaced
	clone.openNamespaces = enc.openNamespaces
	clone.disableErrorVerbose = enc.disableErrorVerbose
	clone.buf = _pool.Get()
	return clone
}

func (enc *textEncoder) EncodeEntry(ent zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	final := enc.cloned()
	if final.TimeKey != "" {
		final.beginQuoteFiled()
		final.AppendTime(ent.Time)
		final.endQuoteFiled()
	}

	if final.LevelKey != "" {
		final.beginQuoteFiled()
		cur := final.buf.Len()
		final.EncodeLevel(ent.Level, final)
		if cur == final.buf.Len() {
			// User-supplied EncodeLevel was a no-op. Fall back to strings to keep
			// output JSON valid.
			final.AppendString(ent.Level.String())
		}
		final.endQuoteFiled()
	}

	if ent.LoggerName != "" && final.NameKey != "" {
		final.beginQuoteFiled()
		cur := final.buf.Len()
		nameEncoder := final.EncodeName

		// if no name encoder provided, fall back to FullNameEncoder for backwards
		// compatibility
		if nameEncoder == nil {
			nameEncoder = zapcore.FullNameEncoder
		}

		nameEncoder(ent.LoggerName, final)
		if cur == final.buf.Len() {
			// User-supplied EncodeName was a no-op. Fall back to strings to
			// keep output JSON valid.
			final.AppendString(ent.LoggerName)
		}
		final.endQuoteFiled()
	}
	if ent.Caller.Defined && final.CallerKey != "" {
		final.beginQuoteFiled()
		cur := final.buf.Len()
		final.EncodeCaller(ent.Caller, final)
		if cur == final.buf.Len() {
			// User-supplied EncodeCaller was a no-op. Fall back to strings to
			// keep output JSON valid.
			final.AppendString(ent.Caller.String())
		}
		final.endQuoteFiled()
	}
	// add Message
	if len(ent.Message) > 0 {
		final.beginQuoteFiled()
		final.AppendString(ent.Message)
		final.endQuoteFiled()
	}
	if enc.buf.Len() > 0 {
		final.buf.AppendByte(' ')
		final.buf.Write(enc.buf.Bytes())
	}
	final.addFields(fields)
	final.closeOpenNamespaces()
	if ent.Stack != "" && final.StacktraceKey != "" {
		final.beginQuoteFiled()
		final.AddString(final.StacktraceKey, ent.Stack)
		final.endQuoteFiled()
	}

	if final.LineEnding != "" {
		final.buf.AppendString(final.LineEnding)
	} else {
		final.buf.AppendString(zapcore.DefaultLineEnding)
	}

	ret := final.buf
	putTextEncoder(final)
	return ret, nil
}

func (enc *textEncoder) truncate() {
	enc.buf.Reset()
}

func (enc *textEncoder) closeOpenNamespaces() {
	for i := 0; i < enc.openNamespaces; i++ {
		enc.buf.AppendByte('}')
	}
}

func (enc *textEncoder) addKey(key string) {
	enc.addElementSeparator()
	enc.safeAddStringWithQuote(key)
	enc.buf.AppendByte('=')
}

func (enc *textEncoder) addElementSeparator() {
	last := enc.buf.Len() - 1
	if last < 0 {
		return
	}
	switch enc.buf.Bytes()[last] {
	case '{', '[', ':', ',', ' ', '=':
		return
	default:
		enc.buf.AppendByte(',')
	}
}

func (enc *textEncoder) appendFloat(val float64, bitSize int) {
	enc.addElementSeparator()
	switch {
	case math.IsNaN(val):
		enc.buf.AppendString("NaN")
	case math.IsInf(val, 1):
		enc.buf.AppendString("+Inf")
	case math.IsInf(val, -1):
		enc.buf.AppendString("-Inf")
	default:
		enc.buf.AppendFloat(val, bitSize)
	}
}

// safeAddString JSON-escapes a string and appends it to the internal buffer.
// Unlike the standard library's encoder, it doesn't attempt to protect the
// user from browser vulnerabilities or JSONP-related problems.
func (enc *textEncoder) safeAddString(s string) {
	for i := 0; i < len(s); {
		if enc.tryAddRuneSelf(s[i]) {
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if enc.tryAddRuneError(r, size) {
			i++
			continue
		}
		enc.buf.AppendString(s[i : i+size])
		i += size
	}
}

// safeAddStringWithQuote will automatically add quotoes.
func (enc *textEncoder) safeAddStringWithQuote(s string) {
	if !enc.needDoubleQuotes(s) {
		enc.safeAddString(s)
		return
	}
	enc.buf.AppendByte('"')
	enc.safeAddString(s)
	enc.buf.AppendByte('"')
}

// safeAddByteString is no-alloc equivalent of safeAddString(string(s)) for s []byte.
func (enc *textEncoder) safeAddByteString(s []byte) {
	for i := 0; i < len(s); {
		if enc.tryAddRuneSelf(s[i]) {
			i++
			continue
		}
		r, size := utf8.DecodeRune(s[i:])
		if enc.tryAddRuneError(r, size) {
			i++
			continue
		}
		enc.buf.Write(s[i : i+size])
		i += size
	}
}

// See [log-fileds](https://github.com/tikv/rfcs/blob/master/text/2018-12-19-unified-log-format.md#log-fields-section).
func (enc *textEncoder) needDoubleQuotes(s string) bool {
	for i := 0; i < len(s); {
		b := s[i]
		if b <= 0x20 {
			return true
		}
		switch b {
		case '\\', '"', '[', ']', '=':
			return true
		}
		i++
	}
	return false
}

// tryAddRuneSelf appends b if it is valid UTF-8 character represented in a single byte.
func (enc *textEncoder) tryAddRuneSelf(b byte) bool {
	if b >= utf8.RuneSelf {
		return false
	}
	if 0x20 <= b && b != '\\' && b != '"' {
		enc.buf.AppendByte(b)
		return true
	}
	switch b {
	case '\\', '"':
		enc.buf.AppendByte('\\')
		enc.buf.AppendByte(b)
	case '\n':
		enc.buf.AppendByte('\\')
		enc.buf.AppendByte('n')
	case '\r':
		enc.buf.AppendByte('\\')
		enc.buf.AppendByte('r')
	case '\t':
		enc.buf.AppendByte('\\')
		enc.buf.AppendByte('t')

	default:
		// Encode bytes < 0x20, except for the escape sequences above.
		enc.buf.AppendString(`\u00`)
		enc.buf.AppendByte(_hex[b>>4])
		enc.buf.AppendByte(_hex[b&0xF])
	}
	return true
}

func (enc *textEncoder) tryAddRuneError(r rune, size int) bool {
	if r == utf8.RuneError && size == 1 {
		enc.buf.AppendString(`\ufffd`)
		return true
	}
	return false
}

func (enc *textEncoder) addFields(fields []zapcore.Field) {
	for _, f := range fields {
		if f.Type == zapcore.ErrorType {
			// handle ErrorType in pingcap/log to fix "[key=?,keyVerbose=?]" problem.
			// see more detail at https://github.com/pingcap/log/pull/5
			enc.encodeError(f)
			continue
		}
		enc.beginQuoteFiled()
		f.AddTo(enc)
		enc.endQuoteFiled()
	}
}

func (enc *textEncoder) encodeError(f zapcore.Field) {
	err := f.Interface.(error)
	basic := err.Error()
	enc.beginQuoteFiled()
	enc.AddString(f.Key, basic)
	enc.endQuoteFiled()
	if enc.disableErrorVerbose {
		return
	}
	if e, isFormatter := err.(fmt.Formatter); isFormatter {
		verbose := fmt.Sprintf("%+v", e)
		if verbose != basic {
			// This is a rich error type, like those produced by
			// errors.
			enc.beginQuoteFiled()
			enc.AddString(f.Key+"Verbose", verbose)
			enc.endQuoteFiled()
		}
	}
}
