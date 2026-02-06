package mlog

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Field is an alias for zap.Field - no wrapper overhead
type Field = zap.Field

// Basic type field constructors - thin wrappers around zap functions

// String types
func String(key string, val string) Field         { return zap.String(key, val) }
func Stringp(key string, val *string) Field       { return zap.Stringp(key, val) }
func Strings(key string, val []string) Field      { return zap.Strings(key, val) }
func ByteString(key string, val []byte) Field     { return zap.ByteString(key, val) }
func ByteStrings(key string, val [][]byte) Field  { return zap.ByteStrings(key, val) }
func Stringer(key string, val fmt.Stringer) Field { return zap.Stringer(key, val) }

// Bool types
func Bool(key string, val bool) Field    { return zap.Bool(key, val) }
func Boolp(key string, val *bool) Field  { return zap.Boolp(key, val) }
func Bools(key string, val []bool) Field { return zap.Bools(key, val) }

// Int types
func Int(key string, val int) Field        { return zap.Int(key, val) }
func Intp(key string, val *int) Field      { return zap.Intp(key, val) }
func Ints(key string, val []int) Field     { return zap.Ints(key, val) }
func Int8(key string, val int8) Field      { return zap.Int8(key, val) }
func Int8p(key string, val *int8) Field    { return zap.Int8p(key, val) }
func Int8s(key string, val []int8) Field   { return zap.Int8s(key, val) }
func Int16(key string, val int16) Field    { return zap.Int16(key, val) }
func Int16p(key string, val *int16) Field  { return zap.Int16p(key, val) }
func Int16s(key string, val []int16) Field { return zap.Int16s(key, val) }
func Int32(key string, val int32) Field    { return zap.Int32(key, val) }
func Int32p(key string, val *int32) Field  { return zap.Int32p(key, val) }
func Int32s(key string, val []int32) Field { return zap.Int32s(key, val) }
func Int64(key string, val int64) Field    { return zap.Int64(key, val) }
func Int64p(key string, val *int64) Field  { return zap.Int64p(key, val) }
func Int64s(key string, val []int64) Field { return zap.Int64s(key, val) }

// Uint types
func Uint(key string, val uint) Field          { return zap.Uint(key, val) }
func Uintp(key string, val *uint) Field        { return zap.Uintp(key, val) }
func Uints(key string, val []uint) Field       { return zap.Uints(key, val) }
func Uint8(key string, val uint8) Field        { return zap.Uint8(key, val) }
func Uint8p(key string, val *uint8) Field      { return zap.Uint8p(key, val) }
func Uint8s(key string, val []uint8) Field     { return zap.Uint8s(key, val) }
func Uint16(key string, val uint16) Field      { return zap.Uint16(key, val) }
func Uint16p(key string, val *uint16) Field    { return zap.Uint16p(key, val) }
func Uint16s(key string, val []uint16) Field   { return zap.Uint16s(key, val) }
func Uint32(key string, val uint32) Field      { return zap.Uint32(key, val) }
func Uint32p(key string, val *uint32) Field    { return zap.Uint32p(key, val) }
func Uint32s(key string, val []uint32) Field   { return zap.Uint32s(key, val) }
func Uint64(key string, val uint64) Field      { return zap.Uint64(key, val) }
func Uint64p(key string, val *uint64) Field    { return zap.Uint64p(key, val) }
func Uint64s(key string, val []uint64) Field   { return zap.Uint64s(key, val) }
func Uintptr(key string, val uintptr) Field    { return zap.Uintptr(key, val) }
func Uintptrp(key string, val *uintptr) Field  { return zap.Uintptrp(key, val) }
func Uintptrs(key string, val []uintptr) Field { return zap.Uintptrs(key, val) }

// Float types
func Float32(key string, val float32) Field    { return zap.Float32(key, val) }
func Float32p(key string, val *float32) Field  { return zap.Float32p(key, val) }
func Float32s(key string, val []float32) Field { return zap.Float32s(key, val) }
func Float64(key string, val float64) Field    { return zap.Float64(key, val) }
func Float64p(key string, val *float64) Field  { return zap.Float64p(key, val) }
func Float64s(key string, val []float64) Field { return zap.Float64s(key, val) }

// Complex types
func Complex64(key string, val complex64) Field      { return zap.Complex64(key, val) }
func Complex64p(key string, val *complex64) Field    { return zap.Complex64p(key, val) }
func Complex64s(key string, val []complex64) Field   { return zap.Complex64s(key, val) }
func Complex128(key string, val complex128) Field    { return zap.Complex128(key, val) }
func Complex128p(key string, val *complex128) Field  { return zap.Complex128p(key, val) }
func Complex128s(key string, val []complex128) Field { return zap.Complex128s(key, val) }

// Time types
func Time(key string, val time.Time) Field            { return zap.Time(key, val) }
func Timep(key string, val *time.Time) Field          { return zap.Timep(key, val) }
func Times(key string, val []time.Time) Field         { return zap.Times(key, val) }
func Duration(key string, val time.Duration) Field    { return zap.Duration(key, val) }
func Durationp(key string, val *time.Duration) Field  { return zap.Durationp(key, val) }
func Durations(key string, val []time.Duration) Field { return zap.Durations(key, val) }

// Error types
func Err(err error) Field                    { return zap.Error(err) }
func NamedError(key string, err error) Field { return zap.NamedError(key, err) }
func Errors(key string, errs []error) Field  { return zap.Errors(key, errs) }

// Special types
func Any(key string, val any) Field       { return zap.Any(key, val) }
func Binary(key string, val []byte) Field { return zap.Binary(key, val) }
func Reflect(key string, val any) Field   { return zap.Reflect(key, val) }

// Structured types
func Object(key string, val zapcore.ObjectMarshaler) Field { return zap.Object(key, val) }
func Array(key string, val zapcore.ArrayMarshaler) Field   { return zap.Array(key, val) }
func Inline(val zapcore.ObjectMarshaler) Field             { return zap.Inline(val) }
func Namespace(key string) Field                           { return zap.Namespace(key) }

// Stack and skip
func Stack(key string) Field               { return zap.Stack(key) }
func StackSkip(key string, skip int) Field { return zap.StackSkip(key, skip) }
func Skip() Field                          { return zap.Skip() }

// propagatedString is an internal type for string fields that should be propagated via RPC.
// It implements zapcore.ObjectMarshaler to output the value when logged.
type propagatedString struct {
	val string
}

// MarshalLogObject implements zapcore.ObjectMarshaler.
func (p propagatedString) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("value", p.val)
	return nil
}

// propagatedInt64 is an internal type for int64 fields that should be propagated via RPC.
// It implements zapcore.ObjectMarshaler to output the value when logged.
type propagatedInt64 struct {
	val int64
}

// MarshalLogObject implements zapcore.ObjectMarshaler.
func (p propagatedInt64) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddInt64("value", p.val)
	return nil
}

// PropagatedString creates a string field that will be propagated via RPC.
// The field is logged and transmitted in gRPC metadata.
// The key is automatically converted to lowercase for gRPC metadata compatibility.
func PropagatedString(key string, val string) Field {
	return Field{
		Key:       strings.ToLower(key),
		Type:      zapcore.ObjectMarshalerType,
		Interface: propagatedString{val: val},
	}
}

// PropagatedInt64 creates an int64 field that will be propagated via RPC.
// The value is converted to string for transmission.
// The key is automatically converted to lowercase for gRPC metadata compatibility.
func PropagatedInt64(key string, val int64) Field {
	return Field{
		Key:       strings.ToLower(key),
		Type:      zapcore.ObjectMarshalerType,
		Interface: propagatedInt64{val: val},
	}
}

// isPropagatedField checks if a field is marked for RPC propagation.
func isPropagatedField(f *Field) bool {
	switch f.Interface.(type) {
	case propagatedString, propagatedInt64:
		return true
	default:
		return false
	}
}

// getPropagatedValue extracts the string value from a propagated field.
func getPropagatedValue(f *Field) string {
	switch v := f.Interface.(type) {
	case propagatedString:
		return v.val
	case propagatedInt64:
		return strconv.FormatInt(v.val, 10)
	default:
		return ""
	}
}
