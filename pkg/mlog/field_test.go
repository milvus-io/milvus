//go:build test

package mlog

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Test all String type field constructors
func TestStringFields(t *testing.T) {
	// String
	f := String("key", "value")
	assert.Equal(t, zap.String("key", "value"), f)

	// Stringp
	s := "value"
	f = Stringp("key", &s)
	assert.Equal(t, zap.Stringp("key", &s), f)

	// Strings
	ss := []string{"a", "b", "c"}
	f = Strings("key", ss)
	assert.Equal(t, zap.Strings("key", ss), f)

	// ByteString
	bs := []byte("bytes")
	f = ByteString("key", bs)
	assert.Equal(t, zap.ByteString("key", bs), f)

	// ByteStrings
	bss := [][]byte{[]byte("a"), []byte("b")}
	f = ByteStrings("key", bss)
	assert.Equal(t, zap.ByteStrings("key", bss), f)
}

// Test all Bool type field constructors
func TestBoolFields(t *testing.T) {
	// Bool
	f := Bool("key", true)
	assert.Equal(t, zap.Bool("key", true), f)

	// Boolp
	b := true
	f = Boolp("key", &b)
	assert.Equal(t, zap.Boolp("key", &b), f)

	// Bools
	bs := []bool{true, false, true}
	f = Bools("key", bs)
	assert.Equal(t, zap.Bools("key", bs), f)
}

// Test all Int type field constructors
func TestIntFields(t *testing.T) {
	// Int
	f := Int("key", 42)
	assert.Equal(t, zap.Int("key", 42), f)

	// Intp
	i := 42
	f = Intp("key", &i)
	assert.Equal(t, zap.Intp("key", &i), f)

	// Ints
	is := []int{1, 2, 3}
	f = Ints("key", is)
	assert.Equal(t, zap.Ints("key", is), f)

	// Int8
	f = Int8("key", 8)
	assert.Equal(t, zap.Int8("key", 8), f)

	// Int8p
	i8 := int8(8)
	f = Int8p("key", &i8)
	assert.Equal(t, zap.Int8p("key", &i8), f)

	// Int8s
	i8s := []int8{1, 2, 3}
	f = Int8s("key", i8s)
	assert.Equal(t, zap.Int8s("key", i8s), f)

	// Int16
	f = Int16("key", 16)
	assert.Equal(t, zap.Int16("key", 16), f)

	// Int16p
	i16 := int16(16)
	f = Int16p("key", &i16)
	assert.Equal(t, zap.Int16p("key", &i16), f)

	// Int16s
	i16s := []int16{1, 2, 3}
	f = Int16s("key", i16s)
	assert.Equal(t, zap.Int16s("key", i16s), f)

	// Int32
	f = Int32("key", 32)
	assert.Equal(t, zap.Int32("key", 32), f)

	// Int32p
	i32 := int32(32)
	f = Int32p("key", &i32)
	assert.Equal(t, zap.Int32p("key", &i32), f)

	// Int32s
	i32s := []int32{1, 2, 3}
	f = Int32s("key", i32s)
	assert.Equal(t, zap.Int32s("key", i32s), f)

	// Int64
	f = Int64("key", 64)
	assert.Equal(t, zap.Int64("key", 64), f)

	// Int64p
	i64 := int64(64)
	f = Int64p("key", &i64)
	assert.Equal(t, zap.Int64p("key", &i64), f)

	// Int64s
	i64s := []int64{1, 2, 3}
	f = Int64s("key", i64s)
	assert.Equal(t, zap.Int64s("key", i64s), f)
}

// Test all Uint type field constructors
func TestUintFields(t *testing.T) {
	// Uint
	f := Uint("key", 42)
	assert.Equal(t, zap.Uint("key", 42), f)

	// Uintp
	u := uint(42)
	f = Uintp("key", &u)
	assert.Equal(t, zap.Uintp("key", &u), f)

	// Uints
	us := []uint{1, 2, 3}
	f = Uints("key", us)
	assert.Equal(t, zap.Uints("key", us), f)

	// Uint8
	f = Uint8("key", 8)
	assert.Equal(t, zap.Uint8("key", 8), f)

	// Uint8p
	u8 := uint8(8)
	f = Uint8p("key", &u8)
	assert.Equal(t, zap.Uint8p("key", &u8), f)

	// Uint8s
	u8s := []uint8{1, 2, 3}
	f = Uint8s("key", u8s)
	assert.Equal(t, zap.Uint8s("key", u8s), f)

	// Uint16
	f = Uint16("key", 16)
	assert.Equal(t, zap.Uint16("key", 16), f)

	// Uint16p
	u16 := uint16(16)
	f = Uint16p("key", &u16)
	assert.Equal(t, zap.Uint16p("key", &u16), f)

	// Uint16s
	u16s := []uint16{1, 2, 3}
	f = Uint16s("key", u16s)
	assert.Equal(t, zap.Uint16s("key", u16s), f)

	// Uint32
	f = Uint32("key", 32)
	assert.Equal(t, zap.Uint32("key", 32), f)

	// Uint32p
	u32 := uint32(32)
	f = Uint32p("key", &u32)
	assert.Equal(t, zap.Uint32p("key", &u32), f)

	// Uint32s
	u32s := []uint32{1, 2, 3}
	f = Uint32s("key", u32s)
	assert.Equal(t, zap.Uint32s("key", u32s), f)

	// Uint64
	f = Uint64("key", 64)
	assert.Equal(t, zap.Uint64("key", 64), f)

	// Uint64p
	u64 := uint64(64)
	f = Uint64p("key", &u64)
	assert.Equal(t, zap.Uint64p("key", &u64), f)

	// Uint64s
	u64s := []uint64{1, 2, 3}
	f = Uint64s("key", u64s)
	assert.Equal(t, zap.Uint64s("key", u64s), f)

	// Uintptr
	f = Uintptr("key", 123)
	assert.Equal(t, zap.Uintptr("key", 123), f)

	// Uintptrp
	up := uintptr(123)
	f = Uintptrp("key", &up)
	assert.Equal(t, zap.Uintptrp("key", &up), f)

	// Uintptrs
	ups := []uintptr{1, 2, 3}
	f = Uintptrs("key", ups)
	assert.Equal(t, zap.Uintptrs("key", ups), f)
}

// Test all Float type field constructors
func TestFloatFields(t *testing.T) {
	// Float32
	f := Float32("key", 3.14)
	assert.Equal(t, zap.Float32("key", 3.14), f)

	// Float32p
	f32 := float32(3.14)
	f = Float32p("key", &f32)
	assert.Equal(t, zap.Float32p("key", &f32), f)

	// Float32s
	f32s := []float32{1.1, 2.2, 3.3}
	f = Float32s("key", f32s)
	assert.Equal(t, zap.Float32s("key", f32s), f)

	// Float64
	f = Float64("key", 3.14159)
	assert.Equal(t, zap.Float64("key", 3.14159), f)

	// Float64p
	f64 := 3.14159
	f = Float64p("key", &f64)
	assert.Equal(t, zap.Float64p("key", &f64), f)

	// Float64s
	f64s := []float64{1.1, 2.2, 3.3}
	f = Float64s("key", f64s)
	assert.Equal(t, zap.Float64s("key", f64s), f)
}

// Test all Complex type field constructors
func TestComplexFields(t *testing.T) {
	// Complex64
	f := Complex64("key", 1+2i)
	assert.Equal(t, zap.Complex64("key", 1+2i), f)

	// Complex64p
	c64 := complex64(1 + 2i)
	f = Complex64p("key", &c64)
	assert.Equal(t, zap.Complex64p("key", &c64), f)

	// Complex64s
	c64s := []complex64{1 + 2i, 3 + 4i}
	f = Complex64s("key", c64s)
	assert.Equal(t, zap.Complex64s("key", c64s), f)

	// Complex128
	f = Complex128("key", 1+2i)
	assert.Equal(t, zap.Complex128("key", 1+2i), f)

	// Complex128p
	c128 := complex128(1 + 2i)
	f = Complex128p("key", &c128)
	assert.Equal(t, zap.Complex128p("key", &c128), f)

	// Complex128s
	c128s := []complex128{1 + 2i, 3 + 4i}
	f = Complex128s("key", c128s)
	assert.Equal(t, zap.Complex128s("key", c128s), f)
}

// Test all Time type field constructors
func TestTimeFields(t *testing.T) {
	now := time.Now()

	// Time
	f := Time("key", now)
	assert.Equal(t, zap.Time("key", now), f)

	// Timep
	f = Timep("key", &now)
	assert.Equal(t, zap.Timep("key", &now), f)

	// Times
	ts := []time.Time{now, now.Add(time.Hour)}
	f = Times("key", ts)
	assert.Equal(t, zap.Times("key", ts), f)

	// Duration
	d := 5 * time.Second
	f = Duration("key", d)
	assert.Equal(t, zap.Duration("key", d), f)

	// Durationp
	f = Durationp("key", &d)
	assert.Equal(t, zap.Durationp("key", &d), f)

	// Durations
	ds := []time.Duration{time.Second, time.Minute}
	f = Durations("key", ds)
	assert.Equal(t, zap.Durations("key", ds), f)
}

// Test all Error type field constructors
func TestErrorFields(t *testing.T) {
	err := errors.New("test error")

	// Err
	f := Err(err)
	assert.Equal(t, zap.Error(err), f)

	// NamedError
	f = NamedError("custom_error", err)
	assert.Equal(t, zap.NamedError("custom_error", err), f)

	// Errors
	errs := []error{err, errors.New("another error")}
	f = Errors("errors", errs)
	assert.Equal(t, zap.Errors("errors", errs), f)
}

// Test special type field constructors
func TestSpecialFields(t *testing.T) {
	// Any
	val := map[string]int{"a": 1}
	f := Any("key", val)
	assert.Equal(t, zap.Any("key", val), f)

	// Binary
	bin := []byte{0x01, 0x02, 0x03}
	f = Binary("key", bin)
	assert.Equal(t, zap.Binary("key", bin), f)

	// Reflect
	f = Reflect("key", val)
	assert.Equal(t, zap.Reflect("key", val), f)
}

// testObjectMarshaler is a test implementation of zapcore.ObjectMarshaler
type testObjectMarshaler struct {
	value string
}

func (t testObjectMarshaler) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("value", t.value)
	return nil
}

// testArrayMarshaler is a test implementation of zapcore.ArrayMarshaler
type testArrayMarshaler struct {
	values []string
}

func (t testArrayMarshaler) MarshalLogArray(enc zapcore.ArrayEncoder) error {
	for _, v := range t.values {
		enc.AppendString(v)
	}
	return nil
}

// Test structured type field constructors
func TestStructuredFields(t *testing.T) {
	// Object
	obj := testObjectMarshaler{value: "test"}
	f := Object("key", obj)
	assert.Equal(t, zap.Object("key", obj), f)

	// Array
	arr := testArrayMarshaler{values: []string{"a", "b"}}
	f = Array("key", arr)
	assert.Equal(t, zap.Array("key", arr), f)

	// Inline
	f = Inline(obj)
	assert.Equal(t, zap.Inline(obj), f)

	// Namespace
	f = Namespace("ns")
	assert.Equal(t, zap.Namespace("ns"), f)
}

// Test stack and skip field constructors
func TestStackAndSkipFields(t *testing.T) {
	// Stack
	f := Stack("stacktrace")
	assert.Equal(t, "stacktrace", f.Key)

	// StackSkip
	f = StackSkip("stacktrace", 1)
	assert.Equal(t, "stacktrace", f.Key)

	// Skip
	f = Skip()
	assert.Equal(t, zap.Skip(), f)
}

// Test propagatedString MarshalLogObject
func TestPropagatedStringMarshalLogObject(t *testing.T) {
	p := propagatedString{val: "test_value"}

	enc := zapcore.NewMapObjectEncoder()
	err := p.MarshalLogObject(enc)
	assert.NoError(t, err)
	assert.Equal(t, "test_value", enc.Fields["value"])
}

// Test propagatedInt64 MarshalLogObject
func TestPropagatedInt64MarshalLogObject(t *testing.T) {
	p := propagatedInt64{val: 12345}

	enc := zapcore.NewMapObjectEncoder()
	err := p.MarshalLogObject(enc)
	assert.NoError(t, err)
	assert.Equal(t, int64(12345), enc.Fields["value"])
}

type testStringer struct {
	value string
}

func (s testStringer) String() string {
	return s.value
}

func TestStringerField(t *testing.T) {
	s := testStringer{value: "hello"}
	field := Stringer("obj", s)
	expected := zap.Stringer("obj", s)
	assert.Equal(t, expected, field)
}

// Test all FieldXXX helper functions from field_enum.go
func TestFieldHelperFunctions(t *testing.T) {
	// FieldNodeID
	f := FieldNodeID(123)
	assert.Equal(t, KeyNodeID, f.Key)
	assert.Equal(t, int64(123), f.Integer)

	// FieldModule
	f = FieldModule("querynode")
	assert.Equal(t, KeyModule, f.Key)
	assert.Equal(t, "querynode", f.String)

	// FieldTraceID
	f = FieldTraceID("trace-123")
	assert.Equal(t, KeyTraceID, f.Key)
	assert.Equal(t, "trace-123", f.String)

	// FieldSpanID
	f = FieldSpanID("span-456")
	assert.Equal(t, KeySpanID, f.Key)
	assert.Equal(t, "span-456", f.String)

	// FieldDbID
	f = FieldDbID(100)
	assert.Equal(t, KeyDbID, f.Key)
	assert.Equal(t, int64(100), f.Integer)

	// FieldDbName
	f = FieldDbName("default")
	assert.Equal(t, KeyDbName, f.Key)
	assert.Equal(t, "default", f.String)

	// FieldCollectionID
	f = FieldCollectionID(200)
	assert.Equal(t, KeyCollectionID, f.Key)
	assert.Equal(t, int64(200), f.Integer)

	// FieldCollectionName
	f = FieldCollectionName("my_collection")
	assert.Equal(t, KeyCollectionName, f.Key)
	assert.Equal(t, "my_collection", f.String)

	// FieldPartitionID
	f = FieldPartitionID(300)
	assert.Equal(t, KeyPartitionID, f.Key)
	assert.Equal(t, int64(300), f.Integer)

	// FieldPartitionName
	f = FieldPartitionName("partition_0")
	assert.Equal(t, KeyPartitionName, f.Key)
	assert.Equal(t, "partition_0", f.String)

	// FieldSegmentID
	f = FieldSegmentID(400)
	assert.Equal(t, KeySegmentID, f.Key)
	assert.Equal(t, int64(400), f.Integer)

	// FieldVChannel
	f = FieldVChannel("vchan_0")
	assert.Equal(t, KeyVChannel, f.Key)
	assert.Equal(t, "vchan_0", f.String)

	// FieldPChannel
	f = FieldPChannel("pchan_0")
	assert.Equal(t, KeyPChannel, f.Key)
	assert.Equal(t, "pchan_0", f.String)

	// FieldMessageID
	msgID := testObjectMarshaler{value: "msg-123"}
	f = FieldMessageID(msgID)
	assert.Equal(t, KeyMessageID, f.Key)

	// FieldMessage
	msg := testObjectMarshaler{value: "message content"}
	f = FieldMessage(msg)
	assert.Equal(t, KeyMessage, f.Key)
}

func TestWellKnownKeysFormat(t *testing.T) {
	// All well-known keys should be lowercase with underscores for readability
	// and gRPC metadata compatibility
	assert.Equal(t, "node_id", KeyNodeID)
	assert.Equal(t, "module", KeyModule)
	assert.Equal(t, "trace_id", KeyTraceID)
	assert.Equal(t, "span_id", KeySpanID)
	assert.Equal(t, "db_id", KeyDbID)
	assert.Equal(t, "db_name", KeyDbName)
	assert.Equal(t, "collection_id", KeyCollectionID)
	assert.Equal(t, "collection_name", KeyCollectionName)
	assert.Equal(t, "partition_id", KeyPartitionID)
	assert.Equal(t, "partition_name", KeyPartitionName)
	assert.Equal(t, "segment_id", KeySegmentID)
	assert.Equal(t, "vchannel", KeyVChannel)
	assert.Equal(t, "pchannel", KeyPChannel)
	assert.Equal(t, "message_id", KeyMessageID)
	assert.Equal(t, "message", KeyMessage)
}

func TestPropagatedStringConvertsKeyToLowercase(t *testing.T) {
	// Keys with mixed case should be converted to lowercase
	f := PropagatedString("CollectionName", "my_collection")
	assert.Equal(t, "collectionname", f.Key, "key should be lowercase")
	assert.Equal(t, "my_collection", getPropagatedValue(&f))
}

func TestPropagatedInt64ConvertsKeyToLowercase(t *testing.T) {
	// Keys with mixed case should be converted to lowercase
	f := PropagatedInt64("CollectionId", 12345)
	assert.Equal(t, "collectionid", f.Key, "key should be lowercase")
	assert.Equal(t, "12345", getPropagatedValue(&f))
}

func TestPropagatedStringAlreadyLowercaseKey(t *testing.T) {
	// Already lowercase keys should remain unchanged
	f := PropagatedString("collectionname", "my_collection")
	assert.Equal(t, "collectionname", f.Key)
}

func TestPropagatedInt64AlreadyLowercaseKey(t *testing.T) {
	// Already lowercase keys should remain unchanged
	f := PropagatedInt64("collectionid", 12345)
	assert.Equal(t, "collectionid", f.Key)
}
