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

package funcutil

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func Test_CheckGrpcReady(t *testing.T) {
	errChan := make(chan error)

	// test errChan can receive nil after interval
	go CheckGrpcReady(context.TODO(), errChan)

	err := <-errChan
	assert.NoError(t, err)

	// test CheckGrpcReady can finish after context done
	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Millisecond)
	CheckGrpcReady(ctx, errChan)
	cancel()
}

func Test_GetValidLocalIPNoValid(t *testing.T) {
	addrs := make([]net.Addr, 0, 1)
	addrs = append(addrs, &net.IPNet{IP: net.IPv4(127, 1, 1, 1), Mask: net.IPv4Mask(255, 255, 255, 255)})
	ip := GetValidLocalIP(addrs)
	assert.Equal(t, "", ip)
}

func Test_GetValidLocalIPIPv4(t *testing.T) {
	addrs := make([]net.Addr, 0, 1)
	addrs = append(addrs, &net.IPNet{IP: net.IPv4(100, 1, 1, 1), Mask: net.IPv4Mask(255, 255, 255, 255)})
	ip := GetValidLocalIP(addrs)
	assert.Equal(t, "100.1.1.1", ip)
}

func Test_GetValidLocalIPIPv6(t *testing.T) {
	addrs := make([]net.Addr, 0, 1)
	addrs = append(addrs, &net.IPNet{IP: net.IP{8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, Mask: net.IPMask{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}})
	ip := GetValidLocalIP(addrs)
	assert.Equal(t, "[800::]", ip)
}

func Test_GetValidLocalIPIPv4Priority(t *testing.T) {
	addrs := make([]net.Addr, 0, 1)
	addrs = append(addrs, &net.IPNet{IP: net.IP{8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, Mask: net.IPMask{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}})
	addrs = append(addrs, &net.IPNet{IP: net.IPv4(100, 1, 1, 1), Mask: net.IPv4Mask(255, 255, 255, 255)})
	ip := GetValidLocalIP(addrs)
	assert.Equal(t, "100.1.1.1", ip)
}

func Test_GetLocalIP(t *testing.T) {
	ip := GetLocalIP()
	assert.NotNil(t, ip)
	assert.NotZero(t, len(ip))
}

func Test_GetIP(t *testing.T) {
	t.Run("empty_fallback_auto", func(t *testing.T) {
		ip := GetIP("")
		assert.NotNil(t, ip)
		assert.NotZero(t, len(ip))
	})

	t.Run("valid_ip", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ip := GetIP("8.8.8.8")
			assert.Equal(t, "8.8.8.8", ip)
		})
	})

	t.Run("invalid_ip", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ip := GetIP("null")
			assert.Equal(t, "null", ip)
		}, "non ip format, could be hostname or service name")

		assert.Panics(t, func() {
			GetIP("0.0.0.0")
		}, "input is unspecified ip address, panicking")

		assert.Panics(t, func() {
			GetIP("224.0.0.1")
		}, "input is multicast ip address, panicking")
	})
}

func Test_ParseIndexParamsMap(t *testing.T) {
	num := 10
	keys := make([]string, 0)
	values := make([]string, 0)
	params := make(map[string]string)

	for i := 0; i < num; i++ {
		keys = append(keys, "key"+strconv.Itoa(i))
		values = append(values, "value"+strconv.Itoa(i))
		params[keys[i]] = values[i]
	}

	paramsBytes, err := json.Marshal(params)
	assert.Equal(t, err, nil)
	paramsStr := string(paramsBytes)

	parsedParams, err := JSONToMap(paramsStr)
	assert.Equal(t, err, nil)
	assert.Equal(t, parsedParams, params)

	invalidStr := "invalid string"
	_, err = JSONToMap(invalidStr)
	assert.NotEqual(t, err, nil)
}

func Test_ParseBuiltinRolesMap(t *testing.T) {
	t.Run("correct format", func(t *testing.T) {
		builtinRoles := `{"db_admin": {"privileges": [{"object_type": "Global", "object_name": "*", "privilege": "CreateCollection", "db_name": "*"}]}}`
		rolePrivilegesMap, err := JSONToRoleDetails(builtinRoles)
		assert.Nil(t, err)
		for role, privilegesJSON := range rolePrivilegesMap {
			assert.Contains(t, []string{"db_admin", "db_rw", "db_ro"}, role)
			for _, privileges := range privilegesJSON[util.RoleConfigPrivileges] {
				assert.Equal(t, privileges[util.RoleConfigObjectType], "Global")
			}
		}
	})
	t.Run("wrong format", func(t *testing.T) {
		builtinRoles := `{"db_admin": {"privileges": [{"object_type": "Global", "object_name": "*", "privilege": "CreateCollection", "db_name": "*"}]}`
		_, err := JSONToRoleDetails(builtinRoles)
		assert.NotNil(t, err)
	})
}

func TestGetAttrByKeyFromRepeatedKV(t *testing.T) {
	kvs := []*commonpb.KeyValuePair{
		{Key: "Key1", Value: "Value1"},
		{Key: "Key2", Value: "Value2"},
		{Key: "Key3", Value: "Value3"},
	}

	cases := []struct {
		key      string
		kvs      []*commonpb.KeyValuePair
		value    string
		errIsNil bool
	}{
		{"Key1", kvs, "Value1", true},
		{"Key2", kvs, "Value2", true},
		{"Key3", kvs, "Value3", true},
		{"other", kvs, "", false},
	}

	for _, test := range cases {
		value, err := GetAttrByKeyFromRepeatedKV(test.key, test.kvs)
		assert.Equal(t, test.value, value)
		assert.Equal(t, test.errIsNil, err == nil)
	}

	value, err := GetAttrByKeyFromRepeatedKV("key1", nil)
	assert.Equal(t, "", value)
	assert.Error(t, err)
}

func TestGetCollectionIDFromVChannel(t *testing.T) {
	vChannel1 := "06b84fe16780ed1-rootcoord-dm_3_449684528748778322v0"
	collectionID := GetCollectionIDFromVChannel(vChannel1)
	assert.Equal(t, int64(449684528748778322), collectionID)

	invailedVChannel := "06b84fe16780ed1-rootcoord-dm_3_v0"
	collectionID = GetCollectionIDFromVChannel(invailedVChannel)
	assert.Equal(t, int64(-1), collectionID)

	invailedVChannel = "06b84fe16780ed1-rootcoord-dm_3_-1v0"
	collectionID = GetCollectionIDFromVChannel(invailedVChannel)
	assert.Equal(t, int64(-1), collectionID)
}

func TestCheckCtxValid(t *testing.T) {
	bgCtx := context.Background()
	timeout := 20 * time.Millisecond
	deltaTime := 5 * time.Millisecond
	ctx1, cancel1 := context.WithTimeout(bgCtx, timeout)
	defer cancel1()
	assert.True(t, CheckCtxValid(ctx1))
	time.Sleep(timeout + deltaTime)
	assert.False(t, CheckCtxValid(ctx1))

	ctx2, cancel2 := context.WithTimeout(bgCtx, timeout)
	assert.True(t, CheckCtxValid(ctx2))
	cancel2()
	assert.False(t, CheckCtxValid(ctx2))

	futureTime := time.Now().Add(timeout)
	ctx3, cancel3 := context.WithDeadline(bgCtx, futureTime)
	defer cancel3()
	assert.True(t, CheckCtxValid(ctx3))
	time.Sleep(timeout + deltaTime)
	assert.False(t, CheckCtxValid(ctx3))
}

func TestCheckPortAvailable(t *testing.T) {
	num := 10
	for i := 0; i < num; i++ {
		port := GetAvailablePort()
		assert.Equal(t, CheckPortAvailable(port), true)
	}
}

func Test_ToPhysicalChannel(t *testing.T) {
	assert.Equal(t, "abc_", ToPhysicalChannel("abc_"))
	assert.Equal(t, "abc_123", ToPhysicalChannel("abc_123"))
	assert.Equal(t, "abc_defgsg", ToPhysicalChannel("abc_defgsg"))
	assert.Equal(t, "abc_123", ToPhysicalChannel("abc_123_456v0"))
	assert.Equal(t, "abc___defgsg", ToPhysicalChannel("abc___defgsg"))
	assert.Equal(t, "abcdef", ToPhysicalChannel("abcdef"))
	channel := "by-dev-rootcoord-dml_3_449883080965365748v0"
	for i := 0; i < 10; i++ {
		channel = ToPhysicalChannel(channel)
		assert.Equal(t, "by-dev-rootcoord-dml_3", channel)
	}
}

func Test_ConvertChannelName(t *testing.T) {
	const (
		chanName      = "by-dev_rootcoord-dml_123v0"
		deltaChanName = "by-dev_rootcoord-delta_123v0"
		tFrom         = "rootcoord-dml"
		tTo           = "rootcoord-delta"
	)
	_, err := ConvertChannelName("by-dev", tFrom, tTo)
	assert.Error(t, err)
	_, err = ConvertChannelName("by-dev", "", tTo)
	assert.Error(t, err)
	_, err = ConvertChannelName("by-dev_rootcoord-delta_123v0", tFrom, tTo)
	assert.Error(t, err)
	str, err := ConvertChannelName(chanName, tFrom, tTo)
	assert.NoError(t, err)
	assert.Equal(t, deltaChanName, str)
}

func TestGetNumRowsOfScalarField(t *testing.T) {
	cases := []struct {
		datas interface{}
		want  uint64
	}{
		{[]bool{}, 0},
		{[]bool{true, false}, 2},
		{[]int32{}, 0},
		{[]int32{1, 2}, 2},
		{[]int64{}, 0},
		{[]int64{1, 2}, 2},
		{[]float32{}, 0},
		{[]float32{1.0, 2.0}, 2},
		{[]float64{}, 0},
		{[]float64{1.0, 2.0}, 2},
	}

	for _, test := range cases {
		if got := getNumRowsOfScalarField(test.datas); got != test.want {
			t.Errorf("getNumRowsOfScalarField(%v) = %v", test.datas, test.want)
		}
	}
}

func TestGetNumRowsOfFloatVectorField(t *testing.T) {
	cases := []struct {
		fDatas   []float32
		dim      int64
		want     uint64
		errIsNil bool
	}{
		{[]float32{}, -1, 0, false},     // dim <= 0
		{[]float32{}, 0, 0, false},      // dim <= 0
		{[]float32{1.0}, 128, 0, false}, // length % dim != 0
		{[]float32{}, 128, 0, true},
		{[]float32{1.0, 2.0}, 2, 1, true},
		{[]float32{1.0, 2.0, 3.0, 4.0}, 2, 2, true},
	}

	for _, test := range cases {
		got, err := GetNumRowsOfFloatVectorField(test.fDatas, test.dim)
		if test.errIsNil {
			assert.Equal(t, nil, err)
			if got != test.want {
				t.Errorf("GetNumRowsOfFloatVectorField(%v, %v) = %v, %v", test.fDatas, test.dim, test.want, nil)
			}
		} else {
			assert.NotEqual(t, nil, err)
		}
	}
}

func TestGetNumRowsOfFloat16VectorField(t *testing.T) {
	cases := []struct {
		bDatas   []byte
		dim      int64
		want     uint64
		errIsNil bool
	}{
		{[]byte{}, -1, 0, false},     // dim <= 0
		{[]byte{}, 0, 0, false},      // dim <= 0
		{[]byte{1.0}, 128, 0, false}, // length % dim != 0
		{[]byte{}, 128, 0, true},
		{[]byte{1.0, 2.0}, 1, 1, true},
		{[]byte{1.0, 2.0, 3.0, 4.0}, 2, 1, true},
	}

	for _, test := range cases {
		got, err := GetNumRowsOfFloat16VectorField(test.bDatas, test.dim)
		if test.errIsNil {
			assert.Equal(t, nil, err)
			if got != test.want {
				t.Errorf("GetNumRowsOfFloat16VectorField(%v, %v) = %v, %v", test.bDatas, test.dim, test.want, nil)
			}
		} else {
			assert.NotEqual(t, nil, err)
		}
	}
}

func TestGetNumRowsOfBFloat16VectorField(t *testing.T) {
	cases := []struct {
		bDatas   []byte
		dim      int64
		want     uint64
		errIsNil bool
	}{
		{[]byte{}, -1, 0, false},     // dim <= 0
		{[]byte{}, 0, 0, false},      // dim <= 0
		{[]byte{1.0}, 128, 0, false}, // length % dim != 0
		{[]byte{}, 128, 0, true},
		{[]byte{1.0, 2.0}, 1, 1, true},
		{[]byte{1.0, 2.0, 3.0, 4.0}, 2, 1, true},
	}

	for _, test := range cases {
		got, err := GetNumRowsOfBFloat16VectorField(test.bDatas, test.dim)
		if test.errIsNil {
			assert.Equal(t, nil, err)
			if got != test.want {
				t.Errorf("GetNumRowsOfBFloat16VectorField(%v, %v) = %v, %v", test.bDatas, test.dim, test.want, nil)
			}
		} else {
			assert.NotEqual(t, nil, err)
		}
	}
}

func TestGetNumRowsOfBinaryVectorField(t *testing.T) {
	cases := []struct {
		bDatas   []byte
		dim      int64
		want     uint64
		errIsNil bool
	}{
		{[]byte{}, -1, 0, false},     // dim <= 0
		{[]byte{}, 0, 0, false},      // dim <= 0
		{[]byte{1.0}, 128, 0, false}, // length % dim != 0
		{[]byte{}, 128, 0, true},
		{[]byte{1.0}, 1, 0, false}, // dim % 8 != 0
		{[]byte{1.0}, 4, 0, false}, // dim % 8 != 0
		{[]byte{1.0, 2.0}, 8, 2, true},
		{[]byte{1.0, 2.0}, 16, 1, true},
		{[]byte{1.0, 2.0, 3.0, 4.0}, 8, 4, true},
		{[]byte{1.0, 2.0, 3.0, 4.0}, 16, 2, true},
		{[]byte{1.0}, 128, 0, false}, // (8*l) % dim != 0
	}

	for _, test := range cases {
		got, err := GetNumRowsOfBinaryVectorField(test.bDatas, test.dim)
		if test.errIsNil {
			assert.Equal(t, nil, err)
			if got != test.want {
				t.Errorf("GetNumRowsOfBinaryVectorField(%v, %v) = %v, %v", test.bDatas, test.dim, test.want, nil)
			}
		} else {
			assert.NotEqual(t, nil, err)
		}
	}
}

func Test_ReadBinary(t *testing.T) {
	// TODO: test big endian.
	// low byte in high address, high byte in low address.
	endian := binary.LittleEndian
	var bs []byte

	bs = []byte{0x1f}
	var i8 int8
	var expectedI8 int8 = 0x1f
	assert.NoError(t, ReadBinary(endian, bs, &i8))
	assert.Equal(t, expectedI8, i8)

	bs = []byte{0xff, 0x1f}
	var i16 int16
	var expectedI16 int16 = 0x1fff
	assert.NoError(t, ReadBinary(endian, bs, &i16))
	assert.Equal(t, expectedI16, i16)

	bs = []byte{0xff, 0xff, 0xff, 0x1f}
	var i32 int32
	var expectedI32 int32 = 0x1fffffff
	assert.NoError(t, ReadBinary(endian, bs, &i32))
	assert.Equal(t, expectedI32, i32)

	bs = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1f}
	var i64 int64
	var expectedI64 int64 = 0x1fffffffffffffff
	assert.NoError(t, ReadBinary(endian, bs, &i64))
	assert.Equal(t, expectedI64, i64)

	// hard to compare float-pointing value.

	bs = []byte{0, 0, 0, 0}
	var f float32
	// var expectedF32 float32 = 0
	var expectedF32 float32
	assert.NoError(t, ReadBinary(endian, bs, &f))
	assert.Equal(t, expectedF32, f)

	bs = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	var d float64
	// var expectedF64 float64 = 0
	var expectedF64 float64
	assert.NoError(t, ReadBinary(endian, bs, &d))
	assert.Equal(t, expectedF64, d)

	bs = []byte{0}
	var fb bool
	assert.NoError(t, ReadBinary(endian, bs, &fb))
	assert.False(t, fb)

	bs = []byte{1}
	var tb bool
	assert.NoError(t, ReadBinary(endian, bs, &tb))
	assert.True(t, tb)

	// float vector
	bs = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	fs := make([]float32, 2)
	assert.NoError(t, ReadBinary(endian, bs, &fs))
	assert.ElementsMatch(t, []float32{0, 0}, fs)
}

func TestIsGrpcErr(t *testing.T) {
	t.Run("nil error", func(t *testing.T) {
		var err error
		assert.False(t, IsGrpcErr(err))
	})

	t.Run("normal errors new", func(t *testing.T) {
		err := errors.New("error")
		assert.False(t, IsGrpcErr(err))
	})

	t.Run("context cancel", func(t *testing.T) {
		assert.False(t, IsGrpcErr(context.Canceled))
	})

	t.Run("context timeout", func(t *testing.T) {
		assert.False(t, IsGrpcErr(context.DeadlineExceeded))
	})

	t.Run("grpc canceled", func(t *testing.T) {
		err := grpcStatus.Error(grpcCodes.Canceled, "test")
		assert.True(t, IsGrpcErr(err))
	})

	t.Run("grpc unavailable", func(t *testing.T) {
		err := grpcStatus.Error(grpcCodes.Unavailable, "test")
		assert.True(t, IsGrpcErr(err))
	})

	t.Run("wrapped grpc error", func(t *testing.T) {
		err := grpcStatus.Error(grpcCodes.Unavailable, "test")
		errWrap := fmt.Errorf("wrap grpc error %w", err)
		assert.True(t, IsGrpcErr(errWrap))
	})

	t.Run("codes_match", func(t *testing.T) {
		err := grpcStatus.Error(grpcCodes.Unavailable, "test")
		errWrap := fmt.Errorf("wrap grpc error %w", err)
		assert.True(t, IsGrpcErr(errWrap, grpcCodes.Unimplemented, grpcCodes.Unavailable))
	})

	t.Run("codes_not_match", func(t *testing.T) {
		err := grpcStatus.Error(grpcCodes.Unavailable, "test")
		errWrap := fmt.Errorf("wrap grpc error %w", err)
		assert.False(t, IsGrpcErr(errWrap, grpcCodes.Unimplemented))
	})
}

func TestIsEmptyString(t *testing.T) {
	assert.Equal(t, IsEmptyString(""), true)
	assert.Equal(t, IsEmptyString(" "), true)
	assert.Equal(t, IsEmptyString("hello"), false)
}

func TestHandleTenantForEtcdKey(t *testing.T) {
	assert.Equal(t, "a/b/c", HandleTenantForEtcdKey("a", "b", "c"))

	assert.Equal(t, "a/b", HandleTenantForEtcdKey("a", "", "b"))

	assert.Equal(t, "a/b", HandleTenantForEtcdKey("a", "b", ""))

	assert.Equal(t, "a", HandleTenantForEtcdKey("a", "", ""))
}

func TestIsRevoke(t *testing.T) {
	assert.Equal(t, true, IsRevoke(milvuspb.OperatePrivilegeType_Revoke))
	assert.Equal(t, false, IsRevoke(milvuspb.OperatePrivilegeType_Grant))
}

func TestIsGrant(t *testing.T) {
	assert.Equal(t, true, IsGrant(milvuspb.OperatePrivilegeType_Grant))
	assert.Equal(t, false, IsGrant(milvuspb.OperatePrivilegeType_Revoke))
}

func TestUserRoleCache(t *testing.T) {
	user, role := "foo", "root"
	cache := EncodeUserRoleCache(user, role)
	assert.Equal(t, fmt.Sprintf("%s/%s", user, role), cache)
	u, r, err := DecodeUserRoleCache(cache)
	assert.Equal(t, user, u)
	assert.Equal(t, role, r)
	assert.NoError(t, err)

	_, _, err = DecodeUserRoleCache("foo")
	assert.Error(t, err)
}

func TestMapToJSON(t *testing.T) {
	s := `{"M": 30,"efConstruction": 360,"index_type": "HNSW", "metric_type": "IP"}`
	m, err := JSONToMap(s)
	assert.NoError(t, err)
	j, err := MapToJSON(m)
	assert.NoError(t, err)
	got, err := JSONToMap(j)
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(m, got))
}

type NumRowsWithSchemaSuite struct {
	suite.Suite
	helper *typeutil.SchemaHelper
}

func (s *NumRowsWithSchemaSuite) SetupSuite() {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "int8", DataType: schemapb.DataType_Int8},
			{FieldID: 102, Name: "int16", DataType: schemapb.DataType_Int16},
			{FieldID: 103, Name: "int32", DataType: schemapb.DataType_Int32},
			{FieldID: 104, Name: "bool", DataType: schemapb.DataType_Bool},
			{FieldID: 105, Name: "float", DataType: schemapb.DataType_Float},
			{FieldID: 106, Name: "double", DataType: schemapb.DataType_Double},
			{FieldID: 107, Name: "varchar", DataType: schemapb.DataType_VarChar},
			{FieldID: 108, Name: "array", DataType: schemapb.DataType_Array},
			{FieldID: 109, Name: "json", DataType: schemapb.DataType_JSON},
			{FieldID: 110, Name: "float_vector", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}},
			{FieldID: 111, Name: "binary_vector", DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}},
			{FieldID: 112, Name: "float16_vector", DataType: schemapb.DataType_Float16Vector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}},
			{FieldID: 113, Name: "bfloat16_vector", DataType: schemapb.DataType_BFloat16Vector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}},
			{FieldID: 114, Name: "sparse_vector", DataType: schemapb.DataType_SparseFloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}},
			{FieldID: 999, Name: "unknown", DataType: schemapb.DataType_None},
		},
	}
	helper, err := typeutil.CreateSchemaHelper(schema)
	s.Require().NoError(err)
	s.helper = helper
}

func (s *NumRowsWithSchemaSuite) TestNormalCases() {
	type testCase struct {
		tag    string
		input  *schemapb.FieldData
		expect uint64
	}

	cases := []*testCase{
		{
			tag: "int64",
			input: &schemapb.FieldData{
				FieldName: "int64",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}}}},
				},
			},
			expect: 3,
		},
		{
			tag: "int8",
			input: &schemapb.FieldData{
				FieldName: "int8",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 2, 3, 4}}}},
				},
			},
			expect: 4,
		},
		{
			tag: "int16",
			input: &schemapb.FieldData{
				FieldName: "int16",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 2, 3, 4, 5}}}},
				},
			},
			expect: 5,
		},
		{
			tag: "int32",
			input: &schemapb.FieldData{
				FieldName: "int32",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{1, 2, 3, 4, 5}}}},
				},
			},
			expect: 5,
		},
		{
			tag: "bool",
			input: &schemapb.FieldData{
				FieldName: "bool",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{Data: make([]bool, 4)}}},
				},
			},
			expect: 4,
		},
		{
			tag: "float",
			input: &schemapb.FieldData{
				FieldName: "float",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{Data: make([]float32, 6)}}},
				},
			},
			expect: 6,
		},
		{
			tag: "double",
			input: &schemapb.FieldData{
				FieldName: "double",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{Data: make([]float64, 8)}}},
				},
			},
			expect: 8,
		},
		{
			tag: "varchar",
			input: &schemapb.FieldData{
				FieldName: "varchar",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: make([]string, 7)}}},
				},
			},
			expect: 7,
		},
		{
			tag: "array",
			input: &schemapb.FieldData{
				FieldName: "array",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: make([]*schemapb.ScalarField, 9)}}},
				},
			},
			expect: 9,
		},
		{
			tag: "json",
			input: &schemapb.FieldData{
				FieldName: "json",
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_JsonData{JsonData: &schemapb.JSONArray{Data: make([][]byte, 7)}}},
				},
			},
			expect: 7,
		},
		{
			tag: "float_vector",
			input: &schemapb.FieldData{
				FieldName: "float_vector",
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim:  8,
						Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, 7*8)}},
					},
				},
			},
			expect: 7,
		},
		{
			tag: "binary_vector",
			input: &schemapb.FieldData{
				FieldName: "binary_vector",
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim:  8,
						Data: &schemapb.VectorField_BinaryVector{BinaryVector: make([]byte, 8)},
					},
				},
			},
			expect: 8,
		},
		{
			tag: "float16_vector",
			input: &schemapb.FieldData{
				FieldName: "float16_vector",
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim:  8,
						Data: &schemapb.VectorField_Float16Vector{Float16Vector: make([]byte, 8*2*5)},
					},
				},
			},
			expect: 5,
		},
		{
			tag: "bfloat16_vector",
			input: &schemapb.FieldData{
				FieldName: "bfloat16_vector",
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim:  8,
						Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: make([]byte, 8*2*5)},
					},
				},
			},
			expect: 5,
		},
		{
			tag: "sparse_vector",
			input: &schemapb.FieldData{
				FieldName: "sparse_vector",
				Field: &schemapb.FieldData_Vectors{
					Vectors: &schemapb.VectorField{
						Dim:  8,
						Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Contents: make([][]byte, 6)}},
					},
				},
			},
			expect: 6,
		},
	}
	for _, tc := range cases {
		s.Run(tc.tag, func() {
			r, err := GetNumRowOfFieldDataWithSchema(tc.input, s.helper)
			s.NoError(err)
			s.Equal(tc.expect, r)
		})
	}
}

func (s *NumRowsWithSchemaSuite) TestErrorCases() {
	s.Run("nil_field_data", func() {
		_, err := GetNumRowOfFieldDataWithSchema(nil, s.helper)
		s.Error(err)
	})

	s.Run("data_type_unknown", func() {
		_, err := GetNumRowOfFieldDataWithSchema(&schemapb.FieldData{
			FieldName: "unknown",
		}, s.helper)
		s.Error(err)
	})

	s.Run("bad_dim_vector", func() {
		type testCase struct {
			tag   string
			input *schemapb.FieldData
		}

		cases := []testCase{
			{
				tag: "float_vector",
				input: &schemapb.FieldData{
					FieldName: "float_vector",
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							Dim:  3,
							Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, 7*8)}},
						},
					},
				},
			},
			{
				tag: "binary_vector",
				input: &schemapb.FieldData{
					FieldName: "binary_vector",
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							Dim:  5,
							Data: &schemapb.VectorField_BinaryVector{BinaryVector: make([]byte, 8)},
						},
					},
				},
			},
			{
				tag: "float16_vector",
				input: &schemapb.FieldData{
					FieldName: "float16_vector",
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							Dim:  13,
							Data: &schemapb.VectorField_Float16Vector{Float16Vector: make([]byte, 8*2*5)},
						},
					},
				},
			},
			{
				tag: "bfloat16_vector",
				input: &schemapb.FieldData{
					FieldName: "bfloat16_vector",
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							Dim:  13,
							Data: &schemapb.VectorField_Bfloat16Vector{Bfloat16Vector: make([]byte, 8*2*5)},
						},
					},
				},
			},
		}

		for _, tc := range cases {
			s.Run(tc.tag, func() {
				_, err := GetNumRowOfFieldDataWithSchema(tc.input, s.helper)
				s.Error(err)
			})
		}
	})
}

func TestNumRowsWithSchema(t *testing.T) {
	suite.Run(t, new(NumRowsWithSchemaSuite))
}

func TestChannelConvert(t *testing.T) {
	t.Run("is physical channel", func(t *testing.T) {
		{
			channel := "by-dev-replicate-msg"
			ok := IsPhysicalChannel(channel)
			assert.True(t, ok)
		}

		{
			channel := "by-dev-rootcoord-dml_2"
			ok := IsPhysicalChannel(channel)
			assert.True(t, ok)
		}

		{
			channel := "by-dev-rootcoord-dml_2_1001v0"
			ok := IsPhysicalChannel(channel)
			assert.False(t, ok)
		}
	})

	t.Run("to physical channel", func(t *testing.T) {
		{
			channel := "by-dev-rootcoord-dml_2_1001v0"
			physicalChannel := ToPhysicalChannel(channel)
			assert.Equal(t, "by-dev-rootcoord-dml_2", physicalChannel)
		}

		{
			channel := "by-dev-rootcoord-dml_2"
			physicalChannel := ToPhysicalChannel(channel)
			assert.Equal(t, "by-dev-rootcoord-dml_2", physicalChannel)
		}

		{
			channel := "by-dev-replicate-msg"
			physicalChannel := ToPhysicalChannel(channel)
			assert.Equal(t, "by-dev-replicate-msg", physicalChannel)
		}
	})

	t.Run("get virtual channel", func(t *testing.T) {
		channel := GetVirtualChannel("by-dev-rootcoord-dml_2", 1001, 0)
		assert.Equal(t, "by-dev-rootcoord-dml_2_1001v0", channel)
	})
}

func TestString2KeyValuePair(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		kvs, err := String2KeyValuePair("{\"key\": \"value\"}")
		assert.NoError(t, err)
		assert.Len(t, kvs, 1)
		assert.Equal(t, "key", kvs[0].Key)
		assert.Equal(t, "value", kvs[0].Value)
	})

	t.Run("err", func(t *testing.T) {
		_, err := String2KeyValuePair("{aa}")
		assert.Error(t, err)
	})

	t.Run("empty", func(t *testing.T) {
		kvs, err := String2KeyValuePair("{}")
		assert.NoError(t, err)
		assert.Len(t, kvs, 0)
	})
}
