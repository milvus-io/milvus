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
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/stretchr/testify/assert"
	grpcCodes "google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"
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

func Test_GetLocalIP(t *testing.T) {
	ip := GetLocalIP()
	assert.NotNil(t, ip)
	assert.NotZero(t, len(ip))
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
	assert.Equal(t, "abc", ToPhysicalChannel("abc_"))
	assert.Equal(t, "abc", ToPhysicalChannel("abc_123"))
	assert.Equal(t, "abc", ToPhysicalChannel("abc_defgsg"))
	assert.Equal(t, "abc__", ToPhysicalChannel("abc___defgsg"))
	assert.Equal(t, "abcdef", ToPhysicalChannel("abcdef"))
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
	var fs = make([]float32, 2)
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
	j := MapToJSON(m)
	got, err := JSONToMap(string(j))
	assert.NoError(t, err)
	assert.True(t, reflect.DeepEqual(m, got))
}
