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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/milvus-io/milvus/internal/proto/milvuspb"

	"go.uber.org/zap"

	"github.com/go-basic/ipv4"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/retry"

	grpcStatus "google.golang.org/grpc/status"
)

// CheckGrpcReady wait for context timeout, or wait 100ms then send nil to targetCh
func CheckGrpcReady(ctx context.Context, targetCh chan error) {
	select {
	case <-time.After(100 * time.Millisecond):
		targetCh <- nil
	case <-ctx.Done():
		return
	}
}

// GetLocalIP return the local ip address
func GetLocalIP() string {
	return ipv4.LocalIP()
}

// WaitForComponentStates wait for component's state to be one of the specific states
func WaitForComponentStates(ctx context.Context, service types.Component, serviceName string, states []internalpb.StateCode, attempts uint, sleep time.Duration) error {
	checkFunc := func() error {
		resp, err := service.GetComponentStates(ctx)
		if err != nil {
			return err
		}

		if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(resp.Status.Reason)
		}

		meet := false
		for _, state := range states {
			if resp.State.StateCode == state {
				meet = true
				break
			}
		}
		if !meet {
			return fmt.Errorf(
				"WaitForComponentStates, not meet, %s current state: %s",
				serviceName,
				resp.State.StateCode.String())
		}
		return nil
	}
	return retry.Do(ctx, checkFunc, retry.Attempts(attempts), retry.Sleep(sleep))
}

// WaitForComponentInitOrHealthy wait for component's state to be initializing or healthy
func WaitForComponentInitOrHealthy(ctx context.Context, service types.Component, serviceName string, attempts uint, sleep time.Duration) error {
	return WaitForComponentStates(ctx, service, serviceName, []internalpb.StateCode{internalpb.StateCode_Initializing, internalpb.StateCode_Healthy}, attempts, sleep)
}

// WaitForComponentInit wait for component's state to be initializing
func WaitForComponentInit(ctx context.Context, service types.Component, serviceName string, attempts uint, sleep time.Duration) error {
	return WaitForComponentStates(ctx, service, serviceName, []internalpb.StateCode{internalpb.StateCode_Initializing}, attempts, sleep)
}

// WaitForComponentHealthy wait for component's state to be healthy
func WaitForComponentHealthy(ctx context.Context, service types.Component, serviceName string, attempts uint, sleep time.Duration) error {
	return WaitForComponentStates(ctx, service, serviceName, []internalpb.StateCode{internalpb.StateCode_Healthy}, attempts, sleep)
}

// ParseIndexParamsMap parse the jsonic index parameters to map
func ParseIndexParamsMap(mStr string) (map[string]string, error) {
	buffer := make(map[string]interface{})
	err := json.Unmarshal([]byte(mStr), &buffer)
	if err != nil {
		return nil, errors.New("Unmarshal params failed")
	}
	ret := make(map[string]string)
	for key, value := range buffer {
		valueStr := fmt.Sprintf("%v", value)
		ret[key] = valueStr
	}
	return ret, nil
}

const (
	// PulsarMaxMessageSizeKey is the key of config item
	PulsarMaxMessageSizeKey = "maxMessageSize"
)

// GetPulsarConfig get pulsar configuration using pulsar admin api
func GetPulsarConfig(protocol, ip, port, url string, args ...int64) (map[string]interface{}, error) {
	var resp *http.Response
	var err error

	getResp := func() error {
		log.Debug("function util", zap.String("url", protocol+"://"+ip+":"+port+url))
		resp, err = http.Get(protocol + "://" + ip + ":" + port + url)
		return err
	}

	var attempt uint = 10
	var interval = time.Second
	if len(args) > 0 && args[0] > 0 {
		attempt = uint(args[0])
	}
	if len(args) > 1 && args[1] > 0 {
		interval = time.Duration(args[1])
	}

	err = retry.Do(context.TODO(), getResp, retry.Attempts(attempt), retry.Sleep(interval))
	if err != nil {
		log.Debug("failed to get config", zap.String("error", err.Error()))
		return nil, err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	log.Debug("get config", zap.String("config", string(body)))
	if err != nil {
		return nil, err
	}

	ret := make(map[string]interface{})
	err = json.Unmarshal(body, &ret)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

// GetAttrByKeyFromRepeatedKV return the value corresponding to key in kv pair
func GetAttrByKeyFromRepeatedKV(key string, kvs []*commonpb.KeyValuePair) (string, error) {
	for _, kv := range kvs {
		if kv.Key == key {
			return kv.Value, nil
		}
	}

	return "", fmt.Errorf("key %s not found", key)
}

// CheckCtxValid check if the context is valid
func CheckCtxValid(ctx context.Context) bool {
	return ctx.Err() != context.DeadlineExceeded && ctx.Err() != context.Canceled
}

func GetVecFieldIDs(schema *schemapb.CollectionSchema) []int64 {
	var vecFieldIDs []int64
	for _, field := range schema.Fields {
		if field.DataType == schemapb.DataType_BinaryVector || field.DataType == schemapb.DataType_FloatVector {
			vecFieldIDs = append(vecFieldIDs, field.FieldID)
		}
	}

	return vecFieldIDs
}

func Map2KeyValuePair(datas map[string]string) []*commonpb.KeyValuePair {
	results := make([]*commonpb.KeyValuePair, len(datas))
	offset := 0
	for key, value := range datas {
		results[offset] = &commonpb.KeyValuePair{
			Key:   key,
			Value: value,
		}
		offset++
	}
	return results
}

func KeyValuePair2Map(datas []*commonpb.KeyValuePair) map[string]string {
	results := make(map[string]string)
	for _, pair := range datas {
		results[pair.Key] = pair.Value
	}

	return results
}

func ConvertToKeyValuePairPointer(datas []commonpb.KeyValuePair) []*commonpb.KeyValuePair {
	var kvs []*commonpb.KeyValuePair
	for i := 0; i < len(datas); i++ {
		kvs = append(kvs, &datas[i])
	}
	return kvs
}

// GenChannelSubName generate subName to watch channel
func GenChannelSubName(prefix string, collectionID int64, nodeID int64) string {
	return fmt.Sprintf("%s-%d-%d", prefix, collectionID, nodeID)
}

// CheckPortAvailable check if a port is available to be listened on
func CheckPortAvailable(port int) bool {
	addr := ":" + strconv.Itoa(port)
	listener, err := net.Listen("tcp", addr)
	if listener != nil {
		listener.Close()
	}
	return err == nil
}

// GetAvailablePort return an available port that can be listened on
func GetAvailablePort() int {
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	return listener.Addr().(*net.TCPAddr).Port
}

// ToPhysicalChannel get physical channel name from virtual channel name
func ToPhysicalChannel(vchannel string) string {
	index := strings.LastIndex(vchannel, "_")
	if index < 0 {
		return vchannel
	}
	return vchannel[:index]
}

// ConvertChannelName assembles channel name according to parameters.
func ConvertChannelName(chanName string, tokenFrom string, tokenTo string) (string, error) {
	if tokenFrom == "" {
		return "", fmt.Errorf("the tokenFrom is empty")
	}
	if !strings.Contains(chanName, tokenFrom) {
		return "", fmt.Errorf("cannot find token '%s' in '%s'", tokenFrom, chanName)
	}
	return strings.Replace(chanName, tokenFrom, tokenTo, 1), nil
}

func getNumRowsOfScalarField(datas interface{}) uint64 {
	realTypeDatas := reflect.ValueOf(datas)
	return uint64(realTypeDatas.Len())
}

func getNumRowsOfFloatVectorField(fDatas []float32, dim int64) (uint64, error) {
	if dim <= 0 {
		return 0, fmt.Errorf("dim(%d) should be greater than 0", dim)
	}
	l := len(fDatas)
	if int64(l)%dim != 0 {
		return 0, fmt.Errorf("the length(%d) of float data should divide the dim(%d)", l, dim)
	}
	return uint64(int64(l) / dim), nil
}

func getNumRowsOfBinaryVectorField(bDatas []byte, dim int64) (uint64, error) {
	if dim <= 0 {
		return 0, fmt.Errorf("dim(%d) should be greater than 0", dim)
	}
	if dim%8 != 0 {
		return 0, fmt.Errorf("dim(%d) should divide 8", dim)
	}
	l := len(bDatas)
	if (8*int64(l))%dim != 0 {
		return 0, fmt.Errorf("the num(%d) of all bits should divide the dim(%d)", 8*l, dim)
	}
	return uint64((8 * int64(l)) / dim), nil
}

// GetNumRowOfFieldData return num rows of the field data
func GetNumRowOfFieldData(fieldData *schemapb.FieldData) (uint64, error) {
	var fieldNumRows uint64
	var err error
	switch fieldType := fieldData.Field.(type) {
	case *schemapb.FieldData_Scalars:
		scalarField := fieldData.GetScalars()
		switch scalarType := scalarField.Data.(type) {
		case *schemapb.ScalarField_BoolData:
			fieldNumRows = getNumRowsOfScalarField(scalarField.GetBoolData().Data)
		case *schemapb.ScalarField_IntData:
			fieldNumRows = getNumRowsOfScalarField(scalarField.GetIntData().Data)
		case *schemapb.ScalarField_LongData:
			fieldNumRows = getNumRowsOfScalarField(scalarField.GetLongData().Data)
		case *schemapb.ScalarField_FloatData:
			fieldNumRows = getNumRowsOfScalarField(scalarField.GetFloatData().Data)
		case *schemapb.ScalarField_DoubleData:
			fieldNumRows = getNumRowsOfScalarField(scalarField.GetDoubleData().Data)
		case *schemapb.ScalarField_StringData:
			fieldNumRows = getNumRowsOfScalarField(scalarField.GetStringData().Data)
		default:
			return 0, fmt.Errorf("%s is not supported now", scalarType)
		}
	case *schemapb.FieldData_Vectors:
		vectorField := fieldData.GetVectors()
		switch vectorFieldType := vectorField.Data.(type) {
		case *schemapb.VectorField_FloatVector:
			dim := vectorField.GetDim()
			fieldNumRows, err = getNumRowsOfFloatVectorField(vectorField.GetFloatVector().Data, dim)
			if err != nil {
				return 0, err
			}
		case *schemapb.VectorField_BinaryVector:
			dim := vectorField.GetDim()
			fieldNumRows, err = getNumRowsOfBinaryVectorField(vectorField.GetBinaryVector(), dim)
			if err != nil {
				return 0, err
			}
		default:
			return 0, fmt.Errorf("%s is not supported now", vectorFieldType)
		}
	default:
		return 0, fmt.Errorf("%s is not supported now", fieldType)
	}

	return fieldNumRows, nil
}

// ReadBinary read byte slice as receiver.
func ReadBinary(endian binary.ByteOrder, bs []byte, receiver interface{}) error {
	buf := bytes.NewReader(bs)
	return binary.Read(buf, endian, receiver)
}

// IsGrpcErr checks whether err is instance of grpc status error.
func IsGrpcErr(err error) bool {
	for {
		if err == nil {
			return false
		}
		_, ok := grpcStatus.FromError(err)
		if ok {
			return true
		}
		err = errors.Unwrap(err)
	}
}

func IsEmptyString(str string) bool {
	return strings.TrimSpace(str) == ""
}

func HandleTenantForEtcdKey(prefix string, tenant string, key string) string {
	res := prefix
	if tenant != "" {
		res += "/" + tenant
	}
	if key != "" {
		res += "/" + key
	}
	return res
}

func IsRevoke(operateType milvuspb.OperatePrivilegeType) bool {
	return operateType == milvuspb.OperatePrivilegeType_Revoke
}

func IsGrant(operateType milvuspb.OperatePrivilegeType) bool {
	return operateType == milvuspb.OperatePrivilegeType_Grant
}

// IsKeyNotExistError Judging by the error message whether the key does not exist or not for the ectd server
func IsKeyNotExistError(err error) bool {
	return strings.Contains(err.Error(), "there is no value on key")
}

func EncodeUserRoleCache(user string, role string) string {
	return fmt.Sprintf("%s/%s", user, role)
}

func DecodeUserRoleCache(cache string) (string, string) {
	index := strings.LastIndex(cache, "/")
	user := cache[:index]
	role := cache[index+1:]
	return user, role
}
