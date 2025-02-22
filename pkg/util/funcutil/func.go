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
	"fmt"
	"net"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	grpcStatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// CheckGrpcReady wait for context timeout, or wait 100ms then send nil to targetCh
func CheckGrpcReady(ctx context.Context, targetCh chan error) {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		targetCh <- nil
	case <-ctx.Done():
		return
	}
}

// GetIP return the ip address
func GetIP(ip string) string {
	if len(ip) == 0 {
		return GetLocalIP()
	}
	netIP := net.ParseIP(ip)
	// not a valid ip addr
	if netIP == nil {
		log.Warn("cannot parse input ip, treat it as hostname/service name", zap.String("ip", ip))
		return ip
	}
	// only localhost or unicast is acceptable
	if netIP.IsUnspecified() {
		panic(errors.Newf(`"%s" in param table is Unspecified IP address and cannot be used`))
	}
	if netIP.IsMulticast() || netIP.IsLinkLocalMulticast() || netIP.IsInterfaceLocalMulticast() {
		panic(errors.Newf(`"%s" in param table is Multicast IP address and cannot be used`))
	}
	return ip
}

// GetLocalIP return the local ip address
func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err == nil {
		ip := GetValidLocalIP(addrs)
		if len(ip) != 0 {
			return ip
		}
	}
	return "127.0.0.1"
}

// GetValidLocalIP return the first valid local ip address
func GetValidLocalIP(addrs []net.Addr) string {
	// Search for valid ipv4 addresses
	for _, addr := range addrs {
		ipaddr, ok := addr.(*net.IPNet)
		if ok && ipaddr.IP.IsGlobalUnicast() && ipaddr.IP.To4() != nil {
			return ipaddr.IP.String()
		}
	}
	// Search for valid ipv6 addresses
	for _, addr := range addrs {
		ipaddr, ok := addr.(*net.IPNet)
		if ok && ipaddr.IP.IsGlobalUnicast() && ipaddr.IP.To16() != nil && ipaddr.IP.To4() == nil {
			return "[" + ipaddr.IP.String() + "]"
		}
	}
	return ""
}

// JSONToMap parse the jsonic index parameters to map
func JSONToMap(mStr string) (map[string]string, error) {
	buffer := make(map[string]any)
	err := json.Unmarshal([]byte(mStr), &buffer)
	if err != nil {
		return nil, fmt.Errorf("unmarshal params failed, %w", err)
	}
	ret := make(map[string]string)
	for key, value := range buffer {
		valueStr := fmt.Sprintf("%v", value)
		ret[key] = valueStr
	}
	return ret, nil
}

func MapToJSON(m map[string]string) (string, error) {
	// error won't happen here.
	bs, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(bs), nil
}

func JSONToRoleDetails(mStr string) (map[string](map[string]([](map[string]string))), error) {
	buffer := make(map[string](map[string]([](map[string]string))), 0)
	err := json.Unmarshal([]byte(mStr), &buffer)
	if err != nil {
		return nil, fmt.Errorf("unmarshal `builtinRoles.Roles` failed, %w", err)
	}
	ret := make(map[string](map[string]([](map[string]string))), 0)
	for role, privilegesJSON := range buffer {
		ret[role] = make(map[string]([](map[string]string)), 0)
		privilegesArray := make([]map[string]string, 0)
		for _, privileges := range privilegesJSON[util.RoleConfigPrivileges] {
			privilegesArray = append(privilegesArray, map[string]string{
				util.RoleConfigObjectType: privileges[util.RoleConfigObjectType],
				util.RoleConfigObjectName: privileges[util.RoleConfigObjectName],
				util.RoleConfigPrivilege:  privileges[util.RoleConfigPrivilege],
				util.RoleConfigDBName:     privileges[util.RoleConfigDBName],
			})
		}
		ret[role]["privileges"] = privilegesArray
	}
	return ret, nil
}

func RoleDetailsToJSON(m map[string](map[string]([](map[string]string)))) []byte {
	bs, _ := json.Marshal(m)
	return bs
}

const (
	// PulsarMaxMessageSizeKey is the key of config item
	PulsarMaxMessageSizeKey = "maxMessageSize"
)

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
		if typeutil.IsVectorType(field.DataType) {
			vecFieldIDs = append(vecFieldIDs, field.FieldID)
		}
	}
	return vecFieldIDs
}

func String2KeyValuePair(v string) ([]*commonpb.KeyValuePair, error) {
	m := make(map[string]string)
	err := json.Unmarshal([]byte(v), &m)
	if err != nil {
		return nil, err
	}
	return Map2KeyValuePair(m), nil
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

// IsPhysicalChannel checks if the channel is a physical channel
func IsPhysicalChannel(channel string) bool {
	i := strings.LastIndex(channel, "_")
	if i == -1 {
		return true
	}
	return !strings.Contains(channel[i+1:], "v")
}

// ToPhysicalChannel get physical channel name from virtual channel name
func ToPhysicalChannel(vchannel string) string {
	if IsPhysicalChannel(vchannel) {
		return vchannel
	}
	index := strings.LastIndex(vchannel, "_")
	if index < 0 {
		return vchannel
	}
	return vchannel[:index]
}

func GetVirtualChannel(pchannel string, collectionID int64, idx int) string {
	return fmt.Sprintf("%s_%dv%d", pchannel, collectionID, idx)
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

func GetCollectionIDFromVChannel(vChannelName string) int64 {
	re := regexp.MustCompile(`.*_(\d+)v\d+`)
	matches := re.FindStringSubmatch(vChannelName)
	if len(matches) > 1 {
		number, err := strconv.ParseInt(matches[1], 0, 64)
		if err == nil {
			return number
		}
	}
	return -1
}

func getNumRowsOfScalarField(datas interface{}) uint64 {
	realTypeDatas := reflect.ValueOf(datas)
	return uint64(realTypeDatas.Len())
}

func GetNumRowsOfFloatVectorField(fDatas []float32, dim int64) (uint64, error) {
	if dim <= 0 {
		return 0, fmt.Errorf("dim(%d) should be greater than 0", dim)
	}
	l := len(fDatas)
	if int64(l)%dim != 0 {
		return 0, fmt.Errorf("the length(%d) of float data should divide the dim(%d)", l, dim)
	}
	return uint64(int64(l) / dim), nil
}

func GetNumRowsOfBinaryVectorField(bDatas []byte, dim int64) (uint64, error) {
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

func GetNumRowsOfFloat16VectorField(f16Datas []byte, dim int64) (uint64, error) {
	if dim <= 0 {
		return 0, fmt.Errorf("dim(%d) should be greater than 0", dim)
	}
	l := len(f16Datas)
	if int64(l)%dim != 0 {
		return 0, fmt.Errorf("the length(%d) of float16 data should divide the dim(%d)", l, dim)
	}
	return uint64((int64(l)) / dim / 2), nil
}

func GetNumRowsOfBFloat16VectorField(bf16Datas []byte, dim int64) (uint64, error) {
	if dim <= 0 {
		return 0, fmt.Errorf("dim(%d) should be greater than 0", dim)
	}
	l := len(bf16Datas)
	if int64(l)%dim != 0 {
		return 0, fmt.Errorf("the length(%d) of bfloat data should divide the dim(%d)", l, dim)
	}
	return uint64((int64(l)) / dim / 2), nil
}

// GetNumRowOfFieldDataWithSchema returns num of rows with schema specification.
func GetNumRowOfFieldDataWithSchema(fieldData *schemapb.FieldData, helper *typeutil.SchemaHelper) (uint64, error) {
	var fieldNumRows uint64
	var err error
	fieldSchema, err := helper.GetFieldFromName(fieldData.GetFieldName())
	if err != nil {
		return 0, err
	}
	switch fieldSchema.GetDataType() {
	case schemapb.DataType_Bool:
		fieldNumRows = getNumRowsOfScalarField(fieldData.GetScalars().GetBoolData().GetData())
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		fieldNumRows = getNumRowsOfScalarField(fieldData.GetScalars().GetIntData().GetData())
	case schemapb.DataType_Int64:
		fieldNumRows = getNumRowsOfScalarField(fieldData.GetScalars().GetLongData().GetData())
	case schemapb.DataType_Float:
		fieldNumRows = getNumRowsOfScalarField(fieldData.GetScalars().GetFloatData().GetData())
	case schemapb.DataType_Double:
		fieldNumRows = getNumRowsOfScalarField(fieldData.GetScalars().GetDoubleData().GetData())
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		fieldNumRows = getNumRowsOfScalarField(fieldData.GetScalars().GetStringData().GetData())
	case schemapb.DataType_Array:
		fieldNumRows = getNumRowsOfScalarField(fieldData.GetScalars().GetArrayData().GetData())
	case schemapb.DataType_JSON:
		fieldNumRows = getNumRowsOfScalarField(fieldData.GetScalars().GetJsonData().GetData())
	case schemapb.DataType_FloatVector:
		dim := fieldData.GetVectors().GetDim()
		fieldNumRows, err = GetNumRowsOfFloatVectorField(fieldData.GetVectors().GetFloatVector().GetData(), dim)
		if err != nil {
			return 0, err
		}
	case schemapb.DataType_BinaryVector:
		dim := fieldData.GetVectors().GetDim()
		fieldNumRows, err = GetNumRowsOfBinaryVectorField(fieldData.GetVectors().GetBinaryVector(), dim)
		if err != nil {
			return 0, err
		}
	case schemapb.DataType_Float16Vector:
		dim := fieldData.GetVectors().GetDim()
		fieldNumRows, err = GetNumRowsOfFloat16VectorField(fieldData.GetVectors().GetFloat16Vector(), dim)
		if err != nil {
			return 0, err
		}
	case schemapb.DataType_BFloat16Vector:
		dim := fieldData.GetVectors().GetDim()
		fieldNumRows, err = GetNumRowsOfBFloat16VectorField(fieldData.GetVectors().GetBfloat16Vector(), dim)
		if err != nil {
			return 0, err
		}
	case schemapb.DataType_SparseFloatVector:
		fieldNumRows = uint64(len(fieldData.GetVectors().GetSparseFloatVector().GetContents()))
	default:
		return 0, fmt.Errorf("%s is not supported now", fieldSchema.GetDataType())
	}

	return fieldNumRows, nil
}

// GetNumRowOfFieldData returns num of rows from the field data type
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
		case *schemapb.ScalarField_ArrayData:
			fieldNumRows = getNumRowsOfScalarField(scalarField.GetArrayData().Data)
		case *schemapb.ScalarField_JsonData:
			fieldNumRows = getNumRowsOfScalarField(scalarField.GetJsonData().Data)
		default:
			return 0, fmt.Errorf("%s is not supported now", scalarType)
		}
	case *schemapb.FieldData_Vectors:
		vectorField := fieldData.GetVectors()
		switch vectorFieldType := vectorField.Data.(type) {
		case *schemapb.VectorField_FloatVector:
			dim := vectorField.GetDim()
			fieldNumRows, err = GetNumRowsOfFloatVectorField(vectorField.GetFloatVector().Data, dim)
			if err != nil {
				return 0, err
			}
		case *schemapb.VectorField_BinaryVector:
			dim := vectorField.GetDim()
			fieldNumRows, err = GetNumRowsOfBinaryVectorField(vectorField.GetBinaryVector(), dim)
			if err != nil {
				return 0, err
			}
		case *schemapb.VectorField_Float16Vector:
			dim := vectorField.GetDim()
			fieldNumRows, err = GetNumRowsOfFloat16VectorField(vectorField.GetFloat16Vector(), dim)
			if err != nil {
				return 0, err
			}
		case *schemapb.VectorField_Bfloat16Vector:
			dim := vectorField.GetDim()
			fieldNumRows, err = GetNumRowsOfBFloat16VectorField(vectorField.GetBfloat16Vector(), dim)
			if err != nil {
				return 0, err
			}
		case *schemapb.VectorField_SparseFloatVector:
			fieldNumRows = uint64(len(vectorField.GetSparseFloatVector().GetContents()))
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
func IsGrpcErr(err error, targets ...codes.Code) bool {
	set := typeutil.NewSet[codes.Code](targets...)
	for {
		if err == nil {
			return false
		}
		s, ok := grpcStatus.FromError(err)
		if ok {
			return set.Len() == 0 || set.Contain(s.Code())
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

func EncodeUserRoleCache(user string, role string) string {
	return fmt.Sprintf("%s/%s", user, role)
}

func DecodeUserRoleCache(cache string) (string, string, error) {
	index := strings.LastIndex(cache, "/")
	if index == -1 {
		return "", "", fmt.Errorf("invalid param, cache: [%s]", cache)
	}
	user := cache[:index]
	role := cache[index+1:]
	return user, role, nil
}
