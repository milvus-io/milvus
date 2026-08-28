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

package storage

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type PrimaryKey interface {
	GT(key PrimaryKey) bool
	GE(key PrimaryKey) bool
	LT(key PrimaryKey) bool
	LE(key PrimaryKey) bool
	EQ(key PrimaryKey) bool
	MarshalJSON() ([]byte, error)
	UnmarshalJSON(data []byte) error
	SetValue(interface{}) error
	GetValue() interface{}
	Type() schemapb.DataType
	Size() int64
}

type Int64PrimaryKey struct {
	Value int64 `json:"pkValue"`
}

func NewInt64PrimaryKey(v int64) *Int64PrimaryKey {
	return &Int64PrimaryKey{
		Value: v,
	}
}

func (ip *Int64PrimaryKey) GT(key PrimaryKey) bool {
	pk, ok := key.(*Int64PrimaryKey)
	if !ok {
		mlog.Warn(context.TODO(), "type of compared pk is not int64")
		return false
	}
	if ip.Value > pk.Value {
		return true
	}

	return false
}

func (ip *Int64PrimaryKey) GE(key PrimaryKey) bool {
	pk, ok := key.(*Int64PrimaryKey)
	if !ok {
		mlog.Warn(context.TODO(), "type of compared pk is not int64")
		return false
	}
	if ip.Value >= pk.Value {
		return true
	}

	return false
}

func (ip *Int64PrimaryKey) LT(key PrimaryKey) bool {
	pk, ok := key.(*Int64PrimaryKey)
	if !ok {
		mlog.Warn(context.TODO(), "type of compared pk is not int64")
		return false
	}

	if ip.Value < pk.Value {
		return true
	}

	return false
}

func (ip *Int64PrimaryKey) LE(key PrimaryKey) bool {
	pk, ok := key.(*Int64PrimaryKey)
	if !ok {
		mlog.Warn(context.TODO(), "type of compared pk is not int64")
		return false
	}

	if ip.Value <= pk.Value {
		return true
	}

	return false
}

func (ip *Int64PrimaryKey) EQ(key PrimaryKey) bool {
	pk, ok := key.(*Int64PrimaryKey)
	if !ok {
		mlog.Warn(context.TODO(), "type of compared pk is not int64")
		return false
	}

	if ip.Value == pk.Value {
		return true
	}

	return false
}

func (ip *Int64PrimaryKey) MarshalJSON() ([]byte, error) {
	ret, err := json.Marshal(ip.Value)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (ip *Int64PrimaryKey) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &ip.Value)
	if err != nil {
		return err
	}

	return nil
}

func (ip *Int64PrimaryKey) SetValue(data interface{}) error {
	value, ok := data.(int64)
	if !ok {
		return merr.WrapErrServiceInternalMsg("wrong type value when setValue for Int64PrimaryKey")
	}

	ip.Value = value
	return nil
}

func (ip *Int64PrimaryKey) Type() schemapb.DataType {
	return schemapb.DataType_Int64
}

func (ip *Int64PrimaryKey) GetValue() interface{} {
	return ip.Value
}

func (ip *Int64PrimaryKey) Size() int64 {
	return 16
}

type VarCharPrimaryKey struct {
	Value string
}

func NewVarCharPrimaryKey(v string) *VarCharPrimaryKey {
	return &VarCharPrimaryKey{
		Value: strings.Clone(v),
	}
}

func (vcp *VarCharPrimaryKey) GT(key PrimaryKey) bool {
	pk, ok := key.(*VarCharPrimaryKey)
	if !ok {
		mlog.Warn(context.TODO(), "type of compared pk is not varChar")
		return false
	}

	return strings.Compare(vcp.Value, pk.Value) > 0
}

func (vcp *VarCharPrimaryKey) GE(key PrimaryKey) bool {
	pk, ok := key.(*VarCharPrimaryKey)
	if !ok {
		mlog.Warn(context.TODO(), "type of compared pk is not varChar")
		return false
	}

	return strings.Compare(vcp.Value, pk.Value) >= 0
}

func (vcp *VarCharPrimaryKey) LT(key PrimaryKey) bool {
	pk, ok := key.(*VarCharPrimaryKey)
	if !ok {
		mlog.Warn(context.TODO(), "type of compared pk is not varChar")
		return false
	}

	return strings.Compare(vcp.Value, pk.Value) < 0
}

func (vcp *VarCharPrimaryKey) LE(key PrimaryKey) bool {
	pk, ok := key.(*VarCharPrimaryKey)
	if !ok {
		mlog.Warn(context.TODO(), "type of compared pk is not varChar")
		return false
	}

	return strings.Compare(vcp.Value, pk.Value) <= 0
}

func (vcp *VarCharPrimaryKey) EQ(key PrimaryKey) bool {
	pk, ok := key.(*VarCharPrimaryKey)
	if !ok {
		mlog.Warn(context.TODO(), "type of compared pk is not varChar")
		return false
	}

	return strings.Compare(vcp.Value, pk.Value) == 0
}

func (vcp *VarCharPrimaryKey) MarshalJSON() ([]byte, error) {
	ret, err := json.Marshal(vcp.Value)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (vcp *VarCharPrimaryKey) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &vcp.Value)
	if err != nil {
		return err
	}

	return nil
}

func (vcp *VarCharPrimaryKey) SetValue(data interface{}) error {
	value, ok := data.(string)
	if !ok {
		return merr.WrapErrServiceInternalMsg("wrong type value when setValue for VarCharPrimaryKey")
	}

	vcp.Value = value
	return nil
}

func (vcp *VarCharPrimaryKey) GetValue() interface{} {
	return vcp.Value
}

func (vcp *VarCharPrimaryKey) Type() schemapb.DataType {
	return schemapb.DataType_VarChar
}

func (vcp *VarCharPrimaryKey) Size() int64 {
	return int64(len(vcp.Value) + 8)
}

type UUIDPrimaryKey struct {
	Value [16]byte
}

func NewUUIDPrimaryKey(v [16]byte) *UUIDPrimaryKey {
	return &UUIDPrimaryKey{
		Value: v,
	}
}

func NewUUIDPrimaryKeyFromString(s string) (*UUIDPrimaryKey, error) {
	u, err := typeutil.ParseUUID(s)
	if err != nil {
		return nil, err
	}
	return &UUIDPrimaryKey{
		Value: u,
	}, nil
}

func NewUUIDPrimaryKeyFromBytes(b []byte) (*UUIDPrimaryKey, error) {
	u, err := typeutil.BytesToUUID(b)
	if err != nil {
		return nil, err
	}
	return &UUIDPrimaryKey{
		Value: u,
	}, nil
}

func (up *UUIDPrimaryKey) GT(key PrimaryKey) bool {
	pk, ok := key.(*UUIDPrimaryKey)
	if !ok {
		mlog.Warn(context.TODO(), "type of compared pk is not UUID")
		return false
	}
	return bytes.Compare(up.Value[:], pk.Value[:]) > 0
}

func (up *UUIDPrimaryKey) GE(key PrimaryKey) bool {
	pk, ok := key.(*UUIDPrimaryKey)
	if !ok {
		mlog.Warn(context.TODO(), "type of compared pk is not UUID")
		return false
	}
	return bytes.Compare(up.Value[:], pk.Value[:]) >= 0
}

func (up *UUIDPrimaryKey) LT(key PrimaryKey) bool {
	pk, ok := key.(*UUIDPrimaryKey)
	if !ok {
		mlog.Warn(context.TODO(), "type of compared pk is not UUID")
		return false
	}
	return bytes.Compare(up.Value[:], pk.Value[:]) < 0
}

func (up *UUIDPrimaryKey) LE(key PrimaryKey) bool {
	pk, ok := key.(*UUIDPrimaryKey)
	if !ok {
		mlog.Warn(context.TODO(), "type of compared pk is not UUID")
		return false
	}
	return bytes.Compare(up.Value[:], pk.Value[:]) <= 0
}

func (up *UUIDPrimaryKey) EQ(key PrimaryKey) bool {
	pk, ok := key.(*UUIDPrimaryKey)
	if !ok {
		mlog.Warn(context.TODO(), "type of compared pk is not UUID")
		return false
	}
	return bytes.Equal(up.Value[:], pk.Value[:])
}

func (up *UUIDPrimaryKey) MarshalJSON() ([]byte, error) {
	str := typeutil.UUIDToString(up.Value)
	return json.Marshal(str)
}

func (up *UUIDPrimaryKey) UnmarshalJSON(data []byte) error {
	var str string
	err := json.Unmarshal(data, &str)
	if err != nil {
		return err
	}
	u, err := typeutil.ParseUUID(str)
	if err != nil {
		return err
	}
	up.Value = u
	return nil
}

func (up *UUIDPrimaryKey) SetValue(data interface{}) error {
	switch v := data.(type) {
	case [16]byte:
		up.Value = v
		return nil
	case []byte:
		u, err := typeutil.BytesToUUID(v)
		if err != nil {
			return err
		}
		up.Value = u
		return nil
	case string:
		u, err := typeutil.ParseUUID(v)
		if err != nil {
			return err
		}
		up.Value = u
		return nil
	default:
		return merr.WrapErrServiceInternalMsg("wrong type value when setValue for UUIDPrimaryKey")
	}
}

func (up *UUIDPrimaryKey) GetValue() interface{} {
	return up.Value
}

func (up *UUIDPrimaryKey) Type() schemapb.DataType {
	return schemapb.DataType_UUID
}

func (up *UUIDPrimaryKey) Size() int64 {
	return 24
}

func GenPrimaryKeyByRawData(data interface{}, pkType schemapb.DataType) (PrimaryKey, error) {
	var result PrimaryKey
	switch pkType {
	case schemapb.DataType_Int64:
		result = &Int64PrimaryKey{
			Value: data.(int64),
		}
	case schemapb.DataType_VarChar:
		result = &VarCharPrimaryKey{
			Value: data.(string),
		}
	case schemapb.DataType_UUID:
		switch v := data.(type) {
		case [16]byte:
			result = &UUIDPrimaryKey{Value: v}
		case []byte:
			u, err := typeutil.BytesToUUID(v)
			if err != nil {
				return nil, err
			}
			result = &UUIDPrimaryKey{Value: u}
		case string:
			u, err := typeutil.ParseUUID(v)
			if err != nil {
				return nil, err
			}
			result = &UUIDPrimaryKey{Value: u}
		default:
			return nil, merr.WrapErrServiceInternalMsg("unsupported raw data type for UUID primary key: %T", data)
		}
	default:
		return nil, merr.WrapErrServiceInternalMsg("not supported primary data type")
	}

	return result, nil
}

func GenInt64PrimaryKeys(data ...int64) ([]PrimaryKey, error) {
	pks := make([]PrimaryKey, len(data))
	var err error
	for i := range data {
		pks[i], err = GenPrimaryKeyByRawData(data[i], schemapb.DataType_Int64)
		if err != nil {
			return nil, err
		}
	}
	return pks, nil
}

func GenVarcharPrimaryKeys(data ...string) ([]PrimaryKey, error) {
	pks := make([]PrimaryKey, len(data))
	var err error
	for i := range data {
		pks[i], err = GenPrimaryKeyByRawData(data[i], schemapb.DataType_VarChar)
		if err != nil {
			return nil, err
		}
	}
	return pks, nil
}

func GenUUIDPrimaryKeys(data ...[16]byte) ([]PrimaryKey, error) {
	pks := make([]PrimaryKey, len(data))
	var err error
	for i := range data {
		pks[i], err = GenPrimaryKeyByRawData(data[i], schemapb.DataType_UUID)
		if err != nil {
			return nil, err
		}
	}
	return pks, nil
}

func ParseFieldData2PrimaryKeys(data *schemapb.FieldData) ([]PrimaryKey, error) {
	ret := make([]PrimaryKey, 0)
	if data == nil {
		return ret, merr.WrapErrServiceInternalMsg("failed to parse pks from nil field data")
	}
	scalarData := data.GetScalars()
	if scalarData == nil {
		return ret, merr.WrapErrServiceInternalMsg("failed to parse pks from nil scalar data")
	}

	switch data.Type {
	case schemapb.DataType_Int64:
		for _, value := range scalarData.GetLongData().GetData() {
			pk := NewInt64PrimaryKey(value)
			ret = append(ret, pk)
		}
	case schemapb.DataType_VarChar:
		for _, value := range scalarData.GetStringData().GetData() {
			pk := NewVarCharPrimaryKey(value)
			ret = append(ret, pk)
		}
	case schemapb.DataType_UUID:
		if scalarData.GetBytesData() != nil {
			for _, value := range scalarData.GetBytesData().GetData() {
				pk, err := NewUUIDPrimaryKeyFromBytes(value)
				if err != nil {
					return nil, err
				}
				ret = append(ret, pk)
			}
		} else if scalarData.GetStringData() != nil {
			for _, value := range scalarData.GetStringData().GetData() {
				pk, err := NewUUIDPrimaryKeyFromString(value)
				if err != nil {
					return nil, err
				}
				ret = append(ret, pk)
			}
		}
	default:
		return ret, merr.WrapErrServiceInternalMsg("not supported primary data type")
	}

	return ret, nil
}

func ParseIDs2PrimaryKeys(ids *schemapb.IDs) ([]PrimaryKey, error) {
	ret := make([]PrimaryKey, 0)
	switch ids.IdField.(type) {
	case *schemapb.IDs_IntId:
		int64Pks := ids.GetIntId().GetData()
		for _, v := range int64Pks {
			pk := NewInt64PrimaryKey(v)
			ret = append(ret, pk)
		}
	case *schemapb.IDs_StrId:
		stringPks := ids.GetStrId().GetData()
		for _, v := range stringPks {
			pk := NewVarCharPrimaryKey(v)
			ret = append(ret, pk)
		}
	case *schemapb.IDs_UuidId:
		uuidPks := ids.GetUuidId().GetData()
		for _, v := range uuidPks {
			pk, err := NewUUIDPrimaryKeyFromBytes(v)
			if err != nil {
				return nil, err
			}
			ret = append(ret, pk)
		}
	default:
		return nil, merr.WrapErrParameterInvalidMsg("invalid IDs type %T", ids.IdField)
	}

	return ret, nil
}

func ParseIDs2PrimaryKeysBatch(ids *schemapb.IDs) PrimaryKeys {
	var result PrimaryKeys
	switch ids.IdField.(type) {
	case *schemapb.IDs_IntId:
		int64Pks := ids.GetIntId().GetData()
		pks := NewInt64PrimaryKeys(int64(len(int64Pks)))
		pks.AppendRaw(int64Pks...)
		result = pks
	case *schemapb.IDs_StrId:
		stringPks := ids.GetStrId().GetData()
		pks := NewVarcharPrimaryKeys(int64(len(stringPks)))
		pks.AppendRaw(stringPks...)
		result = pks
	case *schemapb.IDs_UuidId:
		uuidPks := ids.GetUuidId().GetData()
		pks := NewUUIDPrimaryKeys(int64(len(uuidPks)))
		pks.AppendBytes(uuidPks...)
		result = pks
	default:
		panic(fmt.Sprintf("unexpected schema id field type %T", ids.IdField))
	}
	return result
}

func ParsePrimaryKeysBatch2IDs(pks PrimaryKeys) (*schemapb.IDs, error) {
	ret := &schemapb.IDs{}
	if pks.Len() == 0 {
		return ret, nil
	}
	switch pks.Type() {
	case schemapb.DataType_Int64:
		int64Pks := pks.(*Int64PrimaryKeys)
		ret.IdField = &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: int64Pks.values,
			},
		}
	case schemapb.DataType_VarChar:
		varcharPks := pks.(*VarcharPrimaryKeys)
		ret.IdField = &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{
				Data: varcharPks.values,
			},
		}
	case schemapb.DataType_UUID:
		uuidPks := pks.(*UUIDPrimaryKeys)
		ret.IdField = &schemapb.IDs_UuidId{
			UuidId: &schemapb.UUIDArray{
				Data: uuidPks.Bytes(),
			},
		}
	default:
		return nil, merr.WrapErrServiceInternal("parsing unsupported pk type", pks.Type().String())
	}

	return ret, nil
}

func ParsePrimaryKeys2IDs(pks []PrimaryKey) (*schemapb.IDs, error) {
	ret := &schemapb.IDs{}
	if len(pks) == 0 {
		return ret, nil
	}
	switch pks[0].Type() {
	case schemapb.DataType_Int64:
		int64Pks := make([]int64, 0)
		for _, pk := range pks {
			int64Pks = append(int64Pks, pk.(*Int64PrimaryKey).Value)
		}
		ret.IdField = &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: int64Pks,
			},
		}
	case schemapb.DataType_VarChar:
		stringPks := make([]string, 0)
		for _, pk := range pks {
			stringPks = append(stringPks, pk.(*VarCharPrimaryKey).Value)
		}
		ret.IdField = &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{
				Data: stringPks,
			},
		}
	case schemapb.DataType_UUID:
		uuidPks := make([][]byte, 0, len(pks))
		for _, pk := range pks {
			upk, ok := pk.(*UUIDPrimaryKey)
			if !ok {
				return nil, merr.WrapErrServiceInternalMsg("expected UUIDPrimaryKey, got %T", pk)
			}
			uuidPks = append(uuidPks, upk.Value[:])
		}
		ret.IdField = &schemapb.IDs_UuidId{
			UuidId: &schemapb.UUIDArray{
				Data: uuidPks,
			},
		}
	default:
		return nil, merr.WrapErrParameterInvalidMsg("invalid primary key type %v", pks[0].Type())
	}

	return ret, nil
}

func ParseInt64s2IDs(pks ...int64) *schemapb.IDs {
	ret := &schemapb.IDs{}
	if len(pks) == 0 {
		return ret
	}

	ret.IdField = &schemapb.IDs_IntId{
		IntId: &schemapb.LongArray{
			Data: pks,
		},
	}

	return ret
}
