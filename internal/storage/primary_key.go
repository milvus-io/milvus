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
	"encoding/json"
	"fmt"
	"strings"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
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
		log.Warn("type of compared pk is not int64")
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
		log.Warn("type of compared pk is not int64")
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
		log.Warn("type of compared pk is not int64")
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
		log.Warn("type of compared pk is not int64")
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
		log.Warn("type of compared pk is not int64")
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
		return fmt.Errorf("wrong type value when setValue for Int64PrimaryKey")
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

type BaseStringPrimaryKey struct {
	Value string
}

func (sp *BaseStringPrimaryKey) GT(key BaseStringPrimaryKey) bool {
	return strings.Compare(sp.Value, key.Value) > 0
}

func (sp *BaseStringPrimaryKey) GE(key BaseStringPrimaryKey) bool {
	return strings.Compare(sp.Value, key.Value) >= 0
}

func (sp *BaseStringPrimaryKey) LT(key BaseStringPrimaryKey) bool {
	return strings.Compare(sp.Value, key.Value) < 0
}

func (sp *BaseStringPrimaryKey) LE(key BaseStringPrimaryKey) bool {
	return strings.Compare(sp.Value, key.Value) <= 0
}

func (sp *BaseStringPrimaryKey) EQ(key BaseStringPrimaryKey) bool {
	return strings.Compare(sp.Value, key.Value) == 0
}

func (sp *BaseStringPrimaryKey) MarshalJSON() ([]byte, error) {
	ret, err := json.Marshal(sp.Value)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (sp *BaseStringPrimaryKey) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &sp.Value)
	if err != nil {
		return err
	}

	return nil
}

func (sp *BaseStringPrimaryKey) SetValue(data interface{}) error {
	value, ok := data.(string)
	if !ok {
		return fmt.Errorf("wrong type value when setValue for StringPrimaryKey")
	}

	sp.Value = value
	return nil
}

func (sp *BaseStringPrimaryKey) GetValue() interface{} {
	return sp.Value
}

type VarCharPrimaryKey struct {
	BaseStringPrimaryKey
}

func NewVarCharPrimaryKey(v string) *VarCharPrimaryKey {
	return &VarCharPrimaryKey{
		BaseStringPrimaryKey: BaseStringPrimaryKey{
			Value: v,
		},
	}
}

func (vcp *VarCharPrimaryKey) GT(key PrimaryKey) bool {
	pk, ok := key.(*VarCharPrimaryKey)
	if !ok {
		log.Warn("type of compared pk is not varChar")
		return false
	}

	return vcp.BaseStringPrimaryKey.GT(pk.BaseStringPrimaryKey)
}

func (vcp *VarCharPrimaryKey) GE(key PrimaryKey) bool {
	pk, ok := key.(*VarCharPrimaryKey)
	if !ok {
		log.Warn("type of compared pk is not varChar")
		return false
	}

	return vcp.BaseStringPrimaryKey.GE(pk.BaseStringPrimaryKey)
}

func (vcp *VarCharPrimaryKey) LT(key PrimaryKey) bool {
	pk, ok := key.(*VarCharPrimaryKey)
	if !ok {
		log.Warn("type of compared pk is not varChar")
		return false
	}

	return vcp.BaseStringPrimaryKey.LT(pk.BaseStringPrimaryKey)
}

func (vcp *VarCharPrimaryKey) LE(key PrimaryKey) bool {
	pk, ok := key.(*VarCharPrimaryKey)
	if !ok {
		log.Warn("type of compared pk is not varChar")
		return false
	}

	return vcp.BaseStringPrimaryKey.LE(pk.BaseStringPrimaryKey)
}

func (vcp *VarCharPrimaryKey) EQ(key PrimaryKey) bool {
	pk, ok := key.(*VarCharPrimaryKey)
	if !ok {
		log.Warn("type of compared pk is not varChar")
		return false
	}

	return vcp.BaseStringPrimaryKey.EQ(pk.BaseStringPrimaryKey)
}

func (vcp *VarCharPrimaryKey) Type() schemapb.DataType {
	return schemapb.DataType_VarChar
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
			BaseStringPrimaryKey: BaseStringPrimaryKey{
				Value: data.(string),
			},
		}
	default:
		return nil, fmt.Errorf("not supported primary data type")
	}

	return result, nil
}

func ParseFieldData2PrimaryKeys(data *schemapb.FieldData) ([]PrimaryKey, error) {
	ret := make([]PrimaryKey, 0)
	if data == nil {
		return ret, fmt.Errorf("failed to parse pks from nil field data")
	}
	scalarData := data.GetScalars()
	if scalarData == nil {
		return ret, fmt.Errorf("failed to parse pks from nil scalar data")
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
	default:
		return ret, fmt.Errorf("not supported primary data type")
	}

	return ret, nil
}

func ParseIDs2PrimaryKeys(ids *schemapb.IDs) []PrimaryKey {
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
	default:
		//TODO::
	}

	return ret
}

func ParsePrimaryKeys2IDs(pks []PrimaryKey) *schemapb.IDs {
	ret := &schemapb.IDs{}
	if len(pks) == 0 {
		return ret
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
	default:
		//TODO::
	}

	return ret
}
