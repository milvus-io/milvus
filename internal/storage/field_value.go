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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/log"
)

type ScalarFieldValue interface {
	GT(key ScalarFieldValue) bool
	GE(key ScalarFieldValue) bool
	LT(key ScalarFieldValue) bool
	LE(key ScalarFieldValue) bool
	EQ(key ScalarFieldValue) bool
	MarshalJSON() ([]byte, error)
	UnmarshalJSON(data []byte) error
	SetValue(interface{}) error
	GetValue() interface{}
	Type() schemapb.DataType
	Size() int64
}

type Int64FieldValue struct {
	Value int64 `json:"value"`
}

func NewInt64FieldValue(v int64) *Int64FieldValue {
	return &Int64FieldValue{
		Value: v,
	}
}

func (ifv *Int64FieldValue) GT(obj ScalarFieldValue) bool {
	v, ok := obj.(*Int64FieldValue)
	if !ok {
		log.Warn("type of compared pk is not int64")
		return false
	}
	if ifv.Value > v.Value {
		return true
	}

	return false
}

func (ifv *Int64FieldValue) GE(obj ScalarFieldValue) bool {
	v, ok := obj.(*Int64FieldValue)
	if !ok {
		log.Warn("type of compared pk is not int64")
		return false
	}
	if ifv.Value >= v.Value {
		return true
	}

	return false
}

func (ifv *Int64FieldValue) LT(obj ScalarFieldValue) bool {
	v, ok := obj.(*Int64FieldValue)
	if !ok {
		log.Warn("type of compared pk is not int64")
		return false
	}

	if ifv.Value < v.Value {
		return true
	}

	return false
}

func (ifv *Int64FieldValue) LE(obj ScalarFieldValue) bool {
	v, ok := obj.(*Int64FieldValue)
	if !ok {
		log.Warn("type of compared obj is not int64")
		return false
	}

	if ifv.Value <= v.Value {
		return true
	}

	return false
}

func (ifv *Int64FieldValue) EQ(obj ScalarFieldValue) bool {
	v, ok := obj.(*Int64FieldValue)
	if !ok {
		log.Warn("type of compared obj is not int64")
		return false
	}

	if ifv.Value == v.Value {
		return true
	}

	return false
}

func (ifv *Int64FieldValue) MarshalJSON() ([]byte, error) {
	ret, err := json.Marshal(ifv.Value)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (ifv *Int64FieldValue) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &ifv.Value)
	if err != nil {
		return err
	}

	return nil
}

func (ifv *Int64FieldValue) SetValue(data interface{}) error {
	value, ok := data.(int64)
	if !ok {
		log.Warn("wrong type value when setValue for Int64FieldValue")
		return fmt.Errorf("wrong type value when setValue for Int64FieldValue")
	}

	ifv.Value = value
	return nil
}

func (ifv *Int64FieldValue) Type() schemapb.DataType {
	return schemapb.DataType_Int64
}

func (ifv *Int64FieldValue) GetValue() interface{} {
	return ifv.Value
}

func (ifv *Int64FieldValue) Size() int64 {
	// 8 + reflect.ValueOf(Int64FieldValue).Type().Size()
	return 16
}

type BaseStringFieldValue struct {
	Value string
}

func (sfv *BaseStringFieldValue) GT(obj BaseStringFieldValue) bool {
	return strings.Compare(sfv.Value, obj.Value) > 0
}

func (sfv *BaseStringFieldValue) GE(obj BaseStringFieldValue) bool {
	return strings.Compare(sfv.Value, obj.Value) >= 0
}

func (sfv *BaseStringFieldValue) LT(obj BaseStringFieldValue) bool {
	return strings.Compare(sfv.Value, obj.Value) < 0
}

func (sfv *BaseStringFieldValue) LE(obj BaseStringFieldValue) bool {
	return strings.Compare(sfv.Value, obj.Value) <= 0
}

func (sfv *BaseStringFieldValue) EQ(obj BaseStringFieldValue) bool {
	return strings.Compare(sfv.Value, obj.Value) == 0
}

func (sfv *BaseStringFieldValue) MarshalJSON() ([]byte, error) {
	ret, err := json.Marshal(sfv.Value)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (sfv *BaseStringFieldValue) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &sfv.Value)
	if err != nil {
		return err
	}

	return nil
}

func (sfv *BaseStringFieldValue) SetValue(data interface{}) error {
	value, ok := data.(string)
	if !ok {
		return fmt.Errorf("wrong type value when setValue for StringFieldValue")
	}

	sfv.Value = value
	return nil
}

func (sfv *BaseStringFieldValue) GetValue() interface{} {
	return sfv.Value
}

type VarCharFieldValue struct {
	BaseStringFieldValue
}

func NewVarCharFieldValue(v string) *VarCharFieldValue {
	return &VarCharFieldValue{
		BaseStringFieldValue: BaseStringFieldValue{
			Value: v,
		},
	}
}

func (vcfv *VarCharFieldValue) GT(obj ScalarFieldValue) bool {
	v, ok := obj.(*VarCharFieldValue)
	if !ok {
		log.Warn("type of compared obj is not varChar")
		return false
	}

	return vcfv.BaseStringFieldValue.GT(v.BaseStringFieldValue)
}

func (vcfv *VarCharFieldValue) GE(obj ScalarFieldValue) bool {
	v, ok := obj.(*VarCharFieldValue)
	if !ok {
		log.Warn("type of compared obj is not varChar")
		return false
	}

	return vcfv.BaseStringFieldValue.GE(v.BaseStringFieldValue)
}

func (vcfv *VarCharFieldValue) LT(obj ScalarFieldValue) bool {
	v, ok := obj.(*VarCharFieldValue)
	if !ok {
		log.Warn("type of compared pk is not varChar")
		return false
	}

	return vcfv.BaseStringFieldValue.LT(v.BaseStringFieldValue)
}

func (vcfv *VarCharFieldValue) LE(obj ScalarFieldValue) bool {
	v, ok := obj.(*VarCharFieldValue)
	if !ok {
		log.Warn("type of compared pk is not varChar")
		return false
	}

	return vcfv.BaseStringFieldValue.LE(v.BaseStringFieldValue)
}

func (vcfv *VarCharFieldValue) EQ(obj ScalarFieldValue) bool {
	v, ok := obj.(*VarCharFieldValue)
	if !ok {
		log.Warn("type of compared obj is not varChar")
		return false
	}

	return vcfv.BaseStringFieldValue.EQ(v.BaseStringFieldValue)
}

func (vcfv *VarCharFieldValue) Type() schemapb.DataType {
	return schemapb.DataType_VarChar
}

func (vcfv *VarCharFieldValue) Size() int64 {
	return int64(8*len(vcfv.Value) + 8)
}

func (vcfv *VarCharFieldValue) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &vcfv.Value)
	if err != nil {
		return err
	}

	return nil
}

func GenFieldValueByRawData(data interface{}, dtype schemapb.DataType) (ScalarFieldValue, error) {
	var result ScalarFieldValue
	switch dtype {
	case schemapb.DataType_Int64:
		result = &Int64FieldValue{
			Value: data.(int64),
		}
	case schemapb.DataType_VarChar:
		result = &VarCharFieldValue{
			BaseStringFieldValue: BaseStringFieldValue{
				Value: data.(string),
			},
		}
	default:
		return nil, fmt.Errorf("not supported data type")
	}

	return result, nil
}

func GenInt64FieldValues(data ...int64) ([]ScalarFieldValue, error) {
	values := make([]ScalarFieldValue, len(data))
	var err error
	for i := range data {
		values[i], err = GenFieldValueByRawData(data[i], schemapb.DataType_Int64)
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}

func GenVarcharFieldValues(data ...string) ([]ScalarFieldValue, error) {
	values := make([]ScalarFieldValue, len(data))
	var err error
	for i := range data {
		values[i], err = GenFieldValueByRawData(data[i], schemapb.DataType_VarChar)
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}

func ParseFieldData2FieldValues(data *schemapb.FieldData) ([]ScalarFieldValue, error) {
	ret := make([]ScalarFieldValue, 0)
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
			fv := NewInt64FieldValue(value)
			ret = append(ret, fv)
		}
	case schemapb.DataType_VarChar:
		for _, value := range scalarData.GetStringData().GetData() {
			fv := NewVarCharFieldValue(value)
			ret = append(ret, fv)
		}
	default:
		return ret, fmt.Errorf("not supported data type")
	}

	return ret, nil
}

func ParseIDs2FieldValues(ids *schemapb.IDs) []ScalarFieldValue {
	ret := make([]ScalarFieldValue, 0)
	switch ids.IdField.(type) {
	case *schemapb.IDs_IntId:
		int64Values := ids.GetIntId().GetData()
		for _, v := range int64Values {
			pk := NewInt64FieldValue(v)
			ret = append(ret, pk)
		}
	case *schemapb.IDs_StrId:
		stringValues := ids.GetStrId().GetData()
		for _, v := range stringValues {
			pk := NewVarCharFieldValue(v)
			ret = append(ret, pk)
		}
	default:
		// TODO::
	}

	return ret
}

func ParseFieldValues2IDs(pks []ScalarFieldValue) *schemapb.IDs {
	ret := &schemapb.IDs{}
	if len(pks) == 0 {
		return ret
	}
	switch pks[0].Type() {
	case schemapb.DataType_Int64:
		int64Values := make([]int64, 0)
		for _, pk := range pks {
			int64Values = append(int64Values, pk.(*Int64FieldValue).Value)
		}
		ret.IdField = &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: int64Values,
			},
		}
	case schemapb.DataType_VarChar:
		stringValues := make([]string, 0)
		for _, pk := range pks {
			stringValues = append(stringValues, pk.(*VarCharFieldValue).Value)
		}
		ret.IdField = &schemapb.IDs_StrId{
			StrId: &schemapb.StringArray{
				Data: stringValues,
			},
		}
	default:
		// TODO::
	}

	return ret
}
