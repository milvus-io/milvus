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
	"math"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/proto/planpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
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

func MaxScalar(val1 ScalarFieldValue, val2 ScalarFieldValue) ScalarFieldValue {
	if val1.GE(val2) {
		return val1
	}
	return val2
}

func MinScalar(val1 ScalarFieldValue, val2 ScalarFieldValue) ScalarFieldValue {
	if (val1).LE(val2) {
		return val1
	}
	return val2
}

// DataType_Int8
type Int8FieldValue struct {
	Value int8 `json:"value"`
}

func NewInt8FieldValue(v int8) *Int8FieldValue {
	return &Int8FieldValue{
		Value: v,
	}
}

func (ifv *Int8FieldValue) GT(obj ScalarFieldValue) bool {
	v, ok := obj.(*Int8FieldValue)
	if !ok {
		log.Warn("type of compared pk is not int64")
		return false
	}
	if ifv.Value > v.Value {
		return true
	}

	return false
}

func (ifv *Int8FieldValue) GE(obj ScalarFieldValue) bool {
	v, ok := obj.(*Int8FieldValue)
	if !ok {
		log.Warn("type of compared pk is not int64")
		return false
	}
	if ifv.Value >= v.Value {
		return true
	}

	return false
}

func (ifv *Int8FieldValue) LT(obj ScalarFieldValue) bool {
	v, ok := obj.(*Int8FieldValue)
	if !ok {
		log.Warn("type of compared pk is not int64")
		return false
	}

	if ifv.Value < v.Value {
		return true
	}

	return false
}

func (ifv *Int8FieldValue) LE(obj ScalarFieldValue) bool {
	v, ok := obj.(*Int8FieldValue)
	if !ok {
		log.Warn("type of compared obj is not int64")
		return false
	}

	if ifv.Value <= v.Value {
		return true
	}

	return false
}

func (ifv *Int8FieldValue) EQ(obj ScalarFieldValue) bool {
	v, ok := obj.(*Int8FieldValue)
	if !ok {
		log.Warn("type of compared obj is not int64")
		return false
	}

	if ifv.Value == v.Value {
		return true
	}

	return false
}

func (ifv *Int8FieldValue) MarshalJSON() ([]byte, error) {
	ret, err := json.Marshal(ifv.Value)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (ifv *Int8FieldValue) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &ifv.Value)
	if err != nil {
		return err
	}

	return nil
}

func (ifv *Int8FieldValue) SetValue(data interface{}) error {
	value, ok := data.(int8)
	if !ok {
		log.Warn("wrong type value when setValue for Int64FieldValue")
		return fmt.Errorf("wrong type value when setValue for Int64FieldValue")
	}

	ifv.Value = value
	return nil
}

func (ifv *Int8FieldValue) Type() schemapb.DataType {
	return schemapb.DataType_Int8
}

func (ifv *Int8FieldValue) GetValue() interface{} {
	return ifv.Value
}

func (ifv *Int8FieldValue) Size() int64 {
	return 2
}

// DataType_Int16
type Int16FieldValue struct {
	Value int16 `json:"value"`
}

func NewInt16FieldValue(v int16) *Int16FieldValue {
	return &Int16FieldValue{
		Value: v,
	}
}

func (ifv *Int16FieldValue) GT(obj ScalarFieldValue) bool {
	v, ok := obj.(*Int16FieldValue)
	if !ok {
		log.Warn("type of compared pk is not int64")
		return false
	}
	if ifv.Value > v.Value {
		return true
	}

	return false
}

func (ifv *Int16FieldValue) GE(obj ScalarFieldValue) bool {
	v, ok := obj.(*Int16FieldValue)
	if !ok {
		log.Warn("type of compared pk is not int64")
		return false
	}
	if ifv.Value >= v.Value {
		return true
	}

	return false
}

func (ifv *Int16FieldValue) LT(obj ScalarFieldValue) bool {
	v, ok := obj.(*Int16FieldValue)
	if !ok {
		log.Warn("type of compared pk is not int64")
		return false
	}

	if ifv.Value < v.Value {
		return true
	}

	return false
}

func (ifv *Int16FieldValue) LE(obj ScalarFieldValue) bool {
	v, ok := obj.(*Int16FieldValue)
	if !ok {
		log.Warn("type of compared obj is not int64")
		return false
	}

	if ifv.Value <= v.Value {
		return true
	}

	return false
}

func (ifv *Int16FieldValue) EQ(obj ScalarFieldValue) bool {
	v, ok := obj.(*Int16FieldValue)
	if !ok {
		log.Warn("type of compared obj is not int64")
		return false
	}

	if ifv.Value == v.Value {
		return true
	}

	return false
}

func (ifv *Int16FieldValue) MarshalJSON() ([]byte, error) {
	ret, err := json.Marshal(ifv.Value)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (ifv *Int16FieldValue) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &ifv.Value)
	if err != nil {
		return err
	}

	return nil
}

func (ifv *Int16FieldValue) SetValue(data interface{}) error {
	value, ok := data.(int16)
	if !ok {
		log.Warn("wrong type value when setValue for Int64FieldValue")
		return fmt.Errorf("wrong type value when setValue for Int64FieldValue")
	}

	ifv.Value = value
	return nil
}

func (ifv *Int16FieldValue) Type() schemapb.DataType {
	return schemapb.DataType_Int16
}

func (ifv *Int16FieldValue) GetValue() interface{} {
	return ifv.Value
}

func (ifv *Int16FieldValue) Size() int64 {
	return 4
}

// DataType_Int32
type Int32FieldValue struct {
	Value int32 `json:"value"`
}

func NewInt32FieldValue(v int32) *Int32FieldValue {
	return &Int32FieldValue{
		Value: v,
	}
}

func (ifv *Int32FieldValue) GT(obj ScalarFieldValue) bool {
	v, ok := obj.(*Int32FieldValue)
	if !ok {
		log.Warn("type of compared pk is not int64")
		return false
	}
	if ifv.Value > v.Value {
		return true
	}

	return false
}

func (ifv *Int32FieldValue) GE(obj ScalarFieldValue) bool {
	v, ok := obj.(*Int32FieldValue)
	if !ok {
		log.Warn("type of compared pk is not int64")
		return false
	}
	if ifv.Value >= v.Value {
		return true
	}

	return false
}

func (ifv *Int32FieldValue) LT(obj ScalarFieldValue) bool {
	v, ok := obj.(*Int32FieldValue)
	if !ok {
		log.Warn("type of compared pk is not int64")
		return false
	}

	if ifv.Value < v.Value {
		return true
	}

	return false
}

func (ifv *Int32FieldValue) LE(obj ScalarFieldValue) bool {
	v, ok := obj.(*Int32FieldValue)
	if !ok {
		log.Warn("type of compared obj is not int64")
		return false
	}

	if ifv.Value <= v.Value {
		return true
	}

	return false
}

func (ifv *Int32FieldValue) EQ(obj ScalarFieldValue) bool {
	v, ok := obj.(*Int32FieldValue)
	if !ok {
		log.Warn("type of compared obj is not int64")
		return false
	}

	if ifv.Value == v.Value {
		return true
	}

	return false
}

func (ifv *Int32FieldValue) MarshalJSON() ([]byte, error) {
	ret, err := json.Marshal(ifv.Value)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (ifv *Int32FieldValue) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &ifv.Value)
	if err != nil {
		return err
	}

	return nil
}

func (ifv *Int32FieldValue) SetValue(data interface{}) error {
	value, ok := data.(int32)
	if !ok {
		log.Warn("wrong type value when setValue for Int64FieldValue")
		return fmt.Errorf("wrong type value when setValue for Int64FieldValue")
	}

	ifv.Value = value
	return nil
}

func (ifv *Int32FieldValue) Type() schemapb.DataType {
	return schemapb.DataType_Int32
}

func (ifv *Int32FieldValue) GetValue() interface{} {
	return ifv.Value
}

func (ifv *Int32FieldValue) Size() int64 {
	return 8
}

// DataType_Int64
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

// DataType_Float
type FloatFieldValue struct {
	Value float32 `json:"value"`
}

func NewFloatFieldValue(v float32) *FloatFieldValue {
	return &FloatFieldValue{
		Value: v,
	}
}

func (ifv *FloatFieldValue) GT(obj ScalarFieldValue) bool {
	v, ok := obj.(*FloatFieldValue)
	if !ok {
		log.Warn("type of compared pk is not int64")
		return false
	}
	if ifv.Value > v.Value {
		return true
	}

	return false
}

func (ifv *FloatFieldValue) GE(obj ScalarFieldValue) bool {
	v, ok := obj.(*FloatFieldValue)
	if !ok {
		log.Warn("type of compared pk is not int64")
		return false
	}
	if ifv.Value >= v.Value {
		return true
	}

	return false
}

func (ifv *FloatFieldValue) LT(obj ScalarFieldValue) bool {
	v, ok := obj.(*FloatFieldValue)
	if !ok {
		log.Warn("type of compared pk is not int64")
		return false
	}

	if ifv.Value < v.Value {
		return true
	}

	return false
}

func (ifv *FloatFieldValue) LE(obj ScalarFieldValue) bool {
	v, ok := obj.(*FloatFieldValue)
	if !ok {
		log.Warn("type of compared obj is not int64")
		return false
	}

	if ifv.Value <= v.Value {
		return true
	}

	return false
}

func (ifv *FloatFieldValue) EQ(obj ScalarFieldValue) bool {
	v, ok := obj.(*FloatFieldValue)
	if !ok {
		log.Warn("type of compared obj is not int64")
		return false
	}

	if ifv.Value == v.Value {
		return true
	}

	return false
}

func (ifv *FloatFieldValue) MarshalJSON() ([]byte, error) {
	ret, err := json.Marshal(ifv.Value)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (ifv *FloatFieldValue) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &ifv.Value)
	if err != nil {
		return err
	}

	return nil
}

func (ifv *FloatFieldValue) SetValue(data interface{}) error {
	value, ok := data.(float32)
	if !ok {
		log.Warn("wrong type value when setValue for FloatFieldValue")
		return fmt.Errorf("wrong type value when setValue for FloatFieldValue")
	}

	ifv.Value = value
	return nil
}

func (ifv *FloatFieldValue) Type() schemapb.DataType {
	return schemapb.DataType_Float
}

func (ifv *FloatFieldValue) GetValue() interface{} {
	return ifv.Value
}

func (ifv *FloatFieldValue) Size() int64 {
	return 8
}

// DataType_Double
type DoubleFieldValue struct {
	Value float64 `json:"value"`
}

func NewDoubleFieldValue(v float64) *DoubleFieldValue {
	return &DoubleFieldValue{
		Value: v,
	}
}

func (ifv *DoubleFieldValue) GT(obj ScalarFieldValue) bool {
	v, ok := obj.(*DoubleFieldValue)
	if !ok {
		log.Warn("type of compared pk is not int64")
		return false
	}
	if ifv.Value > v.Value {
		return true
	}

	return false
}

func (ifv *DoubleFieldValue) GE(obj ScalarFieldValue) bool {
	v, ok := obj.(*DoubleFieldValue)
	if !ok {
		log.Warn("type of compared pk is not int64")
		return false
	}
	if ifv.Value >= v.Value {
		return true
	}

	return false
}

func (ifv *DoubleFieldValue) LT(obj ScalarFieldValue) bool {
	v, ok := obj.(*DoubleFieldValue)
	if !ok {
		log.Warn("type of compared pk is not int64")
		return false
	}

	if ifv.Value < v.Value {
		return true
	}

	return false
}

func (ifv *DoubleFieldValue) LE(obj ScalarFieldValue) bool {
	v, ok := obj.(*DoubleFieldValue)
	if !ok {
		log.Warn("type of compared obj is not int64")
		return false
	}

	if ifv.Value <= v.Value {
		return true
	}

	return false
}

func (ifv *DoubleFieldValue) EQ(obj ScalarFieldValue) bool {
	v, ok := obj.(*DoubleFieldValue)
	if !ok {
		log.Warn("type of compared obj is not int64")
		return false
	}

	if ifv.Value == v.Value {
		return true
	}

	return false
}

func (ifv *DoubleFieldValue) MarshalJSON() ([]byte, error) {
	ret, err := json.Marshal(ifv.Value)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (ifv *DoubleFieldValue) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &ifv.Value)
	if err != nil {
		return err
	}

	return nil
}

func (ifv *DoubleFieldValue) SetValue(data interface{}) error {
	value, ok := data.(float64)
	if !ok {
		log.Warn("wrong type value when setValue for DoubleFieldValue")
		return fmt.Errorf("wrong type value when setValue for DoubleFieldValue")
	}

	ifv.Value = value
	return nil
}

func (ifv *DoubleFieldValue) Type() schemapb.DataType {
	return schemapb.DataType_Double
}

func (ifv *DoubleFieldValue) GetValue() interface{} {
	return ifv.Value
}

func (ifv *DoubleFieldValue) Size() int64 {
	return 16
}

type StringFieldValue struct {
	Value string `json:"value"`
}

func NewStringFieldValue(v string) *StringFieldValue {
	return &StringFieldValue{
		Value: v,
	}
}

func (sfv *StringFieldValue) GT(obj ScalarFieldValue) bool {
	v, ok := obj.(*StringFieldValue)
	if !ok {
		log.Warn("type of compared obj is not varchar")
		return false
	}

	return strings.Compare(sfv.Value, v.Value) > 0
}

func (sfv *StringFieldValue) GE(obj ScalarFieldValue) bool {
	v, ok := obj.(*StringFieldValue)
	if !ok {
		log.Warn("type of compared obj is not varchar")
		return false
	}
	return strings.Compare(sfv.Value, v.Value) >= 0
}

func (sfv *StringFieldValue) LT(obj ScalarFieldValue) bool {
	v, ok := obj.(*StringFieldValue)
	if !ok {
		log.Warn("type of compared obj is not varchar")
		return false
	}
	return strings.Compare(sfv.Value, v.Value) < 0
}

func (sfv *StringFieldValue) LE(obj ScalarFieldValue) bool {
	v, ok := obj.(*StringFieldValue)
	if !ok {
		log.Warn("type of compared obj is not varchar")
		return false
	}
	return strings.Compare(sfv.Value, v.Value) <= 0
}

func (sfv *StringFieldValue) EQ(obj ScalarFieldValue) bool {
	v, ok := obj.(*StringFieldValue)
	if !ok {
		log.Warn("type of compared obj is not varchar")
		return false
	}
	return strings.Compare(sfv.Value, v.Value) == 0
}

func (sfv *StringFieldValue) MarshalJSON() ([]byte, error) {
	ret, err := json.Marshal(sfv.Value)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (sfv *StringFieldValue) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &sfv.Value)
	if err != nil {
		return err
	}

	return nil
}

func (sfv *StringFieldValue) SetValue(data interface{}) error {
	value, ok := data.(string)
	if !ok {
		return fmt.Errorf("wrong type value when setValue for StringFieldValue")
	}

	sfv.Value = value
	return nil
}

func (sfv *StringFieldValue) GetValue() interface{} {
	return sfv.Value
}

func (sfv *StringFieldValue) Type() schemapb.DataType {
	return schemapb.DataType_String
}

func (sfv *StringFieldValue) Size() int64 {
	return int64(8*len(sfv.Value) + 8)
}

type VarCharFieldValue struct {
	Value string `json:"value"`
}

func NewVarCharFieldValue(v string) *VarCharFieldValue {
	return &VarCharFieldValue{
		Value: v,
	}
}

func (vcfv *VarCharFieldValue) GT(obj ScalarFieldValue) bool {
	v, ok := obj.(*VarCharFieldValue)
	if !ok {
		log.Warn("type of compared obj is not varchar")
		return false
	}

	return strings.Compare(vcfv.Value, v.Value) > 0
}

func (vcfv *VarCharFieldValue) GE(obj ScalarFieldValue) bool {
	v, ok := obj.(*VarCharFieldValue)
	if !ok {
		log.Warn("type of compared obj is not varchar")
		return false
	}
	return strings.Compare(vcfv.Value, v.Value) >= 0
}

func (vcfv *VarCharFieldValue) LT(obj ScalarFieldValue) bool {
	v, ok := obj.(*VarCharFieldValue)
	if !ok {
		log.Warn("type of compared obj is not varchar")
		return false
	}
	return strings.Compare(vcfv.Value, v.Value) < 0
}

func (vcfv *VarCharFieldValue) LE(obj ScalarFieldValue) bool {
	v, ok := obj.(*VarCharFieldValue)
	if !ok {
		log.Warn("type of compared obj is not varchar")
		return false
	}
	return strings.Compare(vcfv.Value, v.Value) <= 0
}

func (vcfv *VarCharFieldValue) EQ(obj ScalarFieldValue) bool {
	v, ok := obj.(*VarCharFieldValue)
	if !ok {
		log.Warn("type of compared obj is not varchar")
		return false
	}
	return strings.Compare(vcfv.Value, v.Value) == 0
}

func (vcfv *VarCharFieldValue) SetValue(data interface{}) error {
	value, ok := data.(string)
	if !ok {
		return fmt.Errorf("wrong type value when setValue for StringFieldValue")
	}

	vcfv.Value = value
	return nil
}

func (vcfv *VarCharFieldValue) GetValue() interface{} {
	return vcfv.Value
}

func (vcfv *VarCharFieldValue) Type() schemapb.DataType {
	return schemapb.DataType_VarChar
}

func (vcfv *VarCharFieldValue) Size() int64 {
	return int64(8*len(vcfv.Value) + 8)
}

func (vcfv *VarCharFieldValue) MarshalJSON() ([]byte, error) {
	ret, err := json.Marshal(vcfv.Value)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (vcfv *VarCharFieldValue) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &vcfv.Value)
	if err != nil {
		return err
	}

	return nil
}

type VectorFieldValue interface {
	MarshalJSON() ([]byte, error)
	UnmarshalJSON(data []byte) error
	SetValue(interface{}) error
	GetValue() interface{}
	Type() schemapb.DataType
	Size() int64
}

var _ VectorFieldValue = (*FloatVectorFieldValue)(nil)

type FloatVectorFieldValue struct {
	Value []float32 `json:"value"`
}

func NewFloatVectorFieldValue(v []float32) *FloatVectorFieldValue {
	return &FloatVectorFieldValue{
		Value: v,
	}
}

func (ifv *FloatVectorFieldValue) MarshalJSON() ([]byte, error) {
	ret, err := json.Marshal(ifv.Value)
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (ifv *FloatVectorFieldValue) UnmarshalJSON(data []byte) error {
	err := json.Unmarshal(data, &ifv.Value)
	if err != nil {
		return err
	}

	return nil
}

func (ifv *FloatVectorFieldValue) SetValue(data interface{}) error {
	value, ok := data.([]float32)
	if !ok {
		log.Warn("wrong type value when setValue for FloatVectorFieldValue")
		return fmt.Errorf("wrong type value when setValue for FloatVectorFieldValue")
	}

	ifv.Value = value
	return nil
}

func (ifv *FloatVectorFieldValue) Type() schemapb.DataType {
	return schemapb.DataType_FloatVector
}

func (ifv *FloatVectorFieldValue) GetValue() interface{} {
	return ifv.Value
}

func (ifv *FloatVectorFieldValue) Size() int64 {
	return int64(len(ifv.Value) * 8)
}

func NewScalarFieldValueFromGenericValue(dtype schemapb.DataType, gVal *planpb.GenericValue) (ScalarFieldValue, error) {
	switch dtype {
	case schemapb.DataType_Int8:
		i64Val := gVal.Val.(*planpb.GenericValue_Int64Val)
		if i64Val.Int64Val > math.MaxInt8 || i64Val.Int64Val < math.MinInt8 {
			return nil, merr.WrapErrParameterInvalidRange(math.MinInt8, math.MaxInt8, i64Val.Int64Val, "expr value out of bound")
		}
		return NewInt8FieldValue(int8(i64Val.Int64Val)), nil

	case schemapb.DataType_Int16:
		i64Val := gVal.Val.(*planpb.GenericValue_Int64Val)
		if i64Val.Int64Val > math.MaxInt16 || i64Val.Int64Val < math.MinInt16 {
			return nil, merr.WrapErrParameterInvalidRange(math.MinInt16, math.MaxInt16, i64Val.Int64Val, "expr value out of bound")
		}
		return NewInt16FieldValue(int16(i64Val.Int64Val)), nil

	case schemapb.DataType_Int32:
		i64Val := gVal.Val.(*planpb.GenericValue_Int64Val)
		if i64Val.Int64Val > math.MaxInt32 || i64Val.Int64Val < math.MinInt32 {
			return nil, merr.WrapErrParameterInvalidRange(math.MinInt32, math.MaxInt32, i64Val.Int64Val, "expr value out of bound")
		}
		return NewInt32FieldValue(int32(i64Val.Int64Val)), nil
	case schemapb.DataType_Int64:
		i64Val := gVal.Val.(*planpb.GenericValue_Int64Val)
		return NewInt64FieldValue(i64Val.Int64Val), nil
	case schemapb.DataType_Float:
		floatVal := gVal.Val.(*planpb.GenericValue_FloatVal)
		return NewFloatFieldValue(float32(floatVal.FloatVal)), nil
	case schemapb.DataType_Double:
		floatVal := gVal.Val.(*planpb.GenericValue_FloatVal)
		return NewDoubleFieldValue(floatVal.FloatVal), nil
	case schemapb.DataType_String:
		strVal := gVal.Val.(*planpb.GenericValue_StringVal)
		return NewStringFieldValue(strVal.StringVal), nil
	case schemapb.DataType_VarChar:
		strVal := gVal.Val.(*planpb.GenericValue_StringVal)
		return NewVarCharFieldValue(strVal.StringVal), nil
	default:
		// should not be reach
		panic(fmt.Sprintf("not supported datatype: %s", dtype.String()))
	}
}

func NewScalarFieldValue(dtype schemapb.DataType, data interface{}) ScalarFieldValue {
	switch dtype {
	case schemapb.DataType_Int8:
		return NewInt8FieldValue(data.(int8))
	case schemapb.DataType_Int16:
		return NewInt16FieldValue(data.(int16))
	case schemapb.DataType_Int32:
		return NewInt32FieldValue(data.(int32))
	case schemapb.DataType_Int64:
		return NewInt64FieldValue(data.(int64))
	case schemapb.DataType_Float:
		return NewFloatFieldValue(data.(float32))
	case schemapb.DataType_Double:
		return NewDoubleFieldValue(data.(float64))
	case schemapb.DataType_String:
		return NewStringFieldValue(data.(string))
	case schemapb.DataType_VarChar:
		return NewVarCharFieldValue(data.(string))
	default:
		// should not be reach
		panic(fmt.Sprintf("not supported datatype: %s", dtype.String()))
	}
}

func NewVectorFieldValue(dtype schemapb.DataType, data *schemapb.VectorField) VectorFieldValue {
	switch dtype {
	case schemapb.DataType_FloatVector:
		return NewFloatVectorFieldValue(data.GetFloatVector().GetData())
	default:
		// should not be reach
		panic(fmt.Sprintf("not supported datatype: %s", dtype.String()))
	}
}
