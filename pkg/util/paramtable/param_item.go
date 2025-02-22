// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package paramtable

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/samber/lo"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

type ParamItem struct {
	Key          string // which should be named as "A.B.C"
	Version      string
	Doc          string
	DefaultValue string
	FallbackKeys []string
	PanicIfEmpty bool
	Export       bool

	Formatter func(originValue string) string
	Forbidden bool

	manager *config.Manager

	// for unittest.
	tempValue atomic.Pointer[string]
}

func (pi *ParamItem) Init(manager *config.Manager) {
	pi.manager = manager
	if pi.Forbidden {
		pi.manager.ForbidUpdate(pi.Key)
	}
}

// Get original value with error
func (pi *ParamItem) get() (string, error) {
	result, _, err := pi.getWithRaw()
	return result, err
}

func (pi *ParamItem) getWithRaw() (result, raw string, err error) {
	// For unittest.
	if s := pi.tempValue.Load(); s != nil {
		return *s, *s, nil
	}

	if pi.manager == nil {
		panic(fmt.Sprintf("manager is nil %s", pi.Key))
	}
	// raw value set only once
	_, raw, err = pi.manager.GetConfig(pi.Key)
	if err != nil || raw == pi.DefaultValue {
		// try fallback if the entry is not exist or default value,
		//  because default value may already defined in milvus.yaml
		//	and we don't want the fallback keys be overridden.
		for _, key := range pi.FallbackKeys {
			var fallbackRaw string
			_, fallbackRaw, err = pi.manager.GetConfig(key)
			if err == nil {
				raw = fallbackRaw
				break
			}
		}
	}
	if err != nil {
		// use default value
		raw = pi.DefaultValue
	}
	result = raw
	if pi.Formatter != nil {
		result = pi.Formatter(result)
	}
	if result == "" && pi.PanicIfEmpty {
		panic(fmt.Sprintf("%s is empty", pi.Key))
	}
	return result, raw, err
}

// SetTempValue set the value for this ParamItem,
// Once value set, ParamItem will use the value instead of underlying config manager.
// Usage: should only use for unittest, swap empty string will remove the value.
func (pi *ParamItem) SwapTempValue(s string) *string {
	if s == "" {
		return pi.tempValue.Swap(nil)
	}
	pi.manager.EvictCachedValue(pi.Key)
	return pi.tempValue.Swap(&s)
}

func (pi *ParamItem) GetValue() string {
	v, _ := pi.get()
	return v
}

func (pi *ParamItem) GetAsStrings() []string {
	if val, exist := pi.manager.GetCachedValue(pi.Key); exist {
		if strings, ok := val.([]string); ok {
			return strings
		}
	}
	val, raw, _ := pi.getWithRaw()
	realStrs := getAsStrings(val)
	pi.manager.CASCachedValue(pi.Key, raw, realStrs)
	return realStrs
}

func (pi *ParamItem) GetAsBool() bool {
	if val, exist := pi.manager.GetCachedValue(pi.Key); exist {
		if boolVal, ok := val.(bool); ok {
			return boolVal
		}
	}
	val, raw, _ := pi.getWithRaw()
	boolVal := getAsBool(val)
	pi.manager.CASCachedValue(pi.Key, raw, boolVal)
	return boolVal
}

func (pi *ParamItem) GetAsInt() int {
	if val, exist := pi.manager.GetCachedValue(pi.Key); exist {
		if intVal, ok := val.(int); ok {
			return intVal
		}
	}
	val, raw, _ := pi.getWithRaw()
	intVal := getAsInt(val)
	pi.manager.CASCachedValue(pi.Key, raw, intVal)
	return intVal
}

func (pi *ParamItem) GetAsInt32() int32 {
	if val, exist := pi.manager.GetCachedValue(pi.Key); exist {
		if int32Val, ok := val.(int32); ok {
			return int32Val
		}
	}
	val, raw, _ := pi.getWithRaw()
	int32Val := int32(getAsInt64(val))
	pi.manager.CASCachedValue(pi.Key, raw, int32Val)
	return int32Val
}

func (pi *ParamItem) GetAsUint() uint {
	if val, exist := pi.manager.GetCachedValue(pi.Key); exist {
		if uintVal, ok := val.(uint); ok {
			return uintVal
		}
	}
	val, raw, _ := pi.getWithRaw()
	uintVal := uint(getAsUint64(val))
	pi.manager.CASCachedValue(pi.Key, raw, uintVal)
	return uintVal
}

func (pi *ParamItem) GetAsUint32() uint32 {
	if val, exist := pi.manager.GetCachedValue(pi.Key); exist {
		if uint32Val, ok := val.(uint32); ok {
			return uint32Val
		}
	}
	val, raw, _ := pi.getWithRaw()
	uint32Val := uint32(getAsUint64(val))
	pi.manager.CASCachedValue(pi.Key, raw, uint32Val)
	return uint32Val
}

func (pi *ParamItem) GetAsUint64() uint64 {
	if val, exist := pi.manager.GetCachedValue(pi.Key); exist {
		if uint64Val, ok := val.(uint64); ok {
			return uint64Val
		}
	}
	val, raw, _ := pi.getWithRaw()
	uint64Val := getAsUint64(val)
	pi.manager.CASCachedValue(pi.Key, raw, uint64Val)
	return uint64Val
}

func (pi *ParamItem) GetAsUint16() uint16 {
	if val, exist := pi.manager.GetCachedValue(pi.Key); exist {
		if uint16Val, ok := val.(uint16); ok {
			return uint16Val
		}
	}
	val, raw, _ := pi.getWithRaw()
	uint16Val := uint16(getAsUint64(val))
	pi.manager.CASCachedValue(pi.Key, raw, uint16Val)
	return uint16Val
}

func (pi *ParamItem) GetAsInt64() int64 {
	if val, exist := pi.manager.GetCachedValue(pi.Key); exist {
		if int64Val, ok := val.(int64); ok {
			return int64Val
		}
	}
	val, raw, _ := pi.getWithRaw()
	int64Val := getAsInt64(val)
	pi.manager.CASCachedValue(pi.Key, raw, int64Val)
	return int64Val
}

func (pi *ParamItem) GetAsFloat() float64 {
	if val, exist := pi.manager.GetCachedValue(pi.Key); exist {
		if floatVal, ok := val.(float64); ok {
			return floatVal
		}
	}
	val, raw, _ := pi.getWithRaw()
	floatVal := getAsFloat(val)
	pi.manager.CASCachedValue(pi.Key, raw, floatVal)
	return floatVal
}

func (pi *ParamItem) GetAsDuration(unit time.Duration) time.Duration {
	if val, exist := pi.manager.GetCachedValue(pi.Key); exist {
		if durationVal, ok := val.(time.Duration); ok {
			return durationVal
		}
	}
	val, raw, _ := pi.getWithRaw()
	durationVal := getAsDuration(val, unit)
	pi.manager.CASCachedValue(pi.Key, raw, durationVal)
	return durationVal
}

func (pi *ParamItem) GetAsJSONMap() map[string]string {
	return getAndConvert(pi.GetValue(), funcutil.JSONToMap, nil)
}

func (pi *ParamItem) GetAsRoleDetails() map[string](map[string]([](map[string]string))) {
	return getAndConvert(pi.GetValue(), funcutil.JSONToRoleDetails, nil)
}

func (pi *ParamItem) GetAsDurationByParse() time.Duration {
	val, _ := pi.get()
	durationVal, err := time.ParseDuration(val)
	if err != nil {
		durationVal, err = time.ParseDuration(pi.DefaultValue)
		if err != nil {
			panic(fmt.Sprintf("unreachable: parse duration from default value failed, %s, err: %s", pi.DefaultValue, err.Error()))
		}
	}
	return durationVal
}

func (pi *ParamItem) GetAsSize() int64 {
	valueStr := strings.ToLower(pi.GetValue())
	if strings.HasSuffix(valueStr, "g") || strings.HasSuffix(valueStr, "gb") {
		size, err := strconv.ParseInt(strings.Split(valueStr, "g")[0], 10, 64)
		if err != nil {
			return 0
		}
		return size * 1024 * 1024 * 1024
	} else if strings.HasSuffix(valueStr, "m") || strings.HasSuffix(valueStr, "mb") {
		size, err := strconv.ParseInt(strings.Split(valueStr, "m")[0], 10, 64)
		if err != nil {
			return 0
		}
		return size * 1024 * 1024
	} else if strings.HasSuffix(valueStr, "k") || strings.HasSuffix(valueStr, "kb") {
		size, err := strconv.ParseInt(strings.Split(valueStr, "k")[0], 10, 64)
		if err != nil {
			return 0
		}
		return size * 1024
	}
	size, err := strconv.ParseInt(valueStr, 10, 64)
	if err != nil {
		return 0
	}
	return size
}

type CompositeParamItem struct {
	Items  []*ParamItem
	Format func(map[string]string) string
}

func (cpi *CompositeParamItem) GetValue() string {
	kvs := make(map[string]string, len(cpi.Items))
	for _, v := range cpi.Items {
		kvs[v.Key] = v.GetValue()
	}
	return cpi.Format(kvs)
}

type ParamGroup struct {
	KeyPrefix string // which should be named as "A.B."
	Version   string
	Doc       string
	Export    bool

	GetFunc func() map[string]string
	DocFunc func(string) string

	manager *config.Manager
}

func (pg *ParamGroup) Init(manager *config.Manager) {
	pg.manager = manager
}

func (pg *ParamGroup) GetValue() map[string]string {
	if pg.GetFunc != nil {
		return pg.GetFunc()
	}
	values := pg.manager.GetBy(config.WithPrefix(pg.KeyPrefix), config.RemovePrefix(pg.KeyPrefix))
	return values
}

func (pg *ParamGroup) GetDoc(key string) string {
	if pg.DocFunc != nil {
		return pg.DocFunc(key)
	}
	return ""
}

func ParseAsStings(v string) []string {
	return getAsStrings(v)
}

func getAsStrings(v string) []string {
	if len(v) == 0 {
		return []string{}
	}
	return getAndConvert(v, func(value string) ([]string, error) {
		ret := strings.Split(value, ",")
		return lo.Map(ret, func(rg string, _ int) string { return strings.TrimSpace(rg) }), nil
	}, []string{})
}

func getAsBool(v string) bool {
	return getAndConvert(v, strconv.ParseBool, false)
}

func getAsInt(v string) int {
	return getAndConvert(v, strconv.Atoi, 0)
}

func getAsInt64(v string) int64 {
	return getAndConvert(v, func(value string) (int64, error) {
		return strconv.ParseInt(value, 10, 64)
	}, 0)
}

func getAsUint64(v string) uint64 {
	return getAndConvert(v, func(value string) (uint64, error) {
		return strconv.ParseUint(value, 10, 64)
	}, 0)
}

func getAsFloat(v string) float64 {
	return getAndConvert(v, func(value string) (float64, error) {
		return strconv.ParseFloat(value, 64)
	}, 0.0)
}

func getAsDuration(v string, unit time.Duration) time.Duration {
	return getAndConvert(v, func(value string) (time.Duration, error) {
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			fv, err := strconv.ParseFloat(value, 64)
			return time.Duration(fv * float64(unit)), err
		}
		return time.Duration(v) * unit, err
	}, 0)
}

func getAndConvert[T any](v string, converter func(input string) (T, error), defaultValue T) T {
	t, err := converter(v)
	if err != nil {
		return defaultValue
	}
	return t
}

type RuntimeParamItem struct {
	value atomic.Value
}

func (rpi *RuntimeParamItem) GetValue() any {
	return rpi.value.Load()
}

func (rpi *RuntimeParamItem) GetAsString() string {
	value, ok := rpi.value.Load().(string)
	if !ok {
		return ""
	}
	return value
}

func (rpi *RuntimeParamItem) GetAsTime() time.Time {
	value, ok := rpi.value.Load().(time.Time)
	if !ok {
		return time.Time{}
	}
	return value
}

func (rpi *RuntimeParamItem) GetAsInt64() int64 {
	value, ok := rpi.value.Load().(int64)
	if !ok {
		return 0
	}
	return value
}

func (rpi *RuntimeParamItem) SetValue(value any) {
	rpi.value.Store(value)
}
