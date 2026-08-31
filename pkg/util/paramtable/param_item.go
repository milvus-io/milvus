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
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/blang/semver/v4"
	"github.com/samber/lo"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

type ParamChangeCallback func(ctx context.Context, key, oldValue, newValue string) error

// VersionGateSwitcher describes the "version-gated auto-switch" semantics of a
// configuration item:
//   - when the user configures the item with EnableAutoSwitchValue (the
//     sentinel), AutoSwitch is triggered;
//   - once the cluster-wide confirmed version reaches GateVersion and the
//     SwitchDelay stability window has elapsed since the confirmation, the
//     one-shot confirmator flips the config-center value to TargetValue;
//   - before that the item resolves to PreSwitchValue (the value used before
//     the switch, i.e. the pre-change behavior) on every read path.
//
// DefaultValue is allowed to equal EnableAutoSwitchValue, which means the item
// is in AutoSwitch mode by default (no explicit user configuration needed);
// the "not yet switched" state is expressed by resolving reads to
// PreSwitchValue instead of leaking the sentinel value to callers.
//
// The gate is applied at the value-resolution layer (getWithRaw), so GetValue
// and all GetAs* accessors uniformly return the effective value. The raw
// configured value is left untouched for callers that need to detect the
// sentinel (e.g. the version gate confirmator).
//
// nil means no version gating (default, backward compatible).
type VersionGateSwitcher struct {
	EnableAutoSwitchValue string        // sentinel value: configuring this value triggers AutoSwitch
	PreSwitchValue        string        // effective value while the gate is not yet activated (pre-change behavior)
	GateVersion           string        // minimum cluster version (semver) required to switch
	TargetValue           string        // effective value after AutoSwitch takes effect
	SwitchDelay           time.Duration // stability window to wait after cluster-wide confirmation before switching

	// localSatisfied is set by ComponentParam.initVersionGates for embedded-etcd
	// (single-process) deployments: the local process is the entire cluster, so
	// when the local version is already >= GateVersion there is nothing to
	// coordinate and the gate resolves directly to TargetValue. It is a pure
	// paramtable-internal hint, never part of the configurable contract.
	localSatisfied bool
}

// Validate checks the switcher's field contract, panicking on a missing or
// malformed field. A version-gated item must declare its full semantics:
//   - EnableAutoSwitchValue: the sentinel that triggers AutoSwitch;
//   - PreSwitchValue:        the pre-change behavior — without it the sentinel
//     would leak to callers, so an empty value is a coding error;
//   - GateVersion:           a valid semver (the confirmator parses it);
//   - TargetValue:           the post-switch value.
//
// Validate is called from ParamItem.Init, so a misconfigured gated item fails
// fast at startup instead of silently degrading at runtime.
func (sw *VersionGateSwitcher) Validate() {
	if sw.EnableAutoSwitchValue == "" {
		panic("version gate: EnableAutoSwitchValue must not be empty")
	}
	if sw.PreSwitchValue == "" {
		panic("version gate: PreSwitchValue must not be empty (the pre-change behavior is required)")
	}
	if sw.GateVersion == "" {
		panic("version gate: GateVersion must not be empty")
	}
	if _, err := semver.Parse(sw.GateVersion); err != nil {
		panic(fmt.Sprintf("version gate: invalid GateVersion %q: %v", sw.GateVersion, err))
	}
	if sw.TargetValue == "" {
		panic("version gate: TargetValue must not be empty")
	}
	if sw.SwitchDelay < 0 {
		panic("version gate: SwitchDelay must not be negative")
	}
}

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
	Immutable bool

	// VersionGateSwitcher attaches version-gated auto-switch semantics to this
	// item; nil means no version gating (backward compatible).
	VersionGateSwitcher *VersionGateSwitcher

	manager *config.Manager

	// for unittest.
	tempValue atomic.Pointer[string]

	callback  ParamChangeCallback
	lastValue atomic.Pointer[string]
}

func (pi *ParamItem) Init(manager *config.Manager) {
	pi.manager = manager
	if pi.VersionGateSwitcher != nil {
		// A version-gated item must declare its full semantics; a
		// misconfigured switcher is a coding error and must fail fast.
		pi.VersionGateSwitcher.Validate()
	}
	if pi.Forbidden {
		pi.manager.ForbidUpdate(pi.Key)
	}
	if pi.Immutable {
		pi.manager.ImmutableUpdate(pi.Key)
	}

	currentValue := pi.GetValue()
	pi.lastValue.Store(&currentValue)

	if manager != nil && manager.Dispatcher != nil {
		handler := config.NewHandler(pi.Key, func(event *config.Event) {
			if event.Key == strings.ToLower(pi.Key) && event.EventType == config.UpdateType {
				pi.handleConfigChange(event)
			}
		})
		manager.Dispatcher.Register(pi.Key, handler)
	}
}

func (pi *ParamItem) RegisterCallback(callback ParamChangeCallback) {
	pi.callback = callback
}

func (pi *ParamItem) UnregisterCallback() {
	pi.callback = nil
}

func (pi *ParamItem) handleConfigChange(event *config.Event) {
	if pi.callback == nil {
		return
	}

	oldValue := ""
	if lastVal := pi.lastValue.Load(); lastVal != nil {
		oldValue = *lastVal
	}

	newValue := event.Value

	if oldValue == newValue {
		return
	}

	if err := pi.callback(context.Background(), pi.Key, oldValue, newValue); err != nil {
		mlog.Error(context.TODO(), "param change callback failed",
			mlog.String("key", pi.Key),
			mlog.String("oldValue", oldValue),
			mlog.String("newValue", newValue),
			mlog.Err(err))
	} else {
		mlog.Info(context.TODO(), "param value changed",
			mlog.String("key", pi.Key),
			mlog.String("oldValue", oldValue),
			mlog.String("newValue", newValue))
	}

	pi.lastValue.Store(&newValue)
}

// Get original value with error
func (pi *ParamItem) get() (string, error) {
	result, _, err := pi.getWithRaw()
	return result, err
}

func (pi *ParamItem) getWithRaw() (result, raw string, err error) {
	// For unittest.
	if s := pi.tempValue.Load(); s != nil {
		return pi.gateValue(*s), *s, nil
	}

	if pi.manager == nil {
		panic(fmt.Sprintf("manager is nil %s", pi.Key))
	}
	// raw is always the primary key's value, used for CAS comparison.
	// effectiveRaw is the value actually used for computing result (may come from fallback).
	_, raw, err = pi.manager.GetConfig(pi.Key)
	effectiveRaw := raw
	if err != nil || raw == pi.DefaultValue {
		// try fallback if the entry is not exist or default value,
		//  because default value may already defined in milvus.yaml
		//	and we don't want the fallback keys be overridden.
		for _, key := range pi.FallbackKeys {
			var fallbackRaw string
			_, fallbackRaw, err = pi.manager.GetConfig(key)
			if err == nil {
				effectiveRaw = fallbackRaw
				break
			}
		}
	}
	if err != nil {
		// use default value
		effectiveRaw = pi.DefaultValue
		raw = pi.DefaultValue
	}
	result = pi.gateValue(effectiveRaw)
	if pi.Formatter != nil {
		result = pi.Formatter(result)
	}
	if result == "" && pi.PanicIfEmpty {
		panic(fmt.Sprintf("%s is empty", pi.Key))
	}
	return result, raw, err
}

// gateValue applies the version-gated auto-switch semantics to a configured
// value: when the item carries a VersionGateSwitcher and the value is the
// sentinel (EnableAutoSwitchValue), the effective value is TargetValue when
// the gate is locally satisfied (embedded-etcd single-process deployments
// where the local version is already >= GateVersion, see localSatisfied), and
// PreSwitchValue otherwise, until the one-shot confirmator flips the config
// center value to TargetValue. Every read path (GetValue and all GetAs*)
// resolves through this, so the gate is uniformly visible regardless of the
// caller's accessor type. The raw value is unaffected: callers that need to
// detect the sentinel (e.g. the version gate confirmator) still see it.
func (pi *ParamItem) gateValue(v string) string {
	if pi.VersionGateSwitcher == nil || v != pi.VersionGateSwitcher.EnableAutoSwitchValue {
		return v
	}
	if pi.VersionGateSwitcher.localSatisfied {
		return pi.VersionGateSwitcher.TargetValue
	}
	return pi.VersionGateSwitcher.PreSwitchValue
}

// SetTempValue set the value for this ParamItem,
// Once value set, ParamItem will use the value instead of underlying config manager.
func (pi *ParamItem) SwapTempValue(s string) string {
	if s == "" {
		if old := pi.tempValue.Swap(nil); old != nil {
			return *old
		}
		return ""
	}
	pi.manager.EvictCachedValue(pi.Key)
	if old := pi.tempValue.Swap(&s); old != nil {
		return *old
	}
	return ""
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
	if val, exist := pi.manager.GetCachedValue(pi.Key); exist {
		if durationVal, ok := val.(time.Duration); ok {
			return durationVal
		}
	}
	val, raw, _ := pi.getWithRaw()
	durationVal, err := time.ParseDuration(val)
	if err != nil {
		durationVal, err = time.ParseDuration(pi.DefaultValue)
		if err != nil {
			panic(fmt.Sprintf("unreachable: parse duration from default value failed, %s, err: %s", pi.DefaultValue, err.Error()))
		}
	}
	pi.manager.CASCachedValue(pi.Key, raw, durationVal)
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
