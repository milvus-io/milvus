// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.
package paramtable

import (
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/internal/config"
)

type ParamItem struct {
	Key          string // which should be named as "A.B.C"
	Version      string
	Doc          string
	DefaultValue string
	PanicIfEmpty bool

	Formatter func(originValue string) string

	manager *config.Manager
}

func (pi *ParamItem) Init(manager *config.Manager) {
	pi.manager = manager
}

// Get original value with error
func (pi *ParamItem) get() (string, error) {
	ret, err := pi.manager.GetConfig(pi.Key)
	if err != nil {
		ret = pi.DefaultValue
	}
	if pi.Formatter == nil {
		return ret, err
	}
	return pi.Formatter(ret), err
}

func (pi *ParamItem) GetValue() string {
	v, _ := pi.get()
	return v
}

func (pi *ParamItem) GetAsStrings() []string {
	return getAndConvert(pi, func(value string) ([]string, error) {
		return strings.Split(value, ","), nil
	}, []string{})
}

func (pi *ParamItem) GetAsBool() bool {
	return getAndConvert(pi, strconv.ParseBool, false)
}

func (pi *ParamItem) GetAsInt() int {
	return getAndConvert(pi, strconv.Atoi, 0)
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

	GetFunc func() map[string]string

	manager *config.Manager
}

func (pg *ParamGroup) Init(manager *config.Manager) {
	pg.manager = manager
}

func (pg *ParamGroup) GetValue() map[string]string {
	if pg.GetFunc != nil {
		return pg.GetFunc()
	}
	values := pg.manager.GetConfigsByPattern(pg.KeyPrefix, false)
	return values
}

func getAndConvert[T any](pi *ParamItem, converter func(input string) (T, error), defaultValue T) T {
	v, _ := pi.get()
	t, err := converter(v)
	if err != nil {
		return defaultValue
	}
	return t
}
