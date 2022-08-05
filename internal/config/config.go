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

package config

import (
	"errors"
	"strings"
)

var (
	ErrNotInitial   = errors.New("config is not initialized")
	ErrIgnoreChange = errors.New("ignore change")
	ErrKeyNotFound  = errors.New("key not found")
)

func Init(opts ...Option) (*Manager, error) {
	o := &Options{}
	for _, opt := range opts {
		opt(o)
	}
	sourceManager := NewManager()
	if o.File != nil {
		sourceManager.AddSource(NewFileSource(*o.File))
	}
	if o.EnvKeyFormatter != nil {
		sourceManager.AddSource(NewEnvSource(o.EnvKeyFormatter))
	}
	if o.EtcdInfo != nil {
		s, err := NewEtcdSource(o.EtcdInfo)
		if err != nil {
			return nil, err
		}
		s.eh = sourceManager
		sourceManager.AddSource(s)
	}
	return sourceManager, nil

}

func formatKey(key string) string {
	ret := strings.ToLower(key)
	ret = strings.ReplaceAll(ret, "/", "")
	ret = strings.ReplaceAll(ret, "_", "")
	ret = strings.ReplaceAll(ret, ".", "")
	return ret
}
