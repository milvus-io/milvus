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

import "time"

const (
	HighPriority   = 1
	NormalPriority = HighPriority + 10
	LowPriority    = NormalPriority + 10
)

type Source interface {
	GetConfigurations() (map[string]string, error)
	GetConfigurationByKey(string) (string, error)
	GetPriority() int
	GetSourceName() string
	Close()
}

// EventHandler handles config change event
type EventHandler interface {
	OnEvent(event *Event)
}

// EtcdInfo has attribute for config center source initialization
type EtcdInfo struct {
	Endpoints []string
	KeyPrefix string

	RefreshMode int
	//Pull Configuration interval, unit is second
	RefreshInterval time.Duration
}

//Options hold options
type Options struct {
	File            *string
	EtcdInfo        *EtcdInfo
	EnvKeyFormatter func(string) string
}

//Option is a func
type Option func(options *Options)

//WithRequiredFiles tell archaius to manage files, if not exist will return error
func WithFilesSource(f string) Option {
	return func(options *Options) {
		options.File = &f
	}
}

//WithEtcdSource accept the information for initiating a remote source
func WithEtcdSource(ri *EtcdInfo) Option {
	return func(options *Options) {
		options.EtcdInfo = ri
	}
}

//WithEnvSource enable env source
//archaius will read ENV as key value
func WithEnvSource(keyFormatter func(string) string) Option {
	return func(options *Options) {
		options.EnvKeyFormatter = keyFormatter
	}
}
