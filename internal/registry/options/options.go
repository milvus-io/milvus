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

package options

type SessionOpt struct {
	StandBy     bool
	Exclusive   bool
	TriggerKill bool
}

func DefaultSessionOpt() SessionOpt {
	return SessionOpt{
		StandBy:     false,
		Exclusive:   false,
		TriggerKill: true,
	}
}

// RegisterOption used to setup session options when register services.
type RegisterOption func(*SessionOpt)

// WithStandBy enables stand-by feature.
func WithStandBy() RegisterOption {
	return func(opt *SessionOpt) {
		opt.StandBy = true
	}
}

func WithTriggerKill(v bool) RegisterOption {
	return func(opt *SessionOpt) {
		opt.TriggerKill = v
	}
}

type watchOpt struct {
}

// WatchOption used to setup watch services options.
type WatchOption func(*watchOpt)
