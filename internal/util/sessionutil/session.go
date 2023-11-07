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
package sessionutil

import (
	"context"
	"time"

	"github.com/blang/semver/v4"
)

type SessionInterface interface {
	UnmarshalJSON(data []byte) error
	MarshalJSON() ([]byte, error)

	Init(serverName, address string, exclusive bool, triggerKill bool)
	String() string
	Register()

	GetSessions(prefix string) (map[string]*Session, int64, error)
	GetSessionsWithVersionRange(prefix string, r semver.Range) (map[string]*Session, int64, error)

	GoingStop() error
	WatchServices(prefix string, revision int64, rewatch Rewatch) (eventChannel <-chan *SessionEvent)
	WatchServicesWithVersionRange(prefix string, r semver.Range, revision int64, rewatch Rewatch) (eventChannel <-chan *SessionEvent)
	LivenessCheck(ctx context.Context, callback func())
	Stop()
	Revoke(timeout time.Duration)
	UpdateRegistered(b bool)
	Registered() bool
	SetDisconnected(b bool)
	Disconnected() bool
	SetEnableActiveStandBy(enable bool)
	ProcessActiveStandBy(activateFunc func() error) error
	ForceActiveStandby(activateFunc func() error) error

	GetAddress() string
	GetServerID() int64
	IsTriggerKill() bool
}
