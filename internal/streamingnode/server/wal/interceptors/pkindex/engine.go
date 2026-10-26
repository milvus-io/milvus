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

package pkindex

import (
	"sync"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/pkindex/engine"
)

// errNoEngineFactory is returned when no engine factory is registered.
// It is a plain identity sentinel, not a merr error, so that errors.Is matches
// only this error and not every merr service-internal error.
var errNoEngineFactory = errors.New("no primary key index engine factory is registered")

// Engine is the engine of one vchannel. The interceptor creates it with the
// target of the vchannel, the Decider reads it, and the interceptor closes it
// after the Decider.
//
// TODO: replace the test-only memory engine with the persistent engine.
type Engine interface {
	engine.RW
	Close() error
}

// EngineFactory creates the engine of one vchannel.
type EngineFactory func(vchannel string) (Engine, error)

var (
	engineFactoryMu sync.RWMutex
	engineFactory   EngineFactory
)

// RegisterEngineFactory sets the process-wide engine factory. Passing nil removes it.
func RegisterEngineFactory(f EngineFactory) {
	engineFactoryMu.Lock()
	defer engineFactoryMu.Unlock()
	engineFactory = f
}

// HasEngineFactory reports whether an engine factory is registered.
func HasEngineFactory() bool {
	engineFactoryMu.RLock()
	defer engineFactoryMu.RUnlock()
	return engineFactory != nil
}

// newEngine creates the engine of a vchannel with the registered engine factory.
func newEngine(vchannel string) (Engine, error) {
	engineFactoryMu.RLock()
	factory := engineFactory
	engineFactoryMu.RUnlock()
	if factory == nil {
		return nil, errNoEngineFactory
	}
	eng, err := factory(vchannel)
	if err != nil {
		return nil, errors.Wrapf(err, "create primary key index engine of vchannel %s", vchannel)
	}
	return eng, nil
}
