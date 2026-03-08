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

package eventlog

import (
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	global   atomic.Pointer[globalLogger]
	sfGlobal conc.Singleflight[*globalLogger]
)

func getGlobalLogger() *globalLogger {
	if l := global.Load(); l != nil {
		return l
	}
	l, _, _ := sfGlobal.Do("evt_global_logger", func() (*globalLogger, error) {
		if l := global.Load(); l != nil {
			return l, nil
		}

		l := &globalLogger{
			level:   Level_Info,
			loggers: typeutil.NewConcurrentMap[string, Logger](),
		}
		global.Store(l)
		return l, nil
	})
	return l
}

// globalLogger implement `Logger` interface for package level util functions
type globalLogger struct {
	level   Level
	loggers *typeutil.ConcurrentMap[string, Logger]
}

// Records implements `Logger`, dispatches evt to all registered Loggers.
func (l *globalLogger) Record(evt Evt) {
	if evt.Level() < l.level {
		return
	}
	l.loggers.Range(func(_ string, subL Logger) bool {
		subL.Record(evt)
		return true
	})
}

// RecordFunc implements `Logger`, dispatches evtFn to all registered Loggers.
func (l *globalLogger) RecordFunc(lvl Level, fn func() Evt) {
	if lvl < l.level {
		return
	}
	l.loggers.Range(func(_ string, subL Logger) bool {
		subL.RecordFunc(lvl, fn)
		return true
	})
}

// Flush implements `Logger`, dispatch Flush to all registered Loggers.
func (l *globalLogger) Flush() error {
	l.loggers.Range(func(_ string, subL Logger) bool {
		subL.Flush()
		return true
	})
	return nil
}

// Register adds a logger into global loggers with provided key.
func (l *globalLogger) Register(key string, logger Logger) {
	l.loggers.GetOrInsert(key, logger)
}
