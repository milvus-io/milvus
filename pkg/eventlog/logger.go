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

// Logger is the interface for event loggers.
type Logger interface {
	// Record append log into logger directly.
	Record(Evt)
	// RecordFunc performs log level check & other implementation related check before composing the log entity
	// preferred to use this when in performance related path
	RecordFunc(Level, func() Evt)
	// Flush is the API to invoke flush operation for logger (if any).
	Flush() error
}

// Evt is event log interface.
type Evt interface {
	Level() Level
	Type() int32
	Raw() []byte
}

// Record is the global helper function to `globalLogger.Record`.
func Record(evt Evt) {
	getGlobalLogger().Record(evt)
}

// RecordFunc is the global helper function to `globalLogger.RecordFunc`.
func RecordFunc(lvl Level, fn func() Evt) {
	getGlobalLogger().RecordFunc(lvl, fn)
}

// Flush is the global helper function to `globalLogger.Flush`.
func Flush() error {
	return getGlobalLogger().Flush()
}

func Register(key string, logger Logger) {
	getGlobalLogger().Register(key, logger)
}
