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

package log

import (
	"sync/atomic"

	"github.com/uber/jaeger-client-go/utils"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// MLogger is a wrapper type of zap.Logger.
type MLogger struct {
	*zap.Logger
	rl atomic.Value // *utils.ReconfigurableRateLimiter
}

// With encapsulates zap.Logger With method to return MLogger instance.
func (l *MLogger) With(fields ...zap.Field) *MLogger {
	nl := &MLogger{
		Logger: l.Logger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
			return NewLazyWith(core, fields)
		})),
	}
	return nl
}

// WithRateGroup uses named RateLimiter for this logger.
func (l *MLogger) WithRateGroup(groupName string, creditPerSecond, maxBalance float64) *MLogger {
	rl := utils.NewRateLimiter(creditPerSecond, maxBalance)
	actual, loaded := _namedRateLimiters.LoadOrStore(groupName, rl)
	if loaded {
		rl.Update(creditPerSecond, maxBalance)
		rl = actual.(*utils.ReconfigurableRateLimiter)
	}
	l.rl.Store(rl)
	return l
}

func (l *MLogger) r() utils.RateLimiter {
	val := l.rl.Load()
	if l.rl.Load() == nil {
		return R()
	}
	return val.(*utils.ReconfigurableRateLimiter)
}

// RatedDebug calls log.Debug with RateLimiter.
func (l *MLogger) RatedDebug(cost float64, msg string, fields ...zap.Field) bool {
	if l.r().CheckCredit(cost) {
		l.WithOptions(zap.AddCallerSkip(1)).Debug(msg, fields...)
		return true
	}
	return false
}

// RatedInfo calls log.Info with RateLimiter.
func (l *MLogger) RatedInfo(cost float64, msg string, fields ...zap.Field) bool {
	if l.r().CheckCredit(cost) {
		l.WithOptions(zap.AddCallerSkip(1)).Info(msg, fields...)
		return true
	}
	return false
}

// RatedWarn calls log.Warn with RateLimiter.
func (l *MLogger) RatedWarn(cost float64, msg string, fields ...zap.Field) bool {
	if l.r().CheckCredit(cost) {
		l.WithOptions(zap.AddCallerSkip(1)).Warn(msg, fields...)
		return true
	}
	return false
}
