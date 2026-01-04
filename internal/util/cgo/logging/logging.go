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

package logging

/*
extern void goZapLogExt(int severity,
            char* file,
            int line,
            char* msg,
            int msg_len);
*/
import "C"

import (
	"time"

	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
)

const cgoLoggerName = "CGO"

//export goZapLogExt
func goZapLogExt(sev C.int,
	file *C.char,
	line C.int,
	msg *C.char,
	msgLen C.int,
) {
	lv := mapGlogSeverity(int(sev))
	if !log.L().Core().Enabled(lv) {
		return
	}
	ent := zapcore.Entry{
		Level:      mapGlogSeverity(int(sev)),
		Time:       time.Now(),
		LoggerName: cgoLoggerName,
		Message:    C.GoStringN(msg, msgLen),
		Caller: zapcore.EntryCaller{
			Defined: true,
			File:    C.GoString(file),
			Line:    int(line),
		},
	}
	if ce := log.L().Core().Check(ent, nil); ce != nil {
		metrics.LoggingCGOWriteTotal.Inc()
		metrics.LoggingCGOWriteBytes.Add(float64(msgLen))
		ce.Write()
	}
}

func mapGlogSeverity(s int) zapcore.Level {
	switch s {
	case 0: // GLOG_INFO
		return zapcore.InfoLevel
	case 1: // GLOG_WARNING
		return zapcore.WarnLevel
	case 2: // GLOG_ERROR
		return zapcore.ErrorLevel
	case 3: // GLOG_FATAL
		// glog fatal will call std::abort,
		// zap will call os.Exit(1),
		// we don't want to double exit, so we use error level instead
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}
