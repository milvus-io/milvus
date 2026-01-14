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
            int file_len,
            int line,
            char* msg,
            int msg_len);
*/
import "C"

import (
	"time"
	"unsafe"

	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

const cgoLoggerName = "CGO"

const (
	glogInfo    glogSeverity = 0
	glogWarning glogSeverity = 1
	glogError   glogSeverity = 2
	glogFatal   glogSeverity = 3
)

// glogSeverity describes the GLOG severity level.
type glogSeverity = int

//export goZapLogExt
func goZapLogExt(sev C.int,
	file *C.char,
	fileLen C.int,
	line C.int,
	msg *C.char,
	msgLen C.int,
) {
	lv := mapGlogSeverity(int(sev))
	if !log.L().Core().Enabled(lv) {
		return
	}
	core := log.L().Core()
	if c, ok := core.(log.CEntryTextIOCore); ok {
		// if async log is enabled, we use CEntry to write the log, avoid to copy the log message to the heap.
		c.WriteWithCEntry(log.CEntry{
			Time:        time.Now(),
			Level:       lv,
			Filename:    unsafe.Pointer(file),
			FilenameLen: int(fileLen),
			Line:        int(line),
			Message:     unsafe.Pointer(msg),
			MessageLen:  int(msgLen),
		})
		return
	}

	// Currently, milvus will enable async log by default, so following code is never executed.
	// otherwise, we perform a synchronous write, Write directly to the underlying buffered write syncer.
	b := unsafe.Slice((*byte)(unsafe.Pointer(msg)), int(msgLen))
	msgStr := unsafe.String(&b[0], len(b))
	ent := zapcore.Entry{
		Level:      lv,
		Time:       time.Now(),
		LoggerName: cgoLoggerName,
		Message:    msgStr,
		Caller: zapcore.EntryCaller{
			Defined: true,
			File:    C.GoString(file),
			Line:    int(line),
		},
	}
	if ce := log.L().Core().Check(ent, nil); ce != nil {
		ce.Write()
	}
}

func mapGlogSeverity(s int) zapcore.Level {
	switch s {
	case glogInfo: // GLOG_INFO
		return zapcore.InfoLevel
	case glogWarning: // GLOG_WARNING
		return zapcore.WarnLevel
	case glogError: // GLOG_ERROR
		return zapcore.ErrorLevel
	case glogFatal: // GLOG_FATAL
		// glog fatal will call std::abort,
		// zap will call os.Exit(1),
		// we don't want to double exit, so we use error level instead
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}
