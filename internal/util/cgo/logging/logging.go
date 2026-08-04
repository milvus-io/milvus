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
#include <stdbool.h>
#include <stdint.h>

extern void goZapLogExt(int severity,
            char* file,
            int file_len,
            int line,
            char* msg,
            int msg_len);
extern bool goZapLogTantivy(int severity,
            char* target,
            uintptr_t target_len,
            char* file,
            uintptr_t file_len,
            uint32_t line,
            char* msg,
            uintptr_t msg_len);
*/
import "C"

import (
	"time"
	"unsafe"

	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

const (
	cgoLoggerName     = "CGO"
	tantivyLoggerName = "Tantivy"
)

const (
	glogInfo    glogSeverity = 0
	glogWarning glogSeverity = 1
	glogError   glogSeverity = 2
	glogFatal   glogSeverity = 3
)

// glogSeverity describes the GLOG severity level.
type glogSeverity = int

const (
	tantivyTrace tantivySeverity = iota
	tantivyDebug
	tantivyInfo
	tantivyWarn
	tantivyError
)

type tantivySeverity = int

//export goZapLogExt
func goZapLogExt(sev C.int,
	file *C.char,
	fileLen C.int,
	line C.int,
	msg *C.char,
	msgLen C.int,
) {
	lv := mapGlogSeverity(int(sev))
	if !mlog.L().Core().Enabled(lv) {
		return
	}
	core := mlog.L().Core()
	if c, ok := core.(mlog.CEntryTextIOCore); ok {
		// if async log is enabled, we use CEntry to write the log, avoid to copy the log message to the heap.
		c.WriteWithCEntry(mlog.CEntry{
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
	if ce := mlog.L().Core().Check(ent, nil); ce != nil {
		ce.Write()
	}
}

func mapGlogSeverity(s int) mlog.Level {
	switch s {
	case glogInfo: // GLOG_INFO
		return mlog.InfoLevel
	case glogWarning: // GLOG_WARNING
		return mlog.WarnLevel
	case glogError: // GLOG_ERROR
		return mlog.ErrorLevel
	case glogFatal: // GLOG_FATAL
		// glog fatal will call std::abort,
		// zap will call os.Exit(1),
		// we don't want to double exit, so we use error level instead
		return mlog.ErrorLevel
	default:
		return mlog.InfoLevel
	}
}

//export goZapLogTantivy
func goZapLogTantivy(sev C.int,
	target *C.char,
	targetLen C.uintptr_t,
	file *C.char,
	fileLen C.uintptr_t,
	line C.uint32_t,
	msg *C.char,
	msgLen C.uintptr_t,
) C.bool {
	logTantivyRecord(
		int(sev),
		copyCString(target, targetLen),
		copyCString(file, fileLen),
		int(line),
		copyCString(msg, msgLen),
	)
	return C.bool(true)
}

func logTantivyRecord(severity int, target, file string, line int, message string) {
	lv := mapTantivySeverity(severity)
	core := mlog.L().Core()
	if !core.Enabled(lv) {
		return
	}

	loggerName := tantivyLoggerName
	if target != "" {
		loggerName += "/" + target
	}
	ent := zapcore.Entry{
		Level:      lv,
		Time:       time.Now(),
		LoggerName: loggerName,
		Message:    message,
		Caller: zapcore.EntryCaller{
			Defined: file != "",
			File:    file,
			Line:    line,
		},
	}
	if ce := core.Check(ent, nil); ce != nil {
		ce.Write()
	}
}

func copyCString(ptr *C.char, length C.uintptr_t) string {
	if length == 0 {
		return ""
	}
	// A zap Core may retain the Entry after Write returns. Copy the FFI memory
	// so logger implementations never observe a dangling Rust pointer.
	return string(unsafe.Slice((*byte)(unsafe.Pointer(ptr)), int(length)))
}

func mapTantivySeverity(s int) mlog.Level {
	switch s {
	case tantivyTrace, tantivyDebug:
		// mlog intentionally treats the configured trace level as debug.
		return mlog.DebugLevel
	case tantivyInfo:
		return mlog.InfoLevel
	case tantivyWarn:
		return mlog.WarnLevel
	case tantivyError:
		return mlog.ErrorLevel
	default:
		return mlog.InfoLevel
	}
}
