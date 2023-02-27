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

package accesslog

import (
	"context"
	"fmt"
	"path"
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"google.golang.org/grpc"
)

const (
	clientRequestIDKey = "client_request_id"
)

var _globalL, _globalW atomic.Value
var once sync.Once

func A() *zap.Logger {
	return _globalL.Load().(*zap.Logger)
}
func W() *RotateLogger {
	return _globalW.Load().(*RotateLogger)
}

func SetupAccseeLog(logCfg *paramtable.AccessLogConfig, minioCfg *paramtable.MinioConfig) {
	once.Do(func() {
		_, err := InitAccessLogger(logCfg, minioCfg)
		if err != nil {
			log.Fatal("initialize access logger error", zap.Error(err))
		}
	})
}

// InitAccessLogger initializes a zap access logger for proxy
func InitAccessLogger(logCfg *paramtable.AccessLogConfig, minioCfg *paramtable.MinioConfig) (*RotateLogger, error) {
	var lg *RotateLogger
	var err error
	if !logCfg.Enable.GetAsBool() {
		return nil, nil
	}

	var writeSyncer zapcore.WriteSyncer
	if len(logCfg.Filename.GetValue()) > 0 {
		lg, err = NewRotateLogger(logCfg, minioCfg)
		if err != nil {
			return nil, err
		}

		writeSyncer = zapcore.AddSync(lg)
	} else {
		stdout, _, err := zap.Open([]string{"stdout"}...)
		if err != nil {
			return nil, err
		}

		writeSyncer = stdout
	}

	encoder := NewAccessEncoder()

	logger := zap.New(zapcore.NewCore(encoder, writeSyncer, zapcore.DebugLevel))
	logger.Info("Access log start successful")

	_globalL.Store(logger)
	_globalW.Store(lg)
	return lg, nil
}

func NewAccessEncoder() zapcore.Encoder {
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "ts",
		NameKey:        "logger",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeTime:     log.DefaultTimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
	}
	return log.NewTextEncoder(&encoderConfig, false, false)
}

func PrintAccessInfo(ctx context.Context, resp interface{}, err error, rpcInfo *grpc.UnaryServerInfo, timeCost int64) bool {
	if _globalL.Load() == nil {
		return false
	}

	fields := []zap.Field{
		//format time cost of task
		zap.String("timeCost", fmt.Sprintf("%d ms", timeCost)),
	}

	//get trace ID of task
	traceID, ok := getTraceID(ctx)
	if !ok {
		log.Warn("access log print failed: could not get trace ID")
		return false
	}
	fields = append(fields, zap.String("traceId", traceID))

	//get response size of task
	responseSize, ok := getResponseSize(resp)
	if !ok {
		log.Warn("access log print failed: could not get response size")
		return false
	}
	fields = append(fields, zap.Int("responseSize", responseSize))

	//get err code of task
	errCode, ok := getErrCode(resp)
	if !ok {
		// unknown error code
		errCode = -1
	}
	fields = append(fields, zap.Int("errorCode", errCode))

	//get status of grpc
	Status := getGrpcStatus(err)
	if Status == "OK" && errCode > 0 {
		Status = "TaskFailed"
	}

	//get method name of grpc
	_, methodName := path.Split(rpcInfo.FullMethod)

	A().Info(fmt.Sprintf("%v: %s-%s", Status, getAccessAddr(ctx), methodName), fields...)
	return true
}

func Rotate() error {
	err := W().Rotate()
	return err
}
