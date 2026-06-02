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

package logutil

import (
	"context"
	"math"
	"sync"

	"golang.org/x/exp/constraints"
	"google.golang.org/grpc/grpclog"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// LogPanic logs the panic reason and stack, then exit the process.
// Commonly used with a `defer`.
func LogPanic() {
	if e := recover(); e != nil {
		mlog.Fatal(context.TODO(), "panic", mlog.Reflect("recover", e))
	}
}

var once sync.Once

// SetupLogger is used to initialize the log with config.
func SetupLogger(cfg *mlog.Config) {
	once.Do(func() {
		// Initialize logger.
		logger, p, err := mlog.InitLogger(cfg, mlog.AddStacktrace(mlog.ErrorLevel))
		if err == nil {
			mlog.ReplaceGlobals(logger, p)
		} else {
			mlog.Fatal(context.TODO(), "initialize logger error", mlog.Err(err))
		}

		grpclog.SetLoggerV2(mlog.NewGRPCLogger(logger, cfg.GrpcLevel))

		mlog.Info(context.TODO(), "Log directory", mlog.String("configDir", cfg.File.RootPath))
		mlog.Info(context.TODO(), "Set log file to ", mlog.String("path", cfg.File.Filename))
	})
}

func WithModule(ctx context.Context, module string) context.Context {
	return mlog.WithModule(ctx, module)
}

// keeps only 2 decimal places
func ToMB[T constraints.Integer | constraints.Float](mem T) T {
	return T(math.Round(float64(mem)/1024/1024*100) / 100)
}
