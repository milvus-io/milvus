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
	"io"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

const (
	clientRequestIDKey = "client_request_id"
)

var (
	_globalW io.Writer
	_globalR *RotateLogger
	_globalF *FormatterManger
	once     sync.Once
)

func InitAccessLog(logCfg *paramtable.AccessLogConfig, minioCfg *paramtable.MinioConfig) {
	once.Do(func() {
		err := initAccessLogger(logCfg, minioCfg)
		if err != nil {
			log.Fatal("initialize access logger error", zap.Error(err))
		}
		log.Info("Init access log success")
	})
}

func initFormatter(logCfg *paramtable.AccessLogConfig) error {
	formatterManger := NewFormatterManger()
	formatMap := make(map[string]string)   // fommatter name -> formatter format
	methodMap := make(map[string][]string) // fommatter name -> formatter owner method
	for key, value := range logCfg.Formatter.GetValue() {
		formatterName, option, err := parseConfigKey(key)
		if err != nil {
			return err
		}

		if option == fomaterkey {
			formatMap[formatterName] = value
		} else if option == methodKey {
			methodMap[formatterName] = paramtable.ParseAsStings(value)
		}
	}

	for name, format := range formatMap {
		formatterManger.Add(name, format)
		if methods, ok := methodMap[name]; ok {
			formatterManger.SetMethod(name, methods...)
		}
	}

	_globalF = formatterManger
	return nil
}

// initAccessLogger initializes a zap access logger for proxy
func initAccessLogger(logCfg *paramtable.AccessLogConfig, minioCfg *paramtable.MinioConfig) error {
	var lg *RotateLogger
	var err error
	if !logCfg.Enable.GetAsBool() {
		return nil
	}

	err = initFormatter(logCfg)
	if err != nil {
		return err
	}

	if len(logCfg.Filename.GetValue()) > 0 {
		lg, err = NewRotateLogger(logCfg, minioCfg)
		if err != nil {
			return err
		}

		if logCfg.CacheSize.GetAsInt() > 0 {
			blg := NewCacheLogger(lg, logCfg.CacheSize.GetAsInt())
			_globalW = zapcore.AddSync(blg)
		} else {
			_globalW = zapcore.AddSync(lg)
		}
	} else {
		stdout, _, err := zap.Open([]string{"stdout"}...)
		if err != nil {
			return err
		}

		_globalW = stdout
	}
	_globalR = lg
	return nil
}

func Rotate() error {
	if _globalR == nil {
		return nil
	}
	err := _globalR.Rotate()
	return err
}
