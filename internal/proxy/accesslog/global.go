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
	"strconv"
	"sync"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proxy/accesslog/info"
	configEvent "github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var (
	_globalL *AccessLogger
	once     sync.Once
)

type AccessLogger struct {
	enable     atomic.Bool
	writer     io.Writer
	formatters *FormatterManger
	mu         sync.RWMutex
}

func NewAccessLogger() *AccessLogger {
	return &AccessLogger{}
}

func (l *AccessLogger) init(params *paramtable.ComponentParam) error {
	formatters, err := initFormatter(&params.ProxyCfg.AccessLog)
	if err != nil {
		return err
	}
	l.formatters = formatters

	writer, err := initWriter(&params.ProxyCfg.AccessLog, &params.MinioCfg)
	if err != nil {
		return err
	}
	l.writer = writer
	return nil
}

func (l *AccessLogger) Init(params *paramtable.ComponentParam) error {
	if params.ProxyCfg.AccessLog.Enable.GetAsBool() {
		l.mu.Lock()
		defer l.mu.Unlock()

		err := l.init(params)
		if err != nil {
			return err
		}
		l.enable.Store(true)
	}
	return nil
}

func (l *AccessLogger) SetEnable(enable bool) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.enable.Load() == enable {
		return nil
	}

	if enable {
		log.Info("start enable access log")
		params := paramtable.Get()
		err := l.init(params)
		if err != nil {
			log.Warn("enable access log failed", zap.Error(err))
			return err
		}
	} else {
		log.Info("start close access log")
		if write, ok := l.writer.(*RotateWriter); ok {
			write.Close()
		}
	}

	l.enable.Store(enable)
	return nil
}

func (l *AccessLogger) Write(info info.AccessInfo) bool {
	if !l.enable.Load() {
		return false
	}

	l.mu.RLock()
	defer l.mu.RUnlock()

	method := info.MethodName()
	formatter, ok := l.formatters.GetByMethod(method)
	if !ok {
		return false
	}
	_, err := l.writer.Write([]byte(formatter.Format(info)))
	if err != nil {
		log.Warn("write access log failed", zap.Error(err))
		return false
	}
	return true
}

func InitAccessLogger(params *paramtable.ComponentParam) {
	once.Do(func() {
		logger := NewAccessLogger()
		// support dynamic param
		params.Watch(params.ProxyCfg.AccessLog.Enable.Key, configEvent.NewHandler("enable accesslog", func(event *configEvent.Event) {
			value, err := strconv.ParseBool(event.Value)
			if err != nil {
				log.Warn("Failed to parse bool value", zap.String("v", event.Value), zap.Error(err))
				return
			}
			logger.SetEnable(value)
		}))

		err := logger.Init(params)
		if err != nil {
			log.Warn("Init access logger failed", zap.Error(err))
		}
		_globalL = logger
		info.ClusterPrefix.Store(params.CommonCfg.ClusterPrefix.GetValue())
		log.Info("Init access logger success")
	})
}

func initFormatter(logCfg *paramtable.AccessLogConfig) (*FormatterManger, error) {
	formatterManger := NewFormatterManger()
	formatMap := make(map[string]string)   // fommatter name -> formatter format
	methodMap := make(map[string][]string) // fommatter name -> formatter owner method
	for key, value := range logCfg.Formatter.GetValue() {
		formatterName, option, err := parseConfigKey(key)
		if err != nil {
			return nil, err
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

	return formatterManger, nil
}

// initAccessLogger initializes a zap access logger for proxy
func initWriter(logCfg *paramtable.AccessLogConfig, minioCfg *paramtable.MinioConfig) (io.Writer, error) {
	if len(logCfg.Filename.GetValue()) > 0 {
		lg, err := NewRotateWriter(logCfg, minioCfg)
		if err != nil {
			return nil, err
		}

		if logCfg.CacheSize.GetAsInt() > 0 {
			clg := NewCacheWriterWithCloser(lg, lg, logCfg.CacheSize.GetAsInt(), logCfg.CacheFlushInterval.GetAsDuration(time.Second))
			return clg, nil
		}
		return lg, nil
	}

	// wirte to stdout when filename = ""
	stdout, _, err := zap.Open([]string{"stdout"}...)
	if err != nil {
		return nil, err
	}

	if logCfg.CacheSize.GetAsInt() > 0 {
		lg := NewCacheWriter(stdout, logCfg.CacheSize.GetAsInt(), logCfg.CacheFlushInterval.GetAsDuration(time.Second))
		return lg, nil
	}

	return stdout, nil
}
