/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hookutil

import (
	"fmt"
	"plugin"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/hook"
	"github.com/milvus-io/milvus/pkg/config"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

var (
	Hoo       hook.Hook
	Extension hook.Extension
	initOnce  sync.Once
)

func initHook() error {
	Hoo = DefaultHook{}
	Extension = DefaultExtension{}

	path := paramtable.Get().ProxyCfg.SoPath.GetValue()
	if path == "" {
		log.Info("empty so path, skip to load plugin")
		return nil
	}

	log.Info("start to load plugin", zap.String("path", path))
	p, err := plugin.Open(path)
	if err != nil {
		return fmt.Errorf("fail to open the plugin, error: %s", err.Error())
	}
	log.Info("plugin open")

	h, err := p.Lookup("MilvusHook")
	if err != nil {
		return fmt.Errorf("fail to the 'MilvusHook' object in the plugin, error: %s", err.Error())
	}

	var ok bool
	Hoo, ok = h.(hook.Hook)
	if !ok {
		return fmt.Errorf("fail to convert the `Hook` interface")
	}
	if err = Hoo.Init(paramtable.GetHookParams().SoConfig.GetValue()); err != nil {
		return fmt.Errorf("fail to init configs for the hook, error: %s", err.Error())
	}
	paramtable.GetHookParams().WatchHookWithPrefix("watch_hook", "", func(event *config.Event) {
		log.Info("receive the hook refresh event", zap.Any("event", event))
		go func() {
			soConfig := paramtable.GetHookParams().SoConfig.GetValue()
			log.Info("refresh hook configs", zap.Any("config", soConfig))
			if err = Hoo.Init(soConfig); err != nil {
				log.Panic("fail to init configs for the hook when refreshing", zap.Error(err))
			}
		}()
	})

	e, err := p.Lookup("MilvusExtension")
	if err != nil {
		return fmt.Errorf("fail to the 'MilvusExtension' object in the plugin, error: %s", err.Error())
	}
	Extension, ok = e.(hook.Extension)
	if !ok {
		return fmt.Errorf("fail to convert the `Extension` interface")
	}

	return nil
}

func InitOnceHook() {
	initOnce.Do(func() {
		err := initHook()
		if err != nil {
			logFunc := log.Warn
			if paramtable.Get().CommonCfg.PanicWhenPluginFail.GetAsBool() {
				logFunc = log.Panic
			}
			logFunc("fail to init hook",
				zap.String("so_path", paramtable.Get().ProxyCfg.SoPath.GetValue()),
				zap.Error(err))
		}
	})
}
