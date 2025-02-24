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
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/hook"
	"github.com/milvus-io/milvus/pkg/v2/config"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var (
	hoo       atomic.Value // hook.Hook
	extension atomic.Value // hook.Extension
	initOnce  sync.Once
)

// hookContainer is Container to wrap hook.Hook interface
// this struct is used to be stored in atomic.Value
// since different type stored in it will cause panicking.
type hookContainer struct {
	hook hook.Hook
}

// extensionContainer is Container to wrap hook.Extension interface
// this struct is used to be stored in atomic.Value
// since different type stored in it will cause panicking.
type extensionContainer struct {
	extension hook.Extension
}

func storeHook(hook hook.Hook) {
	hoo.Store(hookContainer{hook: hook})
}

func storeExtension(ext hook.Extension) {
	extension.Store(extensionContainer{extension: ext})
}

func initHook() error {
	// setup default hook & extension
	storeHook(DefaultHook{})
	storeExtension(DefaultExtension{})

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

	var hookVal hook.Hook
	var ok bool
	hookVal, ok = h.(hook.Hook)
	if !ok {
		return fmt.Errorf("fail to convert the `Hook` interface")
	}
	if err = hookVal.Init(paramtable.GetHookParams().SoConfig.GetValue()); err != nil {
		return fmt.Errorf("fail to init configs for the hook, error: %s", err.Error())
	}
	storeHook((hookVal))
	paramtable.GetHookParams().WatchHookWithPrefix("watch_hook", "", func(event *config.Event) {
		log.Info("receive the hook refresh event", zap.Any("event", event))
		go func() {
			hookVal := GetHook()
			soConfig := paramtable.GetHookParams().SoConfig.GetValue()
			log.Info("refresh hook configs", zap.Any("config", soConfig))
			if err = hookVal.Init(soConfig); err != nil {
				log.Panic("fail to init configs for the hook when refreshing", zap.Error(err))
			}
			storeHook(hookVal)
		}()
	})

	e, err := p.Lookup("MilvusExtension")
	if err != nil {
		return fmt.Errorf("fail to the 'MilvusExtension' object in the plugin, error: %s", err.Error())
	}
	var extVal hook.Extension
	extVal, ok = e.(hook.Extension)
	if !ok {
		return fmt.Errorf("fail to convert the `Extension` interface")
	}
	storeExtension(extVal)

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

// GetHook returns singleton hook.Hook instance.
func GetHook() hook.Hook {
	InitOnceHook()
	return hoo.Load().(hookContainer).hook
}

// GetHook returns singleton hook.Extension instance.
func GetExtension() hook.Extension {
	InitOnceHook()
	return extension.Load().(extensionContainer).extension
}
