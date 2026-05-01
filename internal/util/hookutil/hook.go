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
	"sync"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus/pkg/v3/config"
	"github.com/milvus-io/milvus/pkg/v3/log"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

type hookSetter interface {
	SetZapLogger(*zap.Logger)
	SetClientInfoProvider(any)
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

	hookVal, err := LoadPlugin[hook.Hook](path, "MilvusHook")
	if err != nil {
		return err
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

	extVal, err := LoadPlugin[hook.Extension](path, "MilvusExtension")
	if err != nil {
		return err
	}
	storeExtension(extVal)

	return nil
}

func SetHook(connectionManager any) {
	hookVal := GetHook()
	if setter, ok := hookVal.(hookSetter); ok {
		setter.SetZapLogger(log.L())
		setter.SetClientInfoProvider(connectionManager)
		log.Info("hook setter injected")
	}
}

func InitOnceHook() {
	initOnce.Do(func() {
		err := initHook()
		if err != nil {
			soPath := paramtable.Get().ProxyCfg.SoPath.GetValue()
			if paramtable.Get().CommonCfg.PanicWhenPluginFail.GetAsBool() {
				log.Panic(fmt.Sprintf("fail to init hook, so_path=%s, error=%v", soPath, err))
			}
			log.Warn("fail to init hook", zap.String("so_path", soPath), zap.Error(err))
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
