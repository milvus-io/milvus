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

package hookutil

import (
	"fmt"
	"plugin"
	"sync"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/hook"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var (
	Cipher         atomic.Value
	initCipherOnce sync.Once
)

// cipherContainer is Container to wrap hook.Cipher interface
// this struct is used to be stored in atomic.Value
// since different type stored in it will cause panicking.
type cipherContainer struct {
	cipher hook.Cipher
}

func storeCipher(cipher hook.Cipher) {
	Cipher.Store(cipherContainer{cipher: cipher})
}

func initCipher() error {
	storeCipher(DefaultCipher{})

	path := paramtable.GetCipherParams().SoPathGo.GetValue()
	if path == "" {
		log.Info("empty so path, skip to load cipher plugin")
		return nil
	}

	log.Info("start to load cipher plugin", zap.String("path", path))
	p, err := plugin.Open(path)
	if err != nil {
		return fmt.Errorf("fail to open the cipher plugin, error: %s", err.Error())
	}
	log.Info("cipher plugin opened", zap.String("path", path))

	h, err := p.Lookup("CipherPlugin")
	if err != nil {
		return fmt.Errorf("fail to the 'CipherPlugin' object in the plugin, error: %s", err.Error())
	}

	var cipherVal hook.Cipher
	var ok bool
	cipherVal, ok = h.(hook.Cipher)
	if !ok {
		return fmt.Errorf("fail to convert the `CipherPlugin` interface")
	}

	if err = cipherVal.Init(paramtable.GetCipherParams().SoConfigs.GetValue()); err != nil {
		return fmt.Errorf("fail to init configs for the cipher plugin, error: %s", err.Error())
	}
	storeCipher((cipherVal))
	return nil
}

func InitOnceCipher() {
	initCipherOnce.Do(func() {
		err := initCipher()
		if err != nil {
			logFunc := log.Warn
			if paramtable.Get().CommonCfg.PanicWhenPluginFail.GetAsBool() {
				logFunc = log.Panic
			}
			logFunc("fail to init cipher plugin",
				zap.String("so_path", paramtable.GetCipherParams().SoPathGo.GetValue()),
				zap.Error(err))
		}
	})
}

// GetCipher returns singleton hook.Encryption instance.
func GetCipher() hook.Cipher {
	InitOnceCipher()
	return Cipher.Load().(cipherContainer).cipher
}
