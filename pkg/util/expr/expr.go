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

package expr

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var (
	v       *vm.VM
	env     map[string]any
	authKey string
)

func Init() {
	v = &vm.VM{}
	env = map[string]any{
		"ctx": context.TODO(),
		"objSize": func(p any) int {
			message, ok := p.(proto.Message)
			if !ok {
				return int(unsafe.Sizeof(p))
			}
			return proto.Size(message)
		},
	}
	authKey = paramtable.Get().EtcdCfg.RootPath.GetValue()
}

func Register(key string, value any) {
	if env != nil {
		env[key] = value
	}
}

func Exec(code, auth string) (res string, err error) {
	defer func() {
		if e := recover(); e != nil {
			err = fmt.Errorf("panic: %v", e)
		}
	}()
	if v == nil {
		return "", fmt.Errorf("the expr isn't inited")
	}
	if code == "" {
		return "", fmt.Errorf("the expr code is empty")
	}
	if auth == "" {
		return "", fmt.Errorf("the expr auth is empty")
	}
	if authKey != auth {
		return "", fmt.Errorf("the expr auth is invalid")
	}
	program, err := expr.Compile(code, expr.Env(env), expr.WithContext("ctx"))
	if err != nil {
		log.Warn("expr compile failed", zap.String("code", code), zap.Error(err))
		return "", err
	}

	output, err := v.Run(program, env)
	if err != nil {
		log.Warn("expr run failed", zap.String("code", code), zap.Error(err))
		return "", err
	}
	return fmt.Sprintf("%v", output), nil
}
