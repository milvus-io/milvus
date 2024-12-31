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
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/hook"
)

type DefaultHook struct{}

var _ hook.Hook = (*DefaultHook)(nil)

func (d DefaultHook) VerifyAPIKey(key string) (string, error) {
	return "", errors.New("default hook, can't verify api key")
}

func (d DefaultHook) Init(params map[string]string) error {
	return nil
}

func (d DefaultHook) Mock(ctx context.Context, req interface{}, fullMethod string) (bool, interface{}, error) {
	return false, nil, nil
}

func (d DefaultHook) Before(ctx context.Context, req interface{}, fullMethod string) (context.Context, error) {
	return ctx, nil
}

func (d DefaultHook) After(ctx context.Context, result interface{}, err error, fullMethod string) error {
	return nil
}

func (d DefaultHook) Release() {}

type DefaultExtension struct{}

var _ hook.Extension = (*DefaultExtension)(nil)

func (d DefaultExtension) Report(info any) int {
	return 0
}

func (d DefaultExtension) ReportRefused(ctx context.Context, req interface{}, resp interface{}, err error, fullMethod string) error {
	return nil
}
