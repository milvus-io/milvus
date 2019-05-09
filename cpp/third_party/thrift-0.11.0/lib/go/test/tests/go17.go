// +build go1.7

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package tests

import (
	"context"
	"fmt"
)

var defaultCtx = context.Background()

type FirstImpl struct{}

func (f *FirstImpl) ReturnOne(ctx context.Context) (r int64, err error) {
	return 1, nil
}

type SecondImpl struct{}

func (s *SecondImpl) ReturnTwo(ctx context.Context) (r int64, err error) {
	return 2, nil
}

type impl struct{}

func (i *impl) Hi(ctx context.Context, in int64, s string) (err error)        { fmt.Println("Hi!"); return }
func (i *impl) Emptyfunc(ctx context.Context) (err error)                     { return }
func (i *impl) EchoInt(ctx context.Context, param int64) (r int64, err error) { return param, nil }
