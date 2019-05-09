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

package main

import (
	"context"
	"sync/atomic"
)

type handler struct{}

func (h *handler) EchoVoid(ctx context.Context) (err error) {
	atomic.AddInt64(&counter, 1)
	return nil
}
func (h *handler) EchoByte(ctx context.Context, arg int8) (r int8, err error) {
	atomic.AddInt64(&counter, 1)
	return arg, nil
}
func (h *handler) EchoI32(ctx context.Context, arg int32) (r int32, err error) {
	atomic.AddInt64(&counter, 1)
	return arg, nil
}
func (h *handler) EchoI64(ctx context.Context, arg int64) (r int64, err error) {
	atomic.AddInt64(&counter, 1)
	return arg, nil
}
func (h *handler) EchoString(ctx context.Context, arg string) (r string, err error) {
	atomic.AddInt64(&counter, 1)
	return arg, nil
}
func (h *handler) EchoList(ctx context.Context, arg []int8) (r []int8, err error) {
	atomic.AddInt64(&counter, 1)
	return arg, nil
}
func (h *handler) EchoSet(ctx context.Context, arg map[int8]struct{}) (r map[int8]struct{}, err error) {
	atomic.AddInt64(&counter, 1)
	return arg, nil
}
func (h *handler) EchoMap(ctx context.Context, arg map[int8]int8) (r map[int8]int8, err error) {
	atomic.AddInt64(&counter, 1)
	return arg, nil
}
