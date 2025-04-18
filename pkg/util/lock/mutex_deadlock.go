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

//go:build test

package lock

import "sync"

// The file originally used "github.com/sasha-s/go-deadlock" as deadlock detection tool in unit tests.
// However, it is broken since go 1.23, as reported in https://github.com/sasha-s/go-deadlock/issues/35.
// To restore deadlock detection, make sure the bug is fixed in go-deadlock, and use "go-deadlock" package instead.

// use `deadlock.Mutex` for test build
type Mutex = sync.Mutex

// use `deadlock.RWMutex` for test build
type RWMutex = sync.RWMutex
