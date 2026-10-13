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

package manifest

import (
	"context"
	"time"
)

// ObjectStore is the object storage that the manifest code reads and writes.
// All keys are complete object keys.
type ObjectStore interface {
	Write(ctx context.Context, key string, data []byte) error
	// Read returns an error marked with pkerr.ErrCorrupted when the key does not exist.
	Read(ctx context.Context, key string) ([]byte, error)
	// Walk calls fn for every object under prefix, recursively, until fn returns false.
	Walk(ctx context.Context, prefix string, fn func(key string, modTime time.Time) bool) error
	MultiRemove(ctx context.Context, keys []string) error
}

// Cond is a condition on one key of a conditional write to the metadata store
// of the manifest pointers.
type Cond struct {
	Key string
	// Absent requires that Key does not exist. When Absent is false, Key
	// must exist and hold Value.
	Absent bool
	Value  string
}

// KeyAbsent returns the condition that key does not exist.
func KeyAbsent(key string) Cond { return Cond{Key: key, Absent: true} }

// ValueIs returns the condition that key exists and holds value.
func ValueIs(key, value string) Cond { return Cond{Key: key, Value: value} }
