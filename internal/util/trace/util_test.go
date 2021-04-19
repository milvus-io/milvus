// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package trace

import (
	"context"
	"fmt"
	"testing"

	"errors"

	oplog "github.com/opentracing/opentracing-go/log"
)

type simpleStruct struct {
	name  string
	value string
}

func TestTracing(t *testing.T) {
	//Already Init in each framework, this can be ignored in debug
	closer := InitTracing("test")
	defer closer.Close()

	// context normally can be propagated through func params
	ctx := context.Background()

	//start span
	//default use function name for operation name
	sp, ctx := StartSpanFromContext(ctx)
	sp.SetTag("tag1", "tag1")
	// use self-defined operation name for span
	// sp, ctx := StartSpanFromContextWithOperationName(ctx, "self-defined name")
	defer sp.Finish()

	ss := &simpleStruct{
		name:  "name",
		value: "value",
	}
	sp.LogFields(oplog.String("key", "value"), oplog.Object("key", ss))

	err := caller(ctx)

	if err != nil {
		LogError(sp, err) //LogError do something error log in trace and returns origin error.
	}

}

func caller(ctx context.Context) error {
	for i := 0; i < 2; i++ {
		// if span starts in a loop, defer is not allowed.
		// manually call span.Finish() if error occurs or one loop ends
		sp, _ := StartSpanFromContextWithOperationName(ctx, fmt.Sprintf("test:%d", i))
		sp.SetTag(fmt.Sprintf("tags:%d", i), fmt.Sprintf("tags:%d", i))

		var err error
		if i == 1 {
			err = errors.New("test")
		}

		if err != nil {
			sp.Finish()
			return LogError(sp, err)
		}

		sp.Finish()
	}
	return nil
}
