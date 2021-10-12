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
	"os"
	"testing"

	"errors"

	"github.com/opentracing/opentracing-go"
	oplog "github.com/opentracing/opentracing-go/log"
	"github.com/stretchr/testify/assert"
)

type simpleStruct struct {
	name  string
	value string
}

func TestMain(m *testing.M) {
	closer := InitTracing("test")
	defer closer.Close()
	os.Exit(m.Run())
}

func TestInit(t *testing.T) {
	cfg := initFromEnv("test")
	assert.NotNil(t, cfg)
}

func TestTracing(t *testing.T) {
	// context normally can be propagated through func params
	ctx := context.Background()

	//start span
	//default use function name for operation name
	sp, ctx := StartSpanFromContext(ctx)
	id, sampled, found := InfoFromContext(ctx)
	fmt.Printf("traceID = %s, sampled = %t, found = %t", id, sampled, found)
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
			LogError(sp, err)
			sp.Finish()
			return nil
		}

		sp.Finish()
	}
	return nil
}

func TestInject(t *testing.T) {
	// context normally can be propagated through func params
	ctx := context.Background()

	//start span
	//default use function name for operation name
	sp, ctx := StartSpanFromContext(ctx)
	id, sampled, found := InfoFromContext(ctx)
	fmt.Printf("traceID = %s, sampled = %t, found = %t", id, sampled, found)
	pp := PropertiesReaderWriter{PpMap: map[string]string{}}
	InjectContextToPulsarMsgProperties(sp.Context(), pp.PpMap)
	tracer := opentracing.GlobalTracer()
	sc, _ := tracer.Extract(opentracing.TextMap, pp)
	assert.NotNil(t, sc)

}

func TestTraceError(t *testing.T) {
	// context normally can be propagated through func params
	sp, ctx := StartSpanFromContext(nil)
	assert.Nil(t, ctx)
	assert.NotNil(t, sp)

	sp, ctx = StartSpanFromContextWithOperationName(nil, "test")
	assert.Nil(t, ctx)
	assert.NotNil(t, sp)

	//Will Cause span log error
	StartSpanFromContextWithOperationNameWithSkip(context.Background(), "test", 10000)

	//Will Cause span log error
	StartSpanFromContextWithSkip(context.Background(), 10000)

	id, sampled, found := InfoFromSpan(nil)
	assert.Equal(t, id, "")
	assert.Equal(t, sampled, false)
	assert.Equal(t, found, false)

	id, sampled, found = InfoFromContext(nil)
	assert.Equal(t, id, "")
	assert.Equal(t, sampled, false)
	assert.Equal(t, found, false)
}
