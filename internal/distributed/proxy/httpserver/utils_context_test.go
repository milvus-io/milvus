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

package httpserver

import (
	"context"
	"errors"
	"testing"

	"github.com/tidwall/gjson"
)

func TestContextAwareRESTConversionStopsAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rows := []gjson.Result{gjson.Parse(`{"id":1}`)}
	if _, _, err := checkAndSetDataRows(ctx, rows, buildStructArrayTestSchema(), false); !errors.Is(err, context.Canceled) {
		t.Fatalf("checkAndSetDataRows error = %v", err)
	}
	if _, err := anyToColumnsWithContext(ctx, []map[string]interface{}{{"id": 1}}, nil, buildStructArrayTestSchema(), true, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("anyToColumnsWithContext error = %v", err)
	}
	if _, err := buildQueryRespWithContext(ctx, 100, nil, nil, nil, nil, true, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("buildQueryRespWithContext error = %v", err)
	}
}
