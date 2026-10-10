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
	"reflect"
	"testing"

	"github.com/tidwall/gjson"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestBudgetRowsPreserveSchemaSensitiveUpsertConversion(t *testing.T) {
	body := []byte(`{"data":[{"id":1,"my_struct":[{"sub_int":18}]},{"id":2,"my_struct":[{"sub_int":21}]}]}`)
	rows := []gjson.Result{
		gjson.Parse(`{"id":1,"my_struct":[{"sub_int":18}]}`),
		gjson.Parse(`{"id":2,"my_struct":[{"sub_int":21}]}`),
	}
	ops := []*schemapb.FieldPartialUpdateOp{{
		FieldName: "my_struct",
		Op:        schemapb.FieldPartialUpdateOp_PATH_REPLACE,
		Path:      "[1][sub_int]",
	}}
	wantSchema, err := schemaForPathReplaceOperands(body, buildStructArrayTestSchema(), ops)
	if err != nil {
		t.Fatal(err)
	}
	wantData, wantValid, err := checkAndSetData(body, wantSchema, true, ops...)
	if err != nil {
		t.Fatal(err)
	}
	gotSchema, err := schemaForPathReplaceOperandsRows(context.Background(), rows, buildStructArrayTestSchema(), ops)
	if err != nil {
		t.Fatal(err)
	}
	gotData, gotValid, err := checkAndSetDataRows(context.Background(), rows, gotSchema, true, ops...)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotSchema, wantSchema) || !reflect.DeepEqual(gotData, wantData) || !reflect.DeepEqual(gotValid, wantValid) {
		t.Fatalf("streamed row conversion differs from the existing body path")
	}
}

func TestBudgetRowsStopConversionAfterCancellation(t *testing.T) {
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
