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

package requestbudget

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type cancelOnMarshal struct {
	cancel context.CancelFunc
}

func (v cancelOnMarshal) MarshalJSON() ([]byte, error) {
	v.cancel()
	return []byte("1"), nil
}

func TestEncodeResponseRowsProducesValidRESTEnvelope(t *testing.T) {
	var output bytes.Buffer
	err := EncodeResponseRows(context.Background(), &output, map[string]any{
		"code": 0,
		"data": []map[string]any{{"id": 1}, {"id": 2}},
		"cost": 3,
	}, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Code int              `json:"code"`
		Data []map[string]int `json:"data"`
		Cost int              `json:"cost"`
	}
	if err := json.Unmarshal(output.Bytes(), &got); err != nil {
		t.Fatalf("invalid JSON %q: %v", output.Bytes(), err)
	}
	if got.Code != 0 || got.Cost != 3 || len(got.Data) != 2 || got.Data[0]["id"] != 1 || got.Data[1]["id"] != 2 {
		t.Fatalf("response = %+v", got)
	}
}

func TestEncodeResponseRowsStopsBeforeNextRowAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var output bytes.Buffer
	err := EncodeResponseRows(ctx, &output, map[string]any{
		"code": 0,
		"data": []map[string]any{{"id": cancelOnMarshal{cancel: cancel}}, {"id": 2}},
	}, 4<<20)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context canceled", err)
	}
	if bytes.Contains(output.Bytes(), []byte(`{"id":2}`)) {
		t.Fatalf("encoded second row after cancellation: %q", output.Bytes())
	}
}

func TestEncodeResponseRowsRejectsOversizeRow(t *testing.T) {
	var output bytes.Buffer
	err := EncodeResponseRows(context.Background(), &output, map[string]any{
		"code": 0,
		"data": []map[string]any{{"text": strings.Repeat("x", 2048)}},
	}, 1024)
	if !errors.Is(err, merr.ErrServiceInternal) {
		t.Fatalf("error = %v, want service internal", err)
	}
	if bytes.Contains(output.Bytes(), []byte(strings.Repeat("x", 2048))) {
		t.Fatal("oversized row was written")
	}
}
