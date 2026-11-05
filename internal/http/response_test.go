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

package http

import (
	"context"
	gojson "encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteJSONResponse(t *testing.T) {
	t.Run("escape_message", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		msg := `boom "quoted" \and\ slashed`

		WriteJSONWithMsg(context.Background(), recorder, http.StatusBadRequest, msg)

		assert.Equal(t, http.StatusBadRequest, recorder.Code)
		assert.Equal(t, "application/json", recorder.Header().Get("Content-Type"))
		assert.Equal(t, `{"msg":"boom \"quoted\" \\and\\ slashed"}`, recorder.Body.String())
		var payload map[string]string
		require.NoError(t, gojson.Unmarshal(recorder.Body.Bytes(), &payload))
		assert.Equal(t, msg, payload["msg"])
	})

	t.Run("marshal_error", func(t *testing.T) {
		recorder := httptest.NewRecorder()

		WriteJSONResponse(context.Background(), recorder, http.StatusOK, make(chan int))

		assert.Equal(t, http.StatusInternalServerError, recorder.Code)
		var payload map[string]string
		require.NoError(t, gojson.Unmarshal(recorder.Body.Bytes(), &payload))
		assert.Contains(t, payload["msg"], "failed to marshal response")
	})
}
