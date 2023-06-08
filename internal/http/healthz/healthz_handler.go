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

package healthz

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
)

// GetComponentStatesInterface defines the interface that get states from component.
type GetComponentStatesInterface interface {
	// GetComponentStates returns the states of component.
	GetComponentStates(ctx context.Context, request *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error)
}

type Indicator interface {
	GetName() string
	Health(ctx context.Context) commonpb.StateCode
}

type IndicatorState struct {
	Name string             `json:"name"`
	Code commonpb.StateCode `json:"code"`
}

type HealthResponse struct {
	State  string            `json:"state"`
	Detail []*IndicatorState `json:"detail"`
}

type HealthHandler struct {
	indicators []Indicator
}

var _ http.Handler = (*HealthHandler)(nil)

var defaultHandler = HealthHandler{}

func Register(indicator Indicator) {
	defaultHandler.indicators = append(defaultHandler.indicators, indicator)
}

func Handler() *HealthHandler {
	return &defaultHandler
}

func (handler *HealthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	resp := &HealthResponse{
		State: "OK",
	}
	ctx := context.Background()
	for _, in := range handler.indicators {
		code := in.Health(ctx)
		resp.Detail = append(resp.Detail, &IndicatorState{
			Name: in.GetName(),
			Code: code,
		})
		if code != commonpb.StateCode_Healthy && code != commonpb.StateCode_StandBy {
			resp.State = fmt.Sprintf("component %s state is %s", in.GetName(), code.String())
		}
	}

	if resp.State == "OK" {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
	// for compatibility
	if r.Header.Get(ContentTypeHeader) != ContentTypeJSON {
		writeText(w, r, resp.State)
		return
	}

	writeJSON(w, r, resp)
}

func writeJSON(w http.ResponseWriter, r *http.Request, resp *HealthResponse) {
	w.Header().Set(ContentTypeHeader, ContentTypeJSON)
	bs, err := json.Marshal(resp)
	if err != nil {
		log.Warn("faild to send response", zap.Error(err))
	}
	w.Write(bs)
}

func writeText(w http.ResponseWriter, r *http.Request, reason string) {
	w.Header().Set(ContentTypeHeader, ContentTypeText)
	_, err := fmt.Fprint(w, reason)
	if err != nil {
		log.Warn("failed to send response",
			zap.Error(err))
	}
}
