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

package roles

import (
	"context"
	"fmt"
	"net/http"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/util/healthz"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

func unhealthyHandler(w http.ResponseWriter, r *http.Request, reason string) {
	w.WriteHeader(http.StatusInternalServerError)
	w.Header().Set(healthz.ContentTypeHeader, healthz.ContentTypeText)
	_, err := fmt.Fprint(w, reason)
	if err != nil {
		log.Warn("failed to send response",
			zap.Error(err))
	}
}

func healthyHandler(w http.ResponseWriter, r *http.Request) {
	var err error

	w.WriteHeader(http.StatusOK)
	w.Header().Set(healthz.ContentTypeHeader, healthz.ContentTypeText)
	_, err = fmt.Fprint(w, "OK")
	if err != nil {
		log.Warn("failed to send response",
			zap.Error(err))
	}
}

// GetComponentStatesInterface defines the interface that get states from component.
type GetComponentStatesInterface interface {
	// GetComponentStates returns the states of component.
	GetComponentStates(ctx context.Context, request *internalpb.GetComponentStatesRequest) (*internalpb.ComponentStates, error)
}

type componentsHealthzHandler struct {
	component GetComponentStatesInterface
}

func (handler *componentsHealthzHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	states, err := handler.component.GetComponentStates(context.Background(), &internalpb.GetComponentStatesRequest{})

	if err != nil {
		unhealthyHandler(w, r, err.Error())
		return
	}

	if states == nil {
		unhealthyHandler(w, r, "failed to get states")
		return
	}

	if states.Status == nil {
		unhealthyHandler(w, r, "failed to get status")
		return
	}

	if states.Status.ErrorCode != commonpb.ErrorCode_Success {
		unhealthyHandler(w, r, states.Status.Reason)
		return
	}

	healthyHandler(w, r)
}
