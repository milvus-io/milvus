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
	"fmt"
	"net/http"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/healthz"
	"github.com/milvus-io/milvus/internal/util/milvuserrors"
	"go.uber.org/zap"
)

func componentsNotServingHandler(w http.ResponseWriter, r *http.Request, msg string) {
	w.WriteHeader(http.StatusInternalServerError)
	w.Header().Set(healthz.ContentTypeHeader, healthz.ContentTypeText)
	_, err := fmt.Fprint(w, msg)
	if err != nil {
		log.Warn("failed to send response",
			zap.Error(err))
	}
}

func rootCoordNotServingHandler(w http.ResponseWriter, r *http.Request) {
	componentsNotServingHandler(w, r, milvuserrors.MsgRootCoordNotServing)
}

func queryCoordNotServingHandler(w http.ResponseWriter, r *http.Request) {
	componentsNotServingHandler(w, r, milvuserrors.MsgQueryCoordNotServing)
}

func dataCoordNotServingHandler(w http.ResponseWriter, r *http.Request) {
	componentsNotServingHandler(w, r, milvuserrors.MsgDataCoordNotServing)
}

func indexCoordNotServingHandler(w http.ResponseWriter, r *http.Request) {
	componentsNotServingHandler(w, r, milvuserrors.MsgIndexCoordNotServing)
}
