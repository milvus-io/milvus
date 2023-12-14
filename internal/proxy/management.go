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

package proxy

import (
	"fmt"
	"net/http"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	management "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
)

// this file contains proxy management restful API handler

const (
	mgrRouteGcPause  = `/management/datacoord/garbage_collection/pause`
	mgrRouteGcResume = `/management/datacoord/garbage_collection/resume`
)

var mgrRouteRegisterOnce sync.Once

func RegisterMgrRoute(proxy *Proxy) {
	mgrRouteRegisterOnce.Do(func() {
		management.Register(&management.Handler{
			Path:        mgrRouteGcPause,
			HandlerFunc: proxy.PauseDatacoordGC,
		})
		management.Register(&management.Handler{
			Path:        mgrRouteGcResume,
			HandlerFunc: proxy.ResumeDatacoordGC,
		})
	})
}

func (node *Proxy) PauseDatacoordGC(w http.ResponseWriter, req *http.Request) {
	pauseSeconds := req.URL.Query().Get("pause_seconds")

	resp, err := node.dataCoord.GcControl(req.Context(), &datapb.GcControlRequest{
		Base:    commonpbutil.NewMsgBase(),
		Command: datapb.GcCommand_Pause,
		Params: []*commonpb.KeyValuePair{
			{Key: "duration", Value: pauseSeconds},
		},
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to pause garbage collection, %s"}`, err.Error())))
		return
	}
	if resp.GetErrorCode() != commonpb.ErrorCode_Success {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to pause garbage collection, %s"}`, resp.GetReason())))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}

func (node *Proxy) ResumeDatacoordGC(w http.ResponseWriter, req *http.Request) {
	resp, err := node.dataCoord.GcControl(req.Context(), &datapb.GcControlRequest{
		Base:    commonpbutil.NewMsgBase(),
		Command: datapb.GcCommand_Resume,
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to pause garbage collection, %s"}`, err.Error())))
		return
	}
	if resp.GetErrorCode() != commonpb.ErrorCode_Success {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to pause garbage collection, %s"}`, resp.GetReason())))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}
