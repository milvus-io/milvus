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
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	management "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

// this file contains proxy management restful API handler
var mgrRouteRegisterOnce sync.Once

func RegisterMgrRoute(proxy *Proxy) {
	mgrRouteRegisterOnce.Do(func() {
		management.Register(&management.Handler{
			Path:        management.RouteGcPause,
			HandlerFunc: proxy.PauseDatacoordGC,
		})
		management.Register(&management.Handler{
			Path:        management.RouteGcResume,
			HandlerFunc: proxy.ResumeDatacoordGC,
		})
		management.Register(&management.Handler{
			Path:        management.RouteListQueryNode,
			HandlerFunc: proxy.ListQueryNode,
		})
		management.Register(&management.Handler{
			Path:        management.RouteGetQueryNodeDistribution,
			HandlerFunc: proxy.GetQueryNodeDistribution,
		})
		management.Register(&management.Handler{
			Path:        management.RouteSuspendQueryCoordBalance,
			HandlerFunc: proxy.SuspendQueryCoordBalance,
		})
		management.Register(&management.Handler{
			Path:        management.RouteResumeQueryCoordBalance,
			HandlerFunc: proxy.ResumeQueryCoordBalance,
		})
		management.Register(&management.Handler{
			Path:        management.RouteSuspendQueryNode,
			HandlerFunc: proxy.SuspendQueryNode,
		})
		management.Register(&management.Handler{
			Path:        management.RouteResumeQueryNode,
			HandlerFunc: proxy.ResumeQueryNode,
		})
		management.Register(&management.Handler{
			Path:        management.RouteTransferSegment,
			HandlerFunc: proxy.TransferSegment,
		})
		management.Register(&management.Handler{
			Path:        management.RouteTransferChannel,
			HandlerFunc: proxy.TransferChannel,
		})
		management.Register(&management.Handler{
			Path:        management.RouteCheckQueryNodeDistribution,
			HandlerFunc: proxy.CheckQueryNodeDistribution,
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

func (node *Proxy) ListQueryNode(w http.ResponseWriter, req *http.Request) {
	resp, err := node.queryCoord.ListQueryNode(req.Context(), &querypb.ListQueryNodeRequest{
		Base: commonpbutil.NewMsgBase(),
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to list query node, %s"}`, err.Error())))
		return
	}

	if !merr.Ok(resp.GetStatus()) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to list query node, %s"}`, resp.GetStatus().GetReason())))
		return
	}

	w.WriteHeader(http.StatusOK)
	// skip marshal status to output
	resp.Status = nil
	bytes, err := json.Marshal(resp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to list query node, %s"}`, err.Error())))
		return
	}
	w.Write(bytes)
}

func (node *Proxy) GetQueryNodeDistribution(w http.ResponseWriter, req *http.Request) {
	err := req.ParseForm()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to transfer segment, %s"}`, err.Error())))
		return
	}

	nodeID, err := strconv.ParseInt(req.FormValue("node_id"), 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to get query node distribution, %s"}`, err.Error())))
		return
	}

	resp, err := node.queryCoord.GetQueryNodeDistribution(req.Context(), &querypb.GetQueryNodeDistributionRequest{
		Base:   commonpbutil.NewMsgBase(),
		NodeID: nodeID,
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to get query node distribution, %s"}`, err.Error())))
		return
	}

	if !merr.Ok(resp.GetStatus()) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to get query node distribution, %s"}`, resp.GetStatus().GetReason())))
		return
	}
	w.WriteHeader(http.StatusOK)
	// skip marshal status to output
	resp.Status = nil
	bytes, err := json.Marshal(resp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to get query node distribution, %s"}`, err.Error())))
		return
	}
	w.Write(bytes)
}

func (node *Proxy) SuspendQueryCoordBalance(w http.ResponseWriter, req *http.Request) {
	resp, err := node.queryCoord.SuspendBalance(req.Context(), &querypb.SuspendBalanceRequest{
		Base: commonpbutil.NewMsgBase(),
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to suspend balance, %s"}`, err.Error())))
		return
	}

	if !merr.Ok(resp) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to suspend balance, %s"}`, resp.GetReason())))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}

func (node *Proxy) ResumeQueryCoordBalance(w http.ResponseWriter, req *http.Request) {
	resp, err := node.queryCoord.ResumeBalance(req.Context(), &querypb.ResumeBalanceRequest{
		Base: commonpbutil.NewMsgBase(),
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to resume balance, %s"}`, err.Error())))
		return
	}

	if !merr.Ok(resp) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to resume balance, %s"}`, resp.GetReason())))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}

func (node *Proxy) SuspendQueryNode(w http.ResponseWriter, req *http.Request) {
	err := req.ParseForm()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to transfer segment, %s"}`, err.Error())))
		return
	}

	nodeID, err := strconv.ParseInt(req.FormValue("node_id"), 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to suspend node, %s"}`, err.Error())))
		return
	}
	resp, err := node.queryCoord.SuspendNode(req.Context(), &querypb.SuspendNodeRequest{
		Base:   commonpbutil.NewMsgBase(),
		NodeID: nodeID,
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to suspend node, %s"}`, err.Error())))
		return
	}

	if !merr.Ok(resp) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to suspend node, %s"}`, resp.GetReason())))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}

func (node *Proxy) ResumeQueryNode(w http.ResponseWriter, req *http.Request) {
	err := req.ParseForm()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to transfer segment, %s"}`, err.Error())))
		return
	}

	nodeID, err := strconv.ParseInt(req.FormValue("node_id"), 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to resume node, %s"}`, err.Error())))
		return
	}
	resp, err := node.queryCoord.ResumeNode(req.Context(), &querypb.ResumeNodeRequest{
		Base:   commonpbutil.NewMsgBase(),
		NodeID: nodeID,
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to resume node, %s"}`, err.Error())))
		return
	}

	if !merr.Ok(resp) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to resume node, %s"}`, resp.GetReason())))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}

func (node *Proxy) TransferSegment(w http.ResponseWriter, req *http.Request) {
	err := req.ParseForm()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to transfer segment, %s"}`, err.Error())))
		return
	}

	request := &querypb.TransferSegmentRequest{
		Base: commonpbutil.NewMsgBase(),
	}

	source, err := strconv.ParseInt(req.FormValue("source_node_id"), 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf(`{"msg": failed to transfer segment", %s"}`, err.Error())))
		return
	}
	request.SourceNodeID = source

	target := req.FormValue("target_node_id")
	if len(target) == 0 {
		request.ToAllNodes = true
	} else {
		value, err := strconv.ParseInt(target, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(fmt.Sprintf(`{"msg": "failed to transfer segment, %s"}`, err.Error())))
			return
		}
		request.TargetNodeID = value
	}

	segmentID := req.FormValue("segment_id")
	if len(segmentID) == 0 {
		request.TransferAll = true
	} else {
		value, err := strconv.ParseInt(segmentID, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(fmt.Sprintf(`{"msg": "failed to transfer segment, %s"}`, err.Error())))
			return
		}
		request.SegmentID = value
	}

	copyMode := req.FormValue("copy_mode")
	if len(copyMode) == 0 {
		request.CopyMode = true
	} else {
		value, err := strconv.ParseBool(copyMode)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(fmt.Sprintf(`{"msg": "failed to transfer segment, %s"}`, err.Error())))
			return
		}
		request.CopyMode = value
	}

	resp, err := node.queryCoord.TransferSegment(req.Context(), request)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to transfer segment, %s"}`, err.Error())))
		return
	}

	if !merr.Ok(resp) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to transfer segment, %s"}`, resp.GetReason())))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}

func (node *Proxy) TransferChannel(w http.ResponseWriter, req *http.Request) {
	err := req.ParseForm()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to transfer channel, %s"}`, err.Error())))
		return
	}

	request := &querypb.TransferChannelRequest{
		Base: commonpbutil.NewMsgBase(),
	}

	source, err := strconv.ParseInt(req.FormValue("source_node_id"), 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf(`{"msg": failed to transfer channel", %s"}`, err.Error())))
		return
	}
	request.SourceNodeID = source

	target := req.FormValue("target_node_id")
	if len(target) == 0 {
		request.ToAllNodes = true
	} else {
		value, err := strconv.ParseInt(target, 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(fmt.Sprintf(`{"msg": "failed to transfer channel, %s"}`, err.Error())))
			return
		}
		request.TargetNodeID = value
	}

	channel := req.FormValue("channel_name")
	if len(channel) == 0 {
		request.TransferAll = true
	} else {
		request.ChannelName = channel
	}

	copyMode := req.FormValue("copy_mode")
	if len(copyMode) == 0 {
		request.CopyMode = false
	} else {
		value, err := strconv.ParseBool(copyMode)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(fmt.Sprintf(`{"msg": "failed to transfer channel, %s"}`, err.Error())))
			return
		}
		request.CopyMode = value
	}

	resp, err := node.queryCoord.TransferChannel(req.Context(), request)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to transfer channel, %s"}`, err.Error())))
		return
	}

	if !merr.Ok(resp) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to transfer channel, %s"}`, resp.GetReason())))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}

func (node *Proxy) CheckQueryNodeDistribution(w http.ResponseWriter, req *http.Request) {
	err := req.ParseForm()
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to check whether query node has same distribution, %s"}`, err.Error())))
		return
	}

	source, err := strconv.ParseInt(req.FormValue("source_node_id"), 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf(`{"msg": failed to check whether query node has same distribution", %s"}`, err.Error())))
		return
	}

	target, err := strconv.ParseInt(req.FormValue("target_node_id"), 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to check whether query node has same distribution, %s"}`, err.Error())))
		return
	}
	resp, err := node.queryCoord.CheckQueryNodeDistribution(req.Context(), &querypb.CheckQueryNodeDistributionRequest{
		Base:         commonpbutil.NewMsgBase(),
		SourceNodeID: source,
		TargetNodeID: target,
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to check whether query node has same distribution, %s"}`, err.Error())))
		return
	}

	if !merr.Ok(resp) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to check whether query node has same distribution, %s"}`, resp.GetReason())))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}
