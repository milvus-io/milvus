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
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	management "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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
		management.Register(&management.Handler{
			Path:        management.RouteQueryCoordBalanceStatus,
			HandlerFunc: proxy.CheckQueryCoordBalanceStatus,
		})

		management.Register(&management.Handler{
			Path:        management.BatchBalanceStatusPath,
			HandlerFunc: proxy.HandleBatchBalanceStatus,
		})
		management.Register(&management.Handler{
			Path:        management.BatchNodesPath,
			HandlerFunc: proxy.ListBatchQueryNodes,
		})
		management.Register(&management.Handler{
			Path:        management.BatchNodeStatusPath,
			HandlerFunc: proxy.HandleBatchNodeStatus,
		})
		management.Register(&management.Handler{
			Path:        management.BatchNodeDistributionPath,
			HandlerFunc: proxy.GetBatchNodeDistribution,
		})
		management.Register(&management.Handler{
			Path:        management.BatchTransferPath,
			HandlerFunc: proxy.TransferBatchSegment,
		})

		management.Register(&management.Handler{
			Path:        management.StreamingBalanceStatusPath,
			HandlerFunc: proxy.HandleStreamingBalanceStatus,
		})
		management.Register(&management.Handler{
			Path:        management.StreamingNodesPath,
			HandlerFunc: proxy.HandleStreamingNodes,
		})
		management.Register(&management.Handler{
			Path:        management.StreamingNodeStatusPath,
			HandlerFunc: proxy.HandleStreamingNodeStatus,
		})
		management.Register(&management.Handler{
			Path:        management.StreamingNodeDistributionPath,
			HandlerFunc: proxy.GetStreamingNodeDistribution,
		})
		management.Register(&management.Handler{
			Path:        management.StreamingTransferPath,
			HandlerFunc: proxy.TransferStreamingChannel,
		})

		management.Register(&management.Handler{
			Path:        management.DataGCPath,
			HandlerFunc: proxy.HandleDatacoordGC,
		})
	})
}

func (node *Proxy) HandleDatacoordGC(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		node.GetDatacoordGCStatus(w, req)
	case http.MethodPut:
		node.ControlDatacoordGC(w, req)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// GetDatacoordGCStatus handles GET requests to fetch the current GC status
// by calling the dedicated GetGcStatus method on MixCoord.
func (node *Proxy) GetDatacoordGCStatus(w http.ResponseWriter, req *http.Request) {
	// Call the dedicated GetGcStatus method, which expects an empty request body.
	resp, err := node.mixCoord.GetGcStatus(req.Context(), &datapb.GetGcStatusRequest{})
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to get garbage collection status: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	// Create a client-friendly JSON response.
	jsonResponse := struct {
		Msg           string `json:"msg"`
		Status        string `json:"status"`
		TimeRemaining int32  `json:"time_remaining_seconds,omitempty"`
	}{
		Msg:    "OK", // Add this line to set the message
		Status: "active",
	}

	if resp.GetIsPaused() {
		jsonResponse.Status = "suspended"
		jsonResponse.TimeRemaining = resp.GetTimeRemainingSeconds()
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(jsonResponse)
}

func (node *Proxy) ControlDatacoordGC(w http.ResponseWriter, req *http.Request) {
	// Defines the request body struct, including status and an optional pause_seconds.
	var requestBody struct {
		Status       string `json:"status"`
		PauseSeconds int64  `json:"pause_seconds"`
	}

	// Parses the JSON from the request body.
	if err := json.NewDecoder(req.Body).Decode(&requestBody); err != nil {
		http.Error(w, `{"msg": "Invalid request body"}`, http.StatusBadRequest)
		return
	}

	var gcCommand datapb.GcCommand
	var params []*commonpb.KeyValuePair

	// Determines the operation based on the status parameter.
	switch requestBody.Status {
	case "suspended":
		gcCommand = datapb.GcCommand_Pause
		// If pause_seconds is provided, add it to the parameters.
		if requestBody.PauseSeconds > 0 {
			params = append(params, &commonpb.KeyValuePair{
				Key:   "duration",
				Value: fmt.Sprintf("%d", requestBody.PauseSeconds),
			})
		}
	case "resumed", "active":
		gcCommand = datapb.GcCommand_Resume
	default:
		http.Error(w, `{"msg": "Invalid status value. Use 'suspended', 'resumed' or 'active'."}`, http.StatusBadRequest)
		return
	}

	resp, err := node.mixCoord.GcControl(req.Context(), &datapb.GcControlRequest{
		Base:    commonpbutil.NewMsgBase(),
		Command: gcCommand,
		Params:  params,
	})
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to control garbage collection: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if resp.GetErrorCode() != commonpb.ErrorCode_Success {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to control garbage collection: %s"}`, resp.GetReason()), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}

func (node *Proxy) PauseDatacoordGC(w http.ResponseWriter, req *http.Request) {
	pauseSeconds := req.URL.Query().Get("pause_seconds")

	resp, err := node.mixCoord.GcControl(req.Context(), &datapb.GcControlRequest{
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
	resp, err := node.mixCoord.GcControl(req.Context(), &datapb.GcControlRequest{
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

// HandleStreamingNodes handles GET requests to list streaming and query nodes.
func (node *Proxy) HandleStreamingNodes(w http.ResponseWriter, req *http.Request) {
	// 1. Fetch data from the streaming service.
	streamingNodes, err := streaming.WAL().Balancer().ListStreamingNode(req.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to list streaming nodes, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	// Define the custom JSON response structures.
	type nodeResponse struct {
		ID      int64  `json:"ID"`
		Address string `json:"address"`
		State   string `json:"state"`
	}

	// Use a map for efficient de-duplication.
	combinedNodes := make(map[int64]nodeResponse)

	// Add streaming nodes to the map.
	for _, info := range streamingNodes {
		// Assume streaming nodes are "active".
		combinedNodes[info.ServerID] = nodeResponse{
			ID:      info.ServerID,
			Address: info.Address,
			State:   "active",
		}
	}

	// Call GetFrozenNodeIDs to get the list of suspended nodes.
	frozenNodeIDs, err := streaming.WAL().Balancer().GetFrozenNodeIDs(req.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to get frozen nodes, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	// Update the state of suspended nodes.
	for _, nodeID := range frozenNodeIDs {
		// Check if the node ID exists in the combined map.
		if node, ok := combinedNodes[nodeID]; ok {
			// If it exists, update its state to "suspended".
			node.State = "suspended"
			combinedNodes[nodeID] = node
		}
	}

	// 2. Fetch data from the mixCoord service.
	queryResp, err := node.mixCoord.ListQueryNode(req.Context(), &querypb.ListQueryNodeRequest{
		Base: commonpbutil.NewMsgBase(),
	})
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to list query nodes, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if !merr.Ok(queryResp.GetStatus()) {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to list query nodes, %s"}`, queryResp.GetStatus().GetReason()), http.StatusInternalServerError)
		return
	}

	// 3. Iterate through query nodes and perform a second RPC for distribution.
	for _, info := range queryResp.NodeInfos {
		// Skip if the node ID is already in the map (from streaming service)
		if _, exists := combinedNodes[info.ID]; exists {
			continue
		}

		// Make the second RPC call to get distribution
		distResp, err := node.mixCoord.GetQueryNodeDistribution(req.Context(), &querypb.GetQueryNodeDistributionRequest{
			Base:   commonpbutil.NewMsgBase(),
			NodeID: info.ID,
		})

		// On error, log it and skip this node, but don't return.
		if err != nil || !merr.Ok(distResp.GetStatus()) {
			// You might want to log this error for debugging:
			// log.Printf("Failed to get distribution for node %d: %v", info.NodeID, err)
			continue
		}

		// 4. Check if the channel names list is not empty.
		if len(distResp.ChannelNames) > 0 {
			// If channels exist and the node is not in the map, add it.
			combinedNodes[info.ID] = nodeResponse{
				ID:      info.ID,
				Address: info.Address,
				State:   info.State,
			}
		}
	}

	// 3. Convert the map values back to a slice for the final response.
	finalNodes := make([]nodeResponse, 0, len(combinedNodes))
	for _, node := range combinedNodes {
		finalNodes = append(finalNodes, node)
	}

	// Define the final response envelope.
	type response struct {
		Msg       string         `json:"msg"`
		NodeInfos []nodeResponse `json:"nodeInfos"`
	}

	res := response{
		Msg:       "OK",
		NodeInfos: finalNodes,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(res); err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to encode response, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
}

func (node *Proxy) ListBatchQueryNodes(w http.ResponseWriter, req *http.Request) {
	node.ListQueryNode(w, req)
}

func (node *Proxy) ListQueryNode(w http.ResponseWriter, req *http.Request) {
	resp, err := node.mixCoord.ListQueryNode(req.Context(), &querypb.ListQueryNodeRequest{
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

	resp, err := node.mixCoord.GetQueryNodeDistribution(req.Context(), &querypb.GetQueryNodeDistributionRequest{
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

	// Use string array for SealedSegmentIDs to prevent precision loss in JSON parsers.
	// Large integers (int64) may be incorrectly rounded when parsed as double.
	type distribution struct {
		Channels         []string `json:"channel_names"`
		SealedSegmentIDs []string `json:"sealed_segmentIDs"`
	}

	dist := distribution{
		Channels: resp.ChannelNames,
		SealedSegmentIDs: lo.Map(resp.SealedSegmentIDs, func(id int64, _ int) string {
			return strconv.FormatInt(id, 10)
		}),
	}

	bytes, err := json.Marshal(dist)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to get query node distribution, %s"}`, err.Error())))
		return
	}
	w.Write(bytes)
}

// GetStreamingNodeDistribution handles GET requests to retrieve streaming node distribution.
func (node *Proxy) GetStreamingNodeDistribution(w http.ResponseWriter, req *http.Request) {
	if err := req.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to parse form data, %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	nodeID, err := strconv.ParseInt(req.FormValue("node_id"), 10, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to get streaming node distribution, invalid node_id: %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	// Call the internal method to get the streaming node assignment.
	resp, err := streaming.WAL().Balancer().GetWALDistribution(req.Context(), nodeID)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to get streaming node distribution, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	// Extract the channel names from the map keys.
	channelNames := make([]string, 0, len(resp.Channels))
	for name := range resp.Channels {
		channelNames = append(channelNames, name)
	}

	// Define the custom response struct to match the desired format.
	type distributionResponse struct {
		ChannelNames     []string `json:"channel_names"`
		SealedSegmentIDs []string `json:"sealed_segmentIDs"`
	}

	// Create the response object. sealed_segmentIDs is empty because GetWALDistribution doesn't provide this data.
	dist := distributionResponse{
		ChannelNames:     channelNames,
		SealedSegmentIDs: []string{},
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// Encode and write the response.
	if err := json.NewEncoder(w).Encode(dist); err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to encode response, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
}

// GetBatchNodeDistribution handles GET requests to retrieve node distribution.
// This handler should be registered to the new path.
func (node *Proxy) GetBatchNodeDistribution(w http.ResponseWriter, req *http.Request) {
	if err := req.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to parse form data, %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	nodeID, err := strconv.ParseInt(req.FormValue("node_id"), 10, 64)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to get query node distribution, invalid node_id: %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	resp, err := node.mixCoord.GetQueryNodeDistribution(req.Context(), &querypb.GetQueryNodeDistributionRequest{
		Base:   commonpbutil.NewMsgBase(),
		NodeID: nodeID,
	})
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to get query node distribution, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	if !merr.Ok(resp.GetStatus()) {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to get query node distribution, %s"}`, resp.GetStatus().GetReason()), http.StatusInternalServerError)
		return
	}

	// Use string array for SealedSegmentIDs to prevent precision loss in JSON parsers.
	// Large integers (int64) may be incorrectly rounded when parsed as double.
	type distribution struct {
		Channels         []string `json:"channel_names"`
		SealedSegmentIDs []string `json:"sealed_segmentIDs"`
	}

	dist := distribution{
		Channels: resp.ChannelNames,
		SealedSegmentIDs: lo.Map(resp.SealedSegmentIDs, func(id int64, _ int) string {
			return strconv.FormatInt(id, 10)
		}),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(dist); err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to encode response, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
}

func (node *Proxy) SuspendQueryCoordBalance(w http.ResponseWriter, req *http.Request) {
	resp, err := node.mixCoord.SuspendBalance(req.Context(), &querypb.SuspendBalanceRequest{
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
	resp, err := node.mixCoord.ResumeBalance(req.Context(), &querypb.ResumeBalanceRequest{
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

func (node *Proxy) CheckQueryCoordBalanceStatus(w http.ResponseWriter, req *http.Request) {
	resp, err := node.mixCoord.CheckBalanceStatus(req.Context(), &querypb.CheckBalanceStatusRequest{
		Base: commonpbutil.NewMsgBase(),
	})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to check balance status, %s"}`, err.Error())))
		return
	}

	if !merr.Ok(resp.GetStatus()) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to check balance status, %s"}`, resp.GetStatus().GetReason())))
		return
	}
	w.WriteHeader(http.StatusOK)
	balanceStatus := "suspended"
	if resp.IsActive {
		balanceStatus = "active"
	}
	w.Write([]byte(fmt.Sprintf(`{"msg": "OK", "status": "%v"}`, balanceStatus)))
}

// HandleBatchBalanceStatus is the main handler for the unified endpoint.
func (node *Proxy) HandleBatchBalanceStatus(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		node.getBatchBalanceStatus(w, req)
	case http.MethodPut:
		node.controlBatchBalanceStatus(w, req)
	default:
		http.Error(w, `{"msg": "Method not allowed"}`, http.StatusMethodNotAllowed)
	}
}

// getBatchBalanceStatus handles GET requests to fetch the balance status.
func (node *Proxy) getBatchBalanceStatus(w http.ResponseWriter, req *http.Request) {
	resp, err := node.mixCoord.CheckBalanceStatus(req.Context(), &querypb.CheckBalanceStatusRequest{
		Base: commonpbutil.NewMsgBase(),
	})
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to check balance status, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if !merr.Ok(resp.GetStatus()) {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to check balance status, %s"}`, resp.GetStatus().GetReason()), http.StatusInternalServerError)
		return
	}

	balanceStatus := "suspended"
	if resp.IsActive {
		balanceStatus = "active"
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf(`{"msg": "OK", "status": "%v"}`, balanceStatus)))
}

// controlBatchBalanceStatus handles PUT requests to suspend or resume balance.
func (node *Proxy) controlBatchBalanceStatus(w http.ResponseWriter, req *http.Request) {
	// Define the request body struct for the PUT request.
	var requestBody struct {
		Status string `json:"status"`
	}

	// Decode the JSON from the request body.
	if err := json.NewDecoder(req.Body).Decode(&requestBody); err != nil {
		http.Error(w, `{"msg": "Invalid request body"}`, http.StatusBadRequest)
		return
	}

	var resp *commonpb.Status
	var err error
	var errMsg string

	// Call the appropriate MixCoord method based on the status.
	switch requestBody.Status {
	case "suspended":
		resp, err = node.mixCoord.SuspendBalance(req.Context(), &querypb.SuspendBalanceRequest{
			Base: commonpbutil.NewMsgBase(),
		})
		errMsg = "failed to suspend balance"
	case "resumed", "active":
		resp, err = node.mixCoord.ResumeBalance(req.Context(), &querypb.ResumeBalanceRequest{
			Base: commonpbutil.NewMsgBase(),
		})
		errMsg = "failed to resume balance"
	default:
		http.Error(w, `{"msg": "Invalid status value. Use 'suspended', 'resumed' or 'active'."}`, http.StatusBadRequest)
		return
	}

	if err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "%s, %s"}`, errMsg, err.Error()), http.StatusInternalServerError)
		return
	}

	if !merr.Ok(resp) {
		http.Error(w, fmt.Sprintf(`{"msg": "%s, %s"}`, errMsg, resp.GetReason()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}

// HandleStreamingBalanceStatus is the main handler for the unified endpoint.
func (node *Proxy) HandleStreamingBalanceStatus(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		node.getStreamingBalanceStatus(w, req)
	case http.MethodPut:
		node.controlStreamingBalanceStatus(w, req)
	default:
		http.Error(w, `{"msg": "Method not allowed"}`, http.StatusMethodNotAllowed)
	}
}

// getStreamingBalanceStatus handles GET requests to fetch the balance status.
func (node *Proxy) getStreamingBalanceStatus(w http.ResponseWriter, req *http.Request) {
	isSuspended, err := streaming.WAL().Balancer().IsRebalanceSuspended(req.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to get balance status: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	status := "activate"
	if isSuspended {
		status = "suspended"
	}

	response := map[string]string{
		"msg":    "OK",
		"status": status,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// controlStreamingBalanceStatus handles PUT requests to change the balance status.
func (node *Proxy) controlStreamingBalanceStatus(w http.ResponseWriter, req *http.Request) {
	var requestBody struct {
		Status string `json:"status"`
	}

	if err := json.NewDecoder(req.Body).Decode(&requestBody); err != nil {
		http.Error(w, `{"msg": "Invalid request body"}`, http.StatusBadRequest)
		return
	}

	var err error
	var errMsg string

	switch requestBody.Status {
	case "suspended":
		err = streaming.WAL().Balancer().SuspendRebalance(req.Context())
		errMsg = "failed to suspend balance"
	case "resumed", "active":
		err = streaming.WAL().Balancer().ResumeRebalance(req.Context())
		errMsg = "failed to resume balance"
	default:
		http.Error(w, `{"msg": "Invalid status value. Use 'suspended', 'resumed' or 'active'."}`, http.StatusBadRequest)
		return
	}

	if err != nil {
		// Use a consistent error message format.
		http.Error(w, fmt.Sprintf(`{"msg": "%s, %s"}`, errMsg, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}

// HandleStreamingNodeStatus is the main handler that dispatches requests
// based on the HTTP method.
func (node *Proxy) HandleStreamingNodeStatus(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		node.handleGetStreamingNodeStatus(w, req)
	case http.MethodPut:
		node.handlePutStreamingNodeStatus(w, req)
	default:
		http.Error(w, `{"msg": "Method not allowed"}`, http.StatusMethodNotAllowed)
	}
}

// handleGetNodeStatus handles GET requests to retrieve a node's status.
func (node *Proxy) handleGetStreamingNodeStatus(w http.ResponseWriter, req *http.Request) {
	// Parse the request form to access URL query parameters.
	if err := req.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to parse form, %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	// Access the query parameter from the populated req.Form field.
	nodeIDStr := req.Form.Get("node_id")
	if nodeIDStr == "" {
		http.Error(w, `{"msg": "node_id query parameter is required"}`, http.StatusBadRequest)
		return
	}

	nodeID, err := strconv.ParseInt(nodeIDStr, 10, 64)
	if err != nil {
		http.Error(w, `{"msg": "Invalid node_id parameter"}`, http.StatusBadRequest)
		return
	}

	// 1. Call GetFrozenNodeIDs to get the list of suspended nodes.
	frozenNodeIDs, err := streaming.WAL().Balancer().GetFrozenNodeIDs(req.Context())
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to get frozen nodes, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	// 2. Use lo.Contains to check if the nodeID is in the list.
	isSuspended := lo.Contains(frozenNodeIDs, nodeID)

	// If the node is not in the streaming list, perform a fallback check on the batch service.
	if !isSuspended {
		resp, err := node.mixCoord.IsNodeSuspended(req.Context(), &querypb.IsNodeSuspendedRequest{
			Base:   commonpbutil.NewMsgBase(),
			NodeID: nodeID,
		})
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"msg": "failed to get batch node status, %s"}`, err.Error()), http.StatusInternalServerError)
			return
		}
		if !merr.Ok(resp.GetStatus()) {
			http.Error(w, fmt.Sprintf(`{"msg": "failed to get batch node status, %s"}`, resp.GetStatus().GetReason()), http.StatusInternalServerError)
			return
		}
		isSuspended = resp.GetIsSuspended()
	}

	status := "active"
	if isSuspended {
		status = "suspended"
	}

	responseBody := struct {
		NodeID int64  `json:"node_id"`
		Status string `json:"status"`
	}{
		NodeID: nodeID,
		Status: status,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(responseBody)
}

// handlePutNodeStatus handles PUT requests to update a node's status.
func (node *Proxy) handlePutStreamingNodeStatus(w http.ResponseWriter, req *http.Request) {
	var requestBody struct {
		Status string `json:"status"`
		NodeID int64  `json:"node_id"`
	}

	if err := json.NewDecoder(req.Body).Decode(&requestBody); err != nil {
		http.Error(w, `{"msg": "Invalid request body"}`, http.StatusBadRequest)
		return
	}

	if requestBody.NodeID == 0 {
		http.Error(w, `{"msg": "node_id is required"}`, http.StatusBadRequest)
		return
	}

	nodeIDs := []int64{requestBody.NodeID}
	var err error
	var errMsg string

	switch requestBody.Status {
	case "suspended":
		err = streaming.WAL().Balancer().FreezeNodeIDs(req.Context(), nodeIDs)
		errMsg = "failed to suspend streaming node"
	case "active":
		err = streaming.WAL().Balancer().DefreezeNodeIDs(req.Context(), nodeIDs)
		errMsg = "failed to activate streaming node"
	default:
		http.Error(w, `{"msg": "Invalid status value. Use 'suspended' or 'active'."}`, http.StatusBadRequest)
		return
	}

	if err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "%s, %s"}`, errMsg, err.Error()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}

// HandleBatchNodeStatus is the main handler that dispatches requests
// based on the HTTP method.
func (node *Proxy) HandleBatchNodeStatus(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		node.handleGetBatchNodeStatus(w, req)
	case http.MethodPut:
		node.handlePutBatchNodeStatus(w, req)
	default:
		http.Error(w, `{"msg": "Method not allowed"}`, http.StatusMethodNotAllowed)
	}
}

// handleGetBatchNodeStatus handles GET requests to retrieve a node's status.
func (node *Proxy) handleGetBatchNodeStatus(w http.ResponseWriter, req *http.Request) {
	// Parse the request form to access URL query parameters.
	if err := req.ParseForm(); err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to parse form, %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	// Access the query parameter from the populated req.Form field.
	nodeIDStr := req.Form.Get("node_id")
	if nodeIDStr == "" {
		http.Error(w, `{"msg": "node_id query parameter is required"}`, http.StatusBadRequest)
		return
	}

	nodeID, err := strconv.ParseInt(nodeIDStr, 10, 64)
	if err != nil {
		http.Error(w, `{"msg": "Invalid node_id parameter"}`, http.StatusBadRequest)
		return
	}

	// Call the gRPC method to check the node's status
	resp, err := node.mixCoord.IsNodeSuspended(req.Context(), &querypb.IsNodeSuspendedRequest{
		Base:   commonpbutil.NewMsgBase(),
		NodeID: nodeID,
	})
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to get node status, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if !merr.Ok(resp.GetStatus()) {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to get node status, %s"}`, resp.GetStatus().GetReason()), http.StatusInternalServerError)
		return
	}

	status := "active"
	if resp.GetIsSuspended() {
		status = "suspended"
	}

	responseBody := struct {
		NodeID int64  `json:"node_id"`
		Status string `json:"status"`
	}{
		NodeID: nodeID,
		Status: status,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(responseBody)
}

// handlePutBatchNodeStatus handles PUT requests to change the node status.
func (node *Proxy) handlePutBatchNodeStatus(w http.ResponseWriter, req *http.Request) {
	var requestBody struct {
		NodeID int64  `json:"node_id"`
		Status string `json:"status"`
	}

	if err := json.NewDecoder(req.Body).Decode(&requestBody); err != nil {
		http.Error(w, `{"msg": "Invalid request body"}`, http.StatusBadRequest)
		return
	}

	if requestBody.NodeID == 0 {
		http.Error(w, `{"msg": "node_id is required"}`, http.StatusBadRequest)
		return
	}

	var resp *commonpb.Status
	var err error
	var errMsg string

	switch requestBody.Status {
	case "suspended":
		resp, err = node.mixCoord.SuspendNode(req.Context(), &querypb.SuspendNodeRequest{
			Base:   commonpbutil.NewMsgBase(),
			NodeID: requestBody.NodeID,
		})
		errMsg = "failed to suspend node"
	case "resumed", "active":
		resp, err = node.mixCoord.ResumeNode(req.Context(), &querypb.ResumeNodeRequest{
			Base:   commonpbutil.NewMsgBase(),
			NodeID: requestBody.NodeID,
		})
		errMsg = "failed to resume node"
	default:
		http.Error(w, `{"msg": "Invalid status value. Use 'suspended', 'resumed' or 'active'."}`, http.StatusBadRequest)
		return
	}

	if err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "%s, %s"}`, errMsg, err.Error()), http.StatusInternalServerError)
		return
	}
	if !merr.Ok(resp) {
		http.Error(w, fmt.Sprintf(`{"msg": "%s, %s"}`, errMsg, resp.GetReason()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
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
	resp, err := node.mixCoord.SuspendNode(req.Context(), &querypb.SuspendNodeRequest{
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
	resp, err := node.mixCoord.ResumeNode(req.Context(), &querypb.ResumeNodeRequest{
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

func (node *Proxy) TransferBatchSegment(w http.ResponseWriter, req *http.Request) {
	// Defines the request body struct with all parameters.
	var requestBody struct {
		SourceNodeID int64 `json:"source_node_id"`
		TargetNodeID int64 `json:"target_node_id,omitempty"`
		SegmentID    int64 `json:"segment_id,omitempty"`
		CopyMode     *bool `json:"copy_mode,omitempty"`
	}

	// Decodes the JSON from the request body.
	if err := json.NewDecoder(req.Body).Decode(&requestBody); err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "Invalid request body, %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	// Check if the mandatory field is provided.
	if requestBody.SourceNodeID == 0 {
		http.Error(w, `{"msg": "source_node_id is required"}`, http.StatusBadRequest)
		return
	}

	request := &querypb.TransferSegmentRequest{
		Base: commonpbutil.NewMsgBase(),
	}
	request.SourceNodeID = requestBody.SourceNodeID

	// Handle optional fields based on whether they were present in the JSON.
	if requestBody.TargetNodeID != 0 {
		request.TargetNodeID = requestBody.TargetNodeID
		request.ToAllNodes = false
	} else {
		request.ToAllNodes = true
	}

	if requestBody.SegmentID != 0 {
		request.SegmentID = requestBody.SegmentID
		request.TransferAll = false
	} else {
		request.TransferAll = true
	}

	// Check if the CopyMode field was provided.
	if requestBody.CopyMode != nil {
		request.CopyMode = *requestBody.CopyMode
	} else {
		request.CopyMode = false // Default to false if not provided
	}

	resp, err := node.mixCoord.TransferSegment(req.Context(), request)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to transfer segment, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	if !merr.Ok(resp) {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to transfer segment, %s"}`, resp.GetReason()), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
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

	resp, err := node.mixCoord.TransferSegment(req.Context(), request)
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

// TransferStreamingChannel handles the transfer and defreeze operation.
func (node *Proxy) TransferStreamingChannel(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, `{"msg": "Method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	var requestBody struct {
		SourceNodeID int64  `json:"source_node_id"`
		TargetNodeID *int64 `json:"target_node_id,omitempty"`
		ChannelName  string `json:"channel_name,omitempty"`
		CopyMode     bool   `json:"copy_mode,omitempty"`
	}

	if err := json.NewDecoder(req.Body).Decode(&requestBody); err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "Invalid request body, %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	if requestBody.SourceNodeID == 0 {
		http.Error(w, `{"msg": "source_node_id is required"}`, http.StatusBadRequest)
		return
	}

	// --- 1. Call the TransferChannel method ---
	transferReq := &querypb.TransferChannelRequest{
		Base:         commonpbutil.NewMsgBase(),
		SourceNodeID: requestBody.SourceNodeID,
		ChannelName:  requestBody.ChannelName,
		CopyMode:     requestBody.CopyMode,
	}

	if requestBody.TargetNodeID != nil {
		transferReq.TargetNodeID = *requestBody.TargetNodeID
		transferReq.ToAllNodes = false
	} else {
		transferReq.ToAllNodes = true
	}

	if len(requestBody.ChannelName) == 0 {
		transferReq.TransferAll = true
	} else {
		transferReq.TransferAll = false
	}

	resp, err := node.mixCoord.TransferChannel(req.Context(), transferReq)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to transfer channel, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	if !merr.Ok(resp) {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to transfer channel, %s"}`, resp.GetReason()), http.StatusInternalServerError)
		return
	}

	// --- 2. Conditionally call DefreezeNodeIDs ---
	nodeIDs := []int64{requestBody.SourceNodeID}
	if err := streaming.WAL().Balancer().DefreezeNodeIDs(req.Context(), nodeIDs); err != nil {
		// Log the error but don't return 500, as the primary operation succeeded.
		// log.Printf("Failed to defreeze streaming node %d: %v", requestBody.SourceNodeID, err)
	}

	w.Header().Set("Content-Type", "application/json")
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

	resp, err := node.mixCoord.TransferChannel(req.Context(), request)
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
	resp, err := node.mixCoord.CheckQueryNodeDistribution(req.Context(), &querypb.CheckQueryNodeDistributionRequest{
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
