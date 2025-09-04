package coordinator

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/pingcap/log"
	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	management "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// this file contains proxy management restful API handler
var mgrRouteRegisterOnce sync.Once

func RegisterMgrRoute(s *mixCoordImpl) {
	mgrRouteRegisterOnce.Do(func() {
		// Define a slice of structs to hold route information.
		// This replaces the repetitive management.Register calls.
		routes := []struct {
			path    string
			handler http.HandlerFunc
		}{
			// batch
			{management.BatchBalanceStatusPath, s.HandleBatchBalanceStatus},
			{management.BatchNodesPath, s.ListBatchQueryNodes},
			{management.BatchNodeStatusPath, s.HandleBatchNodeStatus},
			{management.BatchNodeDistributionPath, s.GetBatchNodeDistribution},
			{management.BatchTransferPath, s.TransferBatchSegment},
			// streaming
			{management.StreamingBalanceStatusPath, s.HandleStreamingBalanceStatus},
			{management.StreamingNodesPath, s.HandleStreamingNodes},
			{management.StreamingNodeStatusPath, s.HandleStreamingNodeStatus},
			{management.StreamingNodeDistributionPath, s.GetStreamingNodeDistribution},
			{management.StreamingTransferPath, s.TransferStreamingChannel},
			{management.DataGCPath, s.HandleDatacoordGC}, // This route is unique, so it's included here.
		}

		// Loop through the slice and register each route.
		for _, route := range routes {
			management.Register(&management.Handler{
				Path:        route.path,
				HandlerFunc: route.handler,
			})
		}
	})
}

func (s *mixCoordImpl) HandleDatacoordGC(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		s.GetDatacoordGCStatus(w, req)
	case http.MethodPut:
		s.ControlDatacoordGC(w, req)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// GetDatacoordGCStatus handles GET requests to fetch the current GC status
// by calling the dedicated GetGcStatus method on MixCoord.
func (s *mixCoordImpl) GetDatacoordGCStatus(w http.ResponseWriter, req *http.Request) {
	// Call the dedicated GetGcStatus method, which expects an empty request body.
	logger := log.With(zap.String("Scope", "Rolling"))
	resp, err := s.datacoordServer.GetGcStatus(req.Context())
	if err != nil {
		logger.Info("failed to GetGcStatus", zap.Error(err))
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
	logger.Info("GetGcStatus success", zap.Any("resp", jsonResponse))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(jsonResponse)
}

func (s *mixCoordImpl) ControlDatacoordGC(w http.ResponseWriter, req *http.Request) {
	// Defines the request body struct, including status and an optional pause_seconds.
	var requestBody struct {
		Status       string `json:"status"`
		PauseSeconds int64  `json:"pause_seconds"`
	}

	logger := log.With(zap.String("Scope", "Rolling"))

	// Parses the JSON from the request body.
	if err := json.NewDecoder(req.Body).Decode(&requestBody); err != nil {
		logger.Info("ControlDataCoordGC failed to decode body", zap.Error(err))
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
		logger.Info("ControlDataCoordGC invalid status value", zap.Any("status", requestBody.Status))
		http.Error(w, `{"msg": "Invalid status value. Use 'suspended', 'resumed' or 'active'."}`, http.StatusBadRequest)
		return
	}

	resp, err := s.GcControl(req.Context(), &datapb.GcControlRequest{
		Base:    commonpbutil.NewMsgBase(),
		Command: gcCommand,
		Params:  params,
	})
	err = merr.CheckRPCCall(resp, err)
	if err != nil {
		logger.Info("ControlDataCoordGC GcControl failed", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to control garbage collection: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	logger.Info("ControlDataCoordGC GcControl success", zap.String("gcCommand", gcCommand.String()))

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}

// HandleStreamingNodes handles GET requests to list streaming and query nodes.
func (s *mixCoordImpl) HandleStreamingNodes(w http.ResponseWriter, req *http.Request) {
	logger := log.With(zap.String("Scope", "Rolling"))

	// 1. Fetch data from the streaming service.
	streamingNodes, err := streaming.WAL().Balancer().ListStreamingNode(req.Context())
	if err != nil {
		logger.Info("HandleStreamingNodes failed to list streaming nodes", zap.Error(err))
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
		logger.Info("HandleStreamingNodes failed to get frozen nodes", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to get frozen nodes, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	logger.Info("HandleStreamingNodes get frozen nodes", zap.Any("frozen nodes", frozenNodeIDs))
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
	queryResp, err := s.getQueryNodes(req.Context())
	if err != nil {
		logger.Info("HandleStreamingNodes failed to get query nodes", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to list query nodes, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	// 3. Iterate through query nodes and perform a second RPC for distribution.
	for _, info := range queryResp.NodeInfos {
		// Skip if the node ID is already in the map (from streaming service)
		if _, exists := combinedNodes[info.ID]; exists {
			continue
		}

		// Make the second RPC call to get distribution
		distResp, err := s.GetQueryNodeDistribution(req.Context(), &querypb.GetQueryNodeDistributionRequest{
			Base:   commonpbutil.NewMsgBase(),
			NodeID: info.ID,
		})

		err = merr.CheckRPCCall(distResp, err)
		// On error, log it and skip this node, but don't return.
		if err != nil {
			if errors.Is(err, snmanager.ErrStreamingServiceNotReady) {
				continue
			}
			if errors.Is(err, merr.ErrNodeNotFound) {
				continue
			}
			logger.Info("HandleStreamingNodes GetQueryNodeDistribution failed", zap.Error(err))
			http.Error(w, fmt.Sprintf(`{"msg": "failed to get query node distribution, %s"}`, err.Error()), http.StatusInternalServerError)
			return
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
	log.Info("HandleStreamingNodes success", zap.Any("response", res))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(res); err != nil {
		logger.Info("HandleStreamingNodes failed to encode response", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to encode response, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
}

// getQueryNodes handles the RPC call to list query nodes and checks for errors.
func (s *mixCoordImpl) getQueryNodes(ctx context.Context) (*querypb.ListQueryNodeResponse, error) {
	resp, err := s.ListQueryNode(ctx, &querypb.ListQueryNodeRequest{
		Base: commonpbutil.NewMsgBase(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list query nodes: %w", err)
	}
	if !merr.Ok(resp.GetStatus()) {
		return nil, fmt.Errorf("failed to list query nodes: %s", resp.GetStatus().GetReason())
	}
	return resp, nil
}

func (s *mixCoordImpl) ListBatchQueryNodes(w http.ResponseWriter, req *http.Request) {
	logger := log.With(zap.String("Scope", "Rolling"))
	resp, err := s.getQueryNodes(req.Context())
	if err != nil {
		logger.Info("ListBatchQueryNodes failed to list query nodes", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to list query node, %s"}`, err.Error())))
		return
	}

	w.WriteHeader(http.StatusOK)
	// skip marshal status to output
	resp.Status = nil
	bytes, err := json.Marshal(resp)
	if err != nil {
		logger.Info("ListBatchQueryNodes failed to encode response", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf(`{"msg": "failed to list query node, %s"}`, err.Error())))
		return
	}
	logger.Info("ListBatchQueryNodes success", zap.Any("response", string(bytes)))
	w.Write(bytes)
}

// GetStreamingNodeDistribution handles GET requests to retrieve streaming node distribution.
func (s *mixCoordImpl) GetStreamingNodeDistribution(w http.ResponseWriter, req *http.Request) {
	logger := log.With(zap.String("Scope", "Rolling"))
	if err := req.ParseForm(); err != nil {
		logger.Info("GetStreamingNodeDistribution failed to parse form", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to parse form data, %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	nodeID, err := strconv.ParseInt(req.FormValue("node_id"), 10, 64)
	if err != nil {
		logger.Info("GetStreamingNodeDistribution failed to parse form", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to get streaming node distribution, invalid node_id: %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	// Define the custom response struct to match the desired format.
	type distributionResponse struct {
		ChannelNames     []string `json:"channel_names"`
		SealedSegmentIDs []string `json:"sealed_segmentIDs"`
	}

	var dist distributionResponse

	// First, try to get the streaming node assignment.
	streamingResp, streamingErr := streaming.WAL().Balancer().GetWALDistribution(req.Context(), nodeID)

	if streamingErr != nil {
		// If streaming node is not found, try to get the batch node distribution.
		if errors.Is(streamingErr, merr.ErrNodeNotFound) {
			logger.Info("GetStreamingNodeDistribution default to QueryNode", zap.Any("node_id", nodeID))
			batchResp, batchErr := s.GetQueryNodeDistribution(req.Context(), &querypb.GetQueryNodeDistributionRequest{
				Base:   commonpbutil.NewMsgBase(),
				NodeID: nodeID,
			})
			batchErr = merr.CheckRPCCall(batchResp, batchErr)
			// If batch fails or returns a non-OK status, check the reason.
			if batchErr != nil {
				// If the status is specifically a node not found error, return an empty distribution.
				if errors.Is(batchErr, merr.ErrNodeNotFound) {
					// Both streaming and batch nodes were not found.
					logger.Info("GetStreamingNodeDistribution ignore node not found", zap.Any("node_id", nodeID))
					dist = distributionResponse{
						ChannelNames:     []string{},
						SealedSegmentIDs: []string{},
					}
				} else {
					// Batch returned an error other than "NodeNotFound".
					logger.Info("GetStreamingNodeDistribution GetQueryNodeDistribution failed", zap.Error(batchErr))
					http.Error(w, fmt.Sprintf(`{"msg": "failed to get query node distribution, %s"}`, batchErr.Error()), http.StatusInternalServerError)
					return
				}
			} else {
				// Batch call succeeded. Populate with channel names and an empty sealed_segmentIDs.
				dist = distributionResponse{
					ChannelNames: batchResp.ChannelNames,
				}
			}
		} else {
			// Streaming returned an error other than "NodeNotFound".
			logger.Error("GetStreamingNodeDistribution failed to get wal distribution", zap.Error(streamingErr))
			http.Error(w, fmt.Sprintf(`{"msg": "failed to get streaming node distribution, %s"}`, streamingErr.Error()), http.StatusInternalServerError)
			return
		}
	} else {
		if streamingResp == nil {
			batchResp, batchErr := s.GetQueryNodeDistribution(req.Context(), &querypb.GetQueryNodeDistributionRequest{
				Base:   commonpbutil.NewMsgBase(),
				NodeID: nodeID,
			})
			batchErr = merr.CheckRPCCall(batchResp, batchErr)
			// If batch fails or returns a non-OK status, check the reason.
			if batchErr != nil {
				// Batch returned an error other than "NodeNotFound".
				logger.Info("GetStreamingNodeDistribution GetQueryNodeDistribution failed", zap.Error(batchErr))
				http.Error(w, fmt.Sprintf(`{"msg": "failed to get query node distribution, %s"}`, batchErr.Error()), http.StatusInternalServerError)
				return
			} else {
				// Batch call succeeded. Populate with channel names and an empty sealed_segmentIDs.
				dist = distributionResponse{
					ChannelNames: batchResp.ChannelNames,
				}
			}
		} else {
			// Streaming call succeeded. Populate with channel names and an empty sealed_segmentIDs.
			channelNames := make([]string, 0, len(streamingResp.Channels))
			for name := range streamingResp.Channels {
				channelNames = append(channelNames, name)
			}
			dist = distributionResponse{
				ChannelNames: channelNames,
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// Encode and write the response.
	if err := json.NewEncoder(w).Encode(dist); err != nil {
		http.Error(w, fmt.Sprintf(`{"msg": "failed to encode response, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	logger.Info("GetStreamingNodeDistribution success", zap.Any("response", dist))
}

// GetBatchNodeDistribution handles GET requests to retrieve node distribution.
// This handler should be registered to the new path.
func (s *mixCoordImpl) GetBatchNodeDistribution(w http.ResponseWriter, req *http.Request) {
	logger := log.With(zap.String("Scope", "Rolling"))
	if err := req.ParseForm(); err != nil {
		logger.Info("GetBatchNodeDistribution failed to parse form", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to parse form data, %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	nodeID, err := strconv.ParseInt(req.FormValue("node_id"), 10, 64)
	if err != nil {
		logger.Info("GetBatchNodeDistribution failed to parse form", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to get query node distribution, invalid node_id: %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	resp, err2 := s.GetQueryNodeDistribution(req.Context(), &querypb.GetQueryNodeDistributionRequest{
		Base:   commonpbutil.NewMsgBase(),
		NodeID: nodeID,
	})

	err = merr.CheckRPCCall(resp, err2)
	if err != nil {
		logger.Info("GetBatchNodeDistribution GetQueryNodeDistribution failed", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to get query node distribution, %s"}`, err.Error()), http.StatusInternalServerError)
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
		logger.Warn("GetBatchNodeDistribution failed to encode response", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to encode response, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	logger.Info("GetBatchNodeDistribution success", zap.Any("response", dist))
}

// HandleBatchBalanceStatus is the main handler for the unified endpoint.
func (s *mixCoordImpl) HandleBatchBalanceStatus(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		s.getBatchBalanceStatus(w, req)
	case http.MethodPut:
		s.controlBatchBalanceStatus(w, req)
	default:
		http.Error(w, `{"msg": "Method not allowed"}`, http.StatusMethodNotAllowed)
	}
}

func (s *mixCoordImpl) getQueryCoordChannelBalanceActive(ctx context.Context) (bool, error) {
	channelActivate, err := s.queryCoordServer.CheckChannelBalanceActive(ctx)
	if err != nil {
		return false, err
	}

	resp, err2 := s.CheckBalanceStatus(ctx, &querypb.CheckBalanceStatusRequest{
		Base: commonpbutil.NewMsgBase(),
	})
	if err2 != nil {
		return channelActivate, err2
	}

	if !merr.Ok(resp.GetStatus()) {
		return channelActivate, fmt.Errorf("%s", resp.GetStatus().GetReason())
	}

	return channelActivate && resp.IsActive, nil
}

func (s *mixCoordImpl) getQueryCoordBalanceActive(ctx context.Context) (bool, error) {
	resp, err := s.CheckBalanceStatus(ctx, &querypb.CheckBalanceStatusRequest{
		Base: commonpbutil.NewMsgBase(),
	})
	if err != nil {
		return false, err
	}
	if !merr.Ok(resp.GetStatus()) {
		return false, fmt.Errorf("%s", resp.GetStatus().GetReason())
	}
	return resp.IsActive, nil
}

// getBatchBalanceStatus handles GET requests to fetch the balance status.
func (s *mixCoordImpl) getBatchBalanceStatus(w http.ResponseWriter, req *http.Request) {
	logger := log.With(zap.String("Scope", "Rolling"))
	isActive, err := s.getQueryCoordBalanceActive(req.Context())
	if err != nil {
		logger.Warn("getBatchBalanceStatus getQueryCoordBalance failed", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to check balance status, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	balanceStatus := "suspended"
	if isActive {
		balanceStatus = "active"
	}
	logger.Info("getBatchBalanceStatus success", zap.Any("response", balanceStatus))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf(`{"msg": "OK", "status": "%v"}`, balanceStatus)))
}

func (s *mixCoordImpl) controlQueryCoordChannelBalanceStatus(ctx context.Context, status string) error {
	// Call the appropriate MixCoord method based on the status.
	var err error
	var errMsg string

	switch status {
	case "suspended":
		err = s.queryCoordServer.SuspendChannelBalance(ctx)
		errMsg = "failed to suspend balance"
	case "resumed", "active":
		err = s.queryCoordServer.ResumeChannelBalance(ctx)
		errMsg = "failed to resume balance"
	default:
		// If the status is not recognized, return an informative error immediately.
		// This avoids proceeding with an invalid state.
		err = fmt.Errorf("invalid status value: '%s'. Use 'suspended', 'resumed' or 'active'", status)
	}

	// --- Unified Error Handling ---
	// After the switch, we handle potential errors from the called method.

	// First, check if there was a low-level error during the method execution (e.g., network issue).
	if err != nil {
		// Wrap the original error with more context.
		return fmt.Errorf("%s: %w", errMsg, err)
	}
	// If no errors were encountered, the operation was successful. Return nil to indicate success.
	return nil
}

// controlBatchBalanceStatus handles PUT requests to suspend or resume balance.
func (s *mixCoordImpl) controlBatchBalanceStatus(w http.ResponseWriter, req *http.Request) {
	logger := log.With(zap.String("Scope", "Rolling"))
	// Define the request body struct for the PUT request.
	var requestBody struct {
		Status string `json:"status"`
	}

	// Decode the JSON from the request body.
	if err := json.NewDecoder(req.Body).Decode(&requestBody); err != nil {
		logger.Warn("ControlBatchBalanceStatus failed to decode request", zap.Error(err))
		http.Error(w, `{"msg": "Invalid request body"}`, http.StatusBadRequest)
		return
	}

	var resp *commonpb.Status
	var err error
	var errMsg string

	// Call the appropriate MixCoord method based on the status.
	switch requestBody.Status {
	case "suspended":
		resp, err = s.SuspendBalance(req.Context(), &querypb.SuspendBalanceRequest{
			Base: commonpbutil.NewMsgBase(),
		})
		errMsg = "failed to suspend balance"
	case "resumed", "active":
		resp, err = s.ResumeBalance(req.Context(), &querypb.ResumeBalanceRequest{
			Base: commonpbutil.NewMsgBase(),
		})
		errMsg = "failed to resume balance"
	default:
		logger.Warn("ControlBatchBalanceStatus invalid status", zap.String("status", requestBody.Status))
		http.Error(w, `{"msg": "Invalid status value. Use 'suspended', 'resumed' or 'active'."}`, http.StatusBadRequest)
		return
	}
	err = merr.CheckRPCCall(resp, err)
	if err != nil {
		logger.Warn("ControlBatchBalanceStatus failed", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "%s, %s"}`, errMsg, err.Error()), http.StatusInternalServerError)
		return
	}
	logger.Info("ControlBatchBalanceStatus success")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}

// HandleStreamingBalanceStatus is the main handler for the unified endpoint.
func (s *mixCoordImpl) HandleStreamingBalanceStatus(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		s.getStreamingBalanceStatus(w, req)
	case http.MethodPut:
		s.controlStreamingBalanceStatus(w, req)
	default:
		http.Error(w, `{"msg": "Method not allowed"}`, http.StatusMethodNotAllowed)
	}
}

// getStreamingBalanceStatus handles GET requests to fetch the balance status.
func (s *mixCoordImpl) getStreamingBalanceStatus(w http.ResponseWriter, req *http.Request) {
	logger := log.With(zap.String("Scope", "Rolling"))
	isSuspended, err := streaming.WAL().Balancer().IsRebalanceSuspended(req.Context())
	if err != nil && !errors.Is(err, snmanager.ErrStreamingServiceNotReady) {
		logger.Info("getStreamingBalanceStatus failed", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to get balance status: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	if errors.Is(err, snmanager.ErrStreamingServiceNotReady) {
		isSuspended = true
	}
	logger.Info("getStreamingBalanceStatus", zap.Any("suspended", isSuspended), zap.Error(err))

	active, err2 := s.getQueryCoordChannelBalanceActive(req.Context())
	if err2 == nil {
		isSuspended = isSuspended && !active
		logger.Info("getStreamingBalanceStatus suspended merge with queryCoord channel", zap.Any("suspended", isSuspended))
	} else {
		logger.Info("getStreamingBalanceStatus getQueryCoordChannelBalanceActive failed", zap.Error(err))
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
	logger.Info("getStreamingBalanceStatus success", zap.Any("status", status))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// controlStreamingBalanceStatus handles PUT requests to change the balance status.
func (s *mixCoordImpl) controlStreamingBalanceStatus(w http.ResponseWriter, req *http.Request) {
	var requestBody struct {
		Status string `json:"status"`
	}
	logger := log.With(zap.String("Scope", "Rolling"))

	if err := json.NewDecoder(req.Body).Decode(&requestBody); err != nil {
		logger.Info("controlStreamingBalanceStatus json decoder failed", zap.Error(err))
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
		logger.Info("controlStreamingBalanceStatus invalid status value", zap.String("status", requestBody.Status))
		http.Error(w, `{"msg": "Invalid status value. Use 'suspended', 'resumed' or 'active'."}`, http.StatusBadRequest)
		return
	}

	if err != nil {
		log.Info("controlStreamingBalanceStatus failed", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "%s"}`, errMsg), http.StatusInternalServerError)
		return
	}

	// For compatibility, this also forwards to QueryCoord to set the channel's balance status.
	err2 := s.controlQueryCoordChannelBalanceStatus(req.Context(), requestBody.Status)
	if err2 != nil {
		logger.Warn("controlStreamingBalanceStatus controlQueryCoordChannelBalanceStatus failed", zap.Error(err2))
		http.Error(w, err2.Error(), http.StatusInternalServerError)
		return
	}
	logger.Info("controlStreamingBalanceStatus success", zap.Any("status", requestBody.Status))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}

// HandleStreamingNodeStatus is the main handler that dispatches requests
// based on the HTTP method.
func (s *mixCoordImpl) HandleStreamingNodeStatus(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		s.handleGetStreamingNodeStatus(w, req)
	case http.MethodPut:
		s.handlePutStreamingNodeStatus(w, req)
	default:
		http.Error(w, `{"msg": "Method not allowed"}`, http.StatusMethodNotAllowed)
	}
}

// handleGetNodeStatus handles GET requests to retrieve a node's status.
func (s *mixCoordImpl) handleGetStreamingNodeStatus(w http.ResponseWriter, req *http.Request) {
	logger := log.With(zap.String("Scope", "Rolling"))
	// Parse the request form to access URL query parameters.
	if err := req.ParseForm(); err != nil {
		logger.Info("handleGetStreamingNodeStatus parse form failed", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to parse form, %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	// Access the query parameter from the populated req.Form field.
	nodeIDStr := req.Form.Get("node_id")
	if nodeIDStr == "" {
		logger.Info("handleGetStreamingNodeStatus missing node_id")
		http.Error(w, `{"msg": "node_id query parameter is required"}`, http.StatusBadRequest)
		return
	}

	nodeID, err := strconv.ParseInt(nodeIDStr, 10, 64)
	if err != nil {
		logger.Info("handleGetStreamingNodeStatus invalid node_id", zap.Error(err))
		http.Error(w, `{"msg": "Invalid node_id parameter"}`, http.StatusBadRequest)
		return
	}

	// 1. Call GetFrozenNodeIDs to get the list of suspended nodes.
	frozenNodeIDs, err := streaming.WAL().Balancer().GetFrozenNodeIDs(req.Context())
	if err != nil {
		logger.Info("handleGetStreamingNodeStatus getFrozenNodeIDs failed", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to get frozen nodes, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	logger.Info("handleGetStreamingNodeStatus getFrozenNodeIDs", zap.Any("frozen nodes", frozenNodeIDs))

	// 2. Use lo.Contains to check if the nodeID is in the list.
	isSuspended := lo.Contains(frozenNodeIDs, nodeID)

	// If the node is not in the streaming list, perform a fallback check on the batch service.
	if !isSuspended {
		suspended, err := s.queryCoordServer.IsNodeSuspended(req.Context(), nodeID)
		logger.Info("handleGetStreamingNodeStatus queryCoord IsNodeSuspended", zap.Any("suspended", suspended), zap.Error(err))
		if err != nil && !errors.Is(err, merr.ErrNodeNotFound) {
			logger.Info("handleGetStreamingNodeStatus queryCoord suspended failed", zap.Error(err))
			http.Error(w, fmt.Sprintf(`{"msg": "failed to get batch node status, %s"}`, err.Error()), http.StatusInternalServerError)
			return
		}
		if err == nil {
			isSuspended = suspended
		} else {
			logger.Info("handleGetStreamingNodeStatus queryCoord complain node not found")
		}
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
	logger.Info("handleGetStreamingNodeStatus success", zap.Any("responseBody", responseBody))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(responseBody)
}

// handlePutNodeStatus handles PUT requests to update a node's status.
func (s *mixCoordImpl) handlePutStreamingNodeStatus(w http.ResponseWriter, req *http.Request) {
	var requestBody struct {
		Status string `json:"status"`
		NodeID int64  `json:"node_id"`
	}
	logger := log.With(zap.String("Scope", "Rolling"))
	if err := json.NewDecoder(req.Body).Decode(&requestBody); err != nil {
		logger.Info("handlePutStreamingNodeStatus json decoder failed", zap.Error(err))
		http.Error(w, `{"msg": "Invalid request body"}`, http.StatusBadRequest)
		return
	}

	if requestBody.NodeID == 0 {
		logger.Info("handlePutStreamingNodeStatus missing node_id")
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
		logger.Info("handlePutStreamingNodeStatus invalid status value", zap.Any("status", requestBody.Status))
		http.Error(w, `{"msg": "Invalid status value. Use 'suspended' or 'active'."}`, http.StatusBadRequest)
		return
	}

	if err != nil {
		logger.Info("handlePutStreamingNodeStatus failed", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "%s, %s"}`, errMsg, err.Error()), http.StatusInternalServerError)
		return
	}
	err = s.handleQueryNodeStatusUpdate(req.Context(), requestBody.NodeID, requestBody.Status)
	if err != nil && !errors.Is(err, merr.ErrNodeNotFound) {
		logger.Info("handlePutStreamingNodeStatus handleQueryNodeStatusUpdate update failed", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "%s", "%s"}`, errMsg, err.Error()), http.StatusInternalServerError)
		return
	}
	if err != nil {
		logger.Info("handlePutStreamingNodeStatus QueryCoord ingore node")
	}
	logger.Info("handlePutStreamingNodeStatus success", zap.Any("status", requestBody.Status))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}

// HandleBatchNodeStatus is the main handler that dispatches requests
// based on the HTTP method.
func (s *mixCoordImpl) HandleBatchNodeStatus(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		s.handleGetBatchNodeStatus(w, req)
	case http.MethodPut:
		s.handlePutBatchNodeStatus(w, req)
	default:
		http.Error(w, `{"msg": "Method not allowed"}`, http.StatusMethodNotAllowed)
	}
}

// handleGetBatchNodeStatus handles GET requests to retrieve a node's status.
func (s *mixCoordImpl) handleGetBatchNodeStatus(w http.ResponseWriter, req *http.Request) {
	logger := log.With(zap.String("Scope", "Rolling"))
	// Parse the request form to access URL query parameters.
	if err := req.ParseForm(); err != nil {
		logger.Warn("handleGetBatchNodeStatus", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to parse form, %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	// Access the query parameter from the populated req.Form field.
	nodeIDStr := req.Form.Get("node_id")
	if nodeIDStr == "" {
		logger.Warn("handleGetBatchNodeStatus missing node_id")
		http.Error(w, `{"msg": "node_id query parameter is required"}`, http.StatusBadRequest)
		return
	}

	nodeID, err := strconv.ParseInt(nodeIDStr, 10, 64)
	if err != nil {
		logger.Info("handleGetBatchNodeStatus invalid node_id", zap.Error(err))
		http.Error(w, `{"msg": "Invalid node_id parameter"}`, http.StatusBadRequest)
		return
	}

	// Call the gRPC method to check the node's status
	isSuspended, err := s.queryCoordServer.IsNodeSuspended(req.Context(), nodeID)
	if err != nil {
		logger.Info("handleGetBatchNodeStatus queryCoord IsNodeSuspended", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to get node status, %s"}`, err.Error()), http.StatusInternalServerError)
		return
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
	logger.Info("handleGetBatchNodeStatus success", zap.Any("responseBody", responseBody))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(responseBody)
}

func (s *mixCoordImpl) handleQueryNodeStatusUpdate(ctx context.Context, nodeID int64, status string) error {
	var resp *commonpb.Status
	var err error
	var errMsg string

	switch status {
	case "suspended":
		resp, err = s.SuspendNode(ctx, &querypb.SuspendNodeRequest{
			Base:   commonpbutil.NewMsgBase(),
			NodeID: nodeID,
		})
		errMsg = "failed to suspend node"
	case "resumed", "active":
		resp, err = s.ResumeNode(ctx, &querypb.ResumeNodeRequest{
			Base:   commonpbutil.NewMsgBase(),
			NodeID: nodeID,
		})
		errMsg = "failed to resume node"
	default:
		errMsg = "invalid status value. Use 'suspended', 'resumed' or 'active'"
		err = fmt.Errorf("%s", errMsg)
	}
	err = merr.CheckRPCCall(resp, err)
	if err != nil {
		err = fmt.Errorf("%s: %w", errMsg, err)
	}
	return err
}

// handlePutBatchNodeStatus handles PUT requests to change the node status.
func (s *mixCoordImpl) handlePutBatchNodeStatus(w http.ResponseWriter, req *http.Request) {
	logger := log.With(zap.String("Scope", "Rolling"))
	var requestBody struct {
		NodeID int64  `json:"node_id"`
		Status string `json:"status"`
	}

	if err := json.NewDecoder(req.Body).Decode(&requestBody); err != nil {
		logger.Info("handlePutBatchNodeStatus json decoder failed", zap.Error(err))
		http.Error(w, `{"msg": "Invalid request body"}`, http.StatusBadRequest)
		return
	}

	if requestBody.NodeID == 0 {
		logger.Info("handlePutBatchNodeStatus missing node_id")
		http.Error(w, `{"msg": "node_id is required"}`, http.StatusBadRequest)
		return
	}

	// Call the new helper function
	if err := s.handleQueryNodeStatusUpdate(req.Context(), requestBody.NodeID, requestBody.Status); err != nil {
		logger.Info("handlePutBatchNodeStatus queryCoord handleQueryNodeStatus", zap.Error(err))
		// Handle errors returned by the helper function
		if strings.Contains(err.Error(), "invalid status value") {
			logger.Info("handlePutBatchNodeStatus invalid status", zap.Error(err))
			http.Error(w, `{"msg": "Invalid status value. Use 'suspended', 'resumed' or 'active'."}`, http.StatusBadRequest)
			return
		}
		logger.Info("handlePutBatchNodeStatus queryCoord handleQueryNodeStatus", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	logger.Info("handlePutBatchNodeStatus success", zap.Any("status", requestBody.Status))
	// Success response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}

func (s *mixCoordImpl) TransferBatchSegment(w http.ResponseWriter, req *http.Request) {
	// Defines the request body struct with all parameters.
	var requestBody struct {
		SourceNodeID int64 `json:"source_node_id"`
		TargetNodeID int64 `json:"target_node_id,omitempty"`
		SegmentID    int64 `json:"segment_id,omitempty"`
		CopyMode     *bool `json:"copy_mode,omitempty"`
	}
	logger := log.With(zap.String("Scope", "Rolling"))
	// Decodes the JSON from the request body.
	if err := json.NewDecoder(req.Body).Decode(&requestBody); err != nil {
		logger.Info("TransferBatchSegment json decoder failed", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "Invalid request body, %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	// Check if the mandatory field is provided.
	if requestBody.SourceNodeID == 0 {
		logger.Info("TransferBatchSegment missing source_node_id")
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

	resp, err := s.TransferSegment(req.Context(), request)
	err = merr.CheckRPCCall(resp, err)
	if err != nil {
		logger.Info("TransferBatchSegment failed", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to transfer segment, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	logger.Info("TransferBatchSegment success")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}

// TransferStreamingChannel handles the transfer and defreeze operation.
func (s *mixCoordImpl) TransferStreamingChannel(w http.ResponseWriter, req *http.Request) {
	logger := log.With(zap.String("Scope", "Rolling"))
	if req.Method != http.MethodPost {
		logger.Info("TransferStreamingChannel invalid method")
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
		logger.Info("TransferStreamingChannel json decoder failed", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "Invalid request body, %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	if requestBody.SourceNodeID == 0 {
		logger.Info("TransferStreamingChannel missing source_node_id")
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

	resp, err := s.TransferChannel(req.Context(), transferReq)
	err = merr.CheckRPCCall(resp, err)
	if err != nil {
		logger.Info("TransferStreamingChannel failed", zap.Error(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to transfer channel, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	logger.Info("TransferStreamingChannel success")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}
