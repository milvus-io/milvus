package coordinator

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	management "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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
			// WAL
			{management.WALAlterPath, s.HandleAlterWAL},
			// config
			{management.ConfigAlterPath, s.HandleAlterConfig},
			{management.ConfigGetPath, s.HandleGetConfig},
			// ops
			{management.ReplicaLoadConfigCompliancePath, s.HandleReplicaLoadConfigCompliance},
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
	logger := mlog.With(mlog.String("Scope", "Rolling"))
	resp, err := s.datacoordServer.GetGcStatus(req.Context())
	if err != nil {
		logger.Info(req.Context(), "failed to GetGcStatus", mlog.Err(err))
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
	logger.Info(req.Context(), "GetGcStatus success", mlog.Any("resp", jsonResponse))
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

	logger := mlog.With(mlog.String("Scope", "Rolling"))

	// Parses the JSON from the request body.
	if err := json.NewDecoder(req.Body).Decode(&requestBody); err != nil {
		logger.Info(req.Context(), "ControlDataCoordGC failed to decode body", mlog.Err(err))
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
		logger.Info(req.Context(), "ControlDataCoordGC invalid status value", mlog.Any("status", requestBody.Status))
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
		logger.Info(req.Context(), "ControlDataCoordGC GcControl failed", mlog.Err(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to control garbage collection: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	logger.Info(req.Context(), "ControlDataCoordGC GcControl success", mlog.String("gcCommand", gcCommand.String()))

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}

// HandleStreamingNodes handles GET requests to list streaming and query nodes.
func (s *mixCoordImpl) HandleStreamingNodes(w http.ResponseWriter, req *http.Request) {
	logger := mlog.With(mlog.String("Scope", "Rolling"))

	// 1. Fetch data from the streaming service.
	streamingNodes, err := streaming.WAL().Balancer().ListStreamingNode(req.Context())
	if err != nil {
		logger.Info(req.Context(), "HandleStreamingNodes failed to list streaming nodes", mlog.Err(err))
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
		logger.Info(req.Context(), "HandleStreamingNodes failed to get frozen nodes", mlog.Err(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to get frozen nodes, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	logger.Info(req.Context(), "HandleStreamingNodes get frozen nodes", mlog.Any("frozen nodes", frozenNodeIDs))
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
		logger.Info(req.Context(), "HandleStreamingNodes failed to get query nodes", mlog.Err(err))
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
			logger.Info(req.Context(), "HandleStreamingNodes GetQueryNodeDistribution failed", mlog.Err(err))
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
	mlog.Info(req.Context(), "HandleStreamingNodes success", mlog.Any("response", res))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(res); err != nil {
		logger.Info(req.Context(), "HandleStreamingNodes failed to encode response", mlog.Err(err))
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
		return nil, merr.Wrapf(err, "failed to list query nodes")
	}
	if !merr.Ok(resp.GetStatus()) {
		return nil, merr.Wrapf(merr.Error(resp.GetStatus()), "failed to list query nodes")
	}
	return resp, nil
}

func (s *mixCoordImpl) ListBatchQueryNodes(w http.ResponseWriter, req *http.Request) {
	logger := mlog.With(mlog.String("Scope", "Rolling"))
	resp, err := s.getQueryNodes(req.Context())
	if err != nil {
		logger.Info(req.Context(), "ListBatchQueryNodes failed to list query nodes", mlog.Err(err))
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"msg": "failed to list query node, %s"}`, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	// skip marshal status to output
	resp.Status = nil
	bytes, err := json.Marshal(resp)
	if err != nil {
		logger.Info(req.Context(), "ListBatchQueryNodes failed to encode response", mlog.Err(err))
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, `{"msg": "failed to list query node, %s"}`, err.Error())
		return
	}
	logger.Info(req.Context(), "ListBatchQueryNodes success", mlog.Any("response", string(bytes)))
	w.Write(bytes)
}

// GetStreamingNodeDistribution handles GET requests to retrieve streaming node distribution.
func (s *mixCoordImpl) GetStreamingNodeDistribution(w http.ResponseWriter, req *http.Request) {
	logger := mlog.With(mlog.String("Scope", "Rolling"))
	if err := req.ParseForm(); err != nil { //nolint:gosec // internal admin endpoint
		logger.Info(req.Context(), "GetStreamingNodeDistribution failed to parse form", mlog.Err(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to parse form data, %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	nodeID, err := strconv.ParseInt(req.FormValue("node_id"), 10, 64) //nolint:gosec // internal admin endpoint
	if err != nil {
		logger.Info(req.Context(), "GetStreamingNodeDistribution failed to parse form", mlog.Err(err))
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
			logger.Info(req.Context(), "GetStreamingNodeDistribution default to QueryNode", mlog.Any("node_id", nodeID))
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
					logger.Info(req.Context(), "GetStreamingNodeDistribution ignore node not found", mlog.Any("node_id", nodeID))
					dist = distributionResponse{
						ChannelNames:     []string{},
						SealedSegmentIDs: []string{},
					}
				} else {
					// Batch returned an error other than "NodeNotFound".
					logger.Info(req.Context(), "GetStreamingNodeDistribution GetQueryNodeDistribution failed", mlog.Err(batchErr))
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
			logger.Error(req.Context(), "GetStreamingNodeDistribution failed to get wal distribution", mlog.Err(streamingErr))
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
				logger.Info(req.Context(), "GetStreamingNodeDistribution GetQueryNodeDistribution failed", mlog.Err(batchErr))
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
	logger.Info(req.Context(), "GetStreamingNodeDistribution success", mlog.Any("response", dist))
}

// GetBatchNodeDistribution handles GET requests to retrieve node distribution.
// This handler should be registered to the new path.
func (s *mixCoordImpl) GetBatchNodeDistribution(w http.ResponseWriter, req *http.Request) {
	logger := mlog.With(mlog.String("Scope", "Rolling"))
	if err := req.ParseForm(); err != nil { //nolint:gosec // internal admin endpoint
		logger.Info(req.Context(), "GetBatchNodeDistribution failed to parse form", mlog.Err(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to parse form data, %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	nodeID, err := strconv.ParseInt(req.FormValue("node_id"), 10, 64) //nolint:gosec // internal admin endpoint
	if err != nil {
		logger.Info(req.Context(), "GetBatchNodeDistribution failed to parse form", mlog.Err(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to get query node distribution, invalid node_id: %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	resp, err2 := s.GetQueryNodeDistribution(req.Context(), &querypb.GetQueryNodeDistributionRequest{
		Base:   commonpbutil.NewMsgBase(),
		NodeID: nodeID,
	})

	err = merr.CheckRPCCall(resp, err2)
	if err != nil {
		logger.Info(req.Context(), "GetBatchNodeDistribution GetQueryNodeDistribution failed", mlog.Err(err))
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
		logger.Warn(req.Context(), "GetBatchNodeDistribution failed to encode response", mlog.Err(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to encode response, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	logger.Info(req.Context(), "GetBatchNodeDistribution success", mlog.Any("response", dist))
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
		return channelActivate, merr.Error(resp.GetStatus())
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
		return false, merr.Error(resp.GetStatus())
	}
	return resp.IsActive, nil
}

// getBatchBalanceStatus handles GET requests to fetch the balance status.
func (s *mixCoordImpl) getBatchBalanceStatus(w http.ResponseWriter, req *http.Request) {
	logger := mlog.With(mlog.String("Scope", "Rolling"))
	isActive, err := s.getQueryCoordBalanceActive(req.Context())
	if err != nil {
		logger.Warn(req.Context(), "getBatchBalanceStatus getQueryCoordBalance failed", mlog.Err(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to check balance status, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	balanceStatus := "suspended"
	if isActive {
		balanceStatus = "active"
	}
	logger.Info(req.Context(), "getBatchBalanceStatus success", mlog.Any("response", balanceStatus))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, `{"msg": "OK", "status": "%v"}`, balanceStatus)
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
		err = merr.WrapErrParameterInvalidMsg("invalid status value: '%s'. Use 'suspended', 'resumed' or 'active'", status)
	}

	// --- Unified Error Handling ---
	// After the switch, we handle potential errors from the called method.

	// First, check if there was a low-level error during the method execution (e.g., network issue).
	if err != nil {
		// Wrap the original error with more context.
		return merr.Wrap(err, errMsg)
	}
	// If no errors were encountered, the operation was successful. Return nil to indicate success.
	return nil
}

// controlBatchBalanceStatus handles PUT requests to suspend or resume balance.
func (s *mixCoordImpl) controlBatchBalanceStatus(w http.ResponseWriter, req *http.Request) {
	logger := mlog.With(mlog.String("Scope", "Rolling"))
	// Define the request body struct for the PUT request.
	var requestBody struct {
		Status string `json:"status"`
	}

	// Decode the JSON from the request body.
	if err := json.NewDecoder(req.Body).Decode(&requestBody); err != nil {
		logger.Warn(req.Context(), "ControlBatchBalanceStatus failed to decode request", mlog.Err(err))
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
		logger.Warn(req.Context(), "ControlBatchBalanceStatus invalid status", mlog.String("status", requestBody.Status))
		http.Error(w, `{"msg": "Invalid status value. Use 'suspended', 'resumed' or 'active'."}`, http.StatusBadRequest)
		return
	}
	err = merr.CheckRPCCall(resp, err)
	if err != nil {
		logger.Warn(req.Context(), "ControlBatchBalanceStatus failed", mlog.Err(err))
		http.Error(w, fmt.Sprintf(`{"msg": "%s, %s"}`, errMsg, err.Error()), http.StatusInternalServerError)
		return
	}
	logger.Info(req.Context(), "ControlBatchBalanceStatus success")
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
	logger := mlog.With(mlog.String("Scope", "Rolling"))
	isSuspended, err := streaming.WAL().Balancer().IsRebalanceSuspended(req.Context())
	if err != nil && !errors.Is(err, snmanager.ErrStreamingServiceNotReady) {
		logger.Info(req.Context(), "getStreamingBalanceStatus failed", mlog.Err(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to get balance status: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	if errors.Is(err, snmanager.ErrStreamingServiceNotReady) {
		isSuspended = true
	}
	logger.Info(req.Context(), "getStreamingBalanceStatus", mlog.Any("suspended", isSuspended), mlog.Err(err))

	active, err2 := s.getQueryCoordChannelBalanceActive(req.Context())
	if err2 == nil {
		isSuspended = isSuspended && !active
		logger.Info(req.Context(), "getStreamingBalanceStatus suspended merge with queryCoord channel", mlog.Any("suspended", isSuspended))
	} else {
		logger.Info(req.Context(), "getStreamingBalanceStatus getQueryCoordChannelBalanceActive failed", mlog.Err(err))
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
	logger.Info(req.Context(), "getStreamingBalanceStatus success", mlog.Any("status", status))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(response)
}

// controlStreamingBalanceStatus handles PUT requests to change the balance status.
func (s *mixCoordImpl) controlStreamingBalanceStatus(w http.ResponseWriter, req *http.Request) {
	var requestBody struct {
		Status string `json:"status"`
	}
	logger := mlog.With(mlog.String("Scope", "Rolling"))

	if err := json.NewDecoder(req.Body).Decode(&requestBody); err != nil {
		logger.Info(req.Context(), "controlStreamingBalanceStatus json decoder failed", mlog.Err(err))
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
		logger.Info(req.Context(), "controlStreamingBalanceStatus invalid status value", mlog.String("status", requestBody.Status))
		http.Error(w, `{"msg": "Invalid status value. Use 'suspended', 'resumed' or 'active'."}`, http.StatusBadRequest)
		return
	}

	if err != nil {
		mlog.Info(req.Context(), "controlStreamingBalanceStatus failed", mlog.Err(err))
		http.Error(w, fmt.Sprintf(`{"msg": "%s"}`, errMsg), http.StatusInternalServerError)
		return
	}

	// For compatibility, this also forwards to QueryCoord to set the channel's balance status.
	err2 := s.controlQueryCoordChannelBalanceStatus(req.Context(), requestBody.Status)
	if err2 != nil {
		logger.Warn(req.Context(), "controlStreamingBalanceStatus controlQueryCoordChannelBalanceStatus failed", mlog.Err(err2))
		http.Error(w, err2.Error(), http.StatusInternalServerError)
		return
	}
	logger.Info(req.Context(), "controlStreamingBalanceStatus success", mlog.Any("status", requestBody.Status))

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
	logger := mlog.With(mlog.String("Scope", "Rolling"))
	// Parse the request form to access URL query parameters.
	if err := req.ParseForm(); err != nil { //nolint:gosec // internal admin endpoint
		logger.Info(req.Context(), "handleGetStreamingNodeStatus parse form failed", mlog.Err(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to parse form, %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	// Access the query parameter from the populated req.Form field.
	nodeIDStr := req.Form.Get("node_id")
	if nodeIDStr == "" {
		logger.Info(req.Context(), "handleGetStreamingNodeStatus missing node_id")
		http.Error(w, `{"msg": "node_id query parameter is required"}`, http.StatusBadRequest)
		return
	}

	nodeID, err := strconv.ParseInt(nodeIDStr, 10, 64)
	if err != nil {
		logger.Info(req.Context(), "handleGetStreamingNodeStatus invalid node_id", mlog.Err(err))
		http.Error(w, `{"msg": "Invalid node_id parameter"}`, http.StatusBadRequest)
		return
	}

	// 1. Call GetFrozenNodeIDs to get the list of suspended nodes.
	frozenNodeIDs, err := streaming.WAL().Balancer().GetFrozenNodeIDs(req.Context())
	if err != nil {
		logger.Info(req.Context(), "handleGetStreamingNodeStatus getFrozenNodeIDs failed", mlog.Err(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to get frozen nodes, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	logger.Info(req.Context(), "handleGetStreamingNodeStatus getFrozenNodeIDs", mlog.Any("frozen nodes", frozenNodeIDs))

	// 2. Use lo.Contains to check if the nodeID is in the list.
	isSuspended := lo.Contains(frozenNodeIDs, nodeID)

	// If the node is not in the streaming list, perform a fallback check on the batch service.
	if !isSuspended {
		suspended, err := s.queryCoordServer.IsNodeSuspended(req.Context(), nodeID)
		logger.Info(req.Context(), "handleGetStreamingNodeStatus queryCoord IsNodeSuspended", mlog.Any("suspended", suspended), mlog.Err(err))
		if err != nil && !errors.Is(err, merr.ErrNodeNotFound) {
			logger.Info(req.Context(), "handleGetStreamingNodeStatus queryCoord suspended failed", mlog.Err(err))
			http.Error(w, fmt.Sprintf(`{"msg": "failed to get batch node status, %s"}`, err.Error()), http.StatusInternalServerError)
			return
		}
		if err == nil {
			isSuspended = suspended
		} else {
			logger.Info(req.Context(), "handleGetStreamingNodeStatus queryCoord complain node not found")
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
	logger.Info(req.Context(), "handleGetStreamingNodeStatus success", mlog.Any("responseBody", responseBody))
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
	logger := mlog.With(mlog.String("Scope", "Rolling"))
	if err := json.NewDecoder(req.Body).Decode(&requestBody); err != nil {
		logger.Info(req.Context(), "handlePutStreamingNodeStatus json decoder failed", mlog.Err(err))
		http.Error(w, `{"msg": "Invalid request body"}`, http.StatusBadRequest)
		return
	}

	if requestBody.NodeID == 0 {
		logger.Info(req.Context(), "handlePutStreamingNodeStatus missing node_id")
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
		logger.Info(req.Context(), "handlePutStreamingNodeStatus invalid status value", mlog.Any("status", requestBody.Status))
		http.Error(w, `{"msg": "Invalid status value. Use 'suspended' or 'active'."}`, http.StatusBadRequest)
		return
	}

	if err != nil {
		logger.Info(req.Context(), "handlePutStreamingNodeStatus failed", mlog.Err(err))
		http.Error(w, fmt.Sprintf(`{"msg": "%s, %s"}`, errMsg, err.Error()), http.StatusInternalServerError)
		return
	}
	err = s.handleQueryNodeStatusUpdate(req.Context(), requestBody.NodeID, requestBody.Status)
	if err != nil && !errors.Is(err, merr.ErrNodeNotFound) {
		logger.Info(req.Context(), "handlePutStreamingNodeStatus handleQueryNodeStatusUpdate update failed", mlog.Err(err))
		http.Error(w, fmt.Sprintf(`{"msg": "%s", "%s"}`, errMsg, err.Error()), http.StatusInternalServerError)
		return
	}
	if err != nil {
		logger.Info(req.Context(), "handlePutStreamingNodeStatus QueryCoord ingore node")
	}
	logger.Info(req.Context(), "handlePutStreamingNodeStatus success", mlog.Any("status", requestBody.Status))
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
	logger := mlog.With(mlog.String("Scope", "Rolling"))
	// Parse the request form to access URL query parameters.
	if err := req.ParseForm(); err != nil { //nolint:gosec // internal admin endpoint
		logger.Warn(req.Context(), "handleGetBatchNodeStatus", mlog.Err(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to parse form, %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	// Access the query parameter from the populated req.Form field.
	nodeIDStr := req.Form.Get("node_id")
	if nodeIDStr == "" {
		logger.Warn(req.Context(), "handleGetBatchNodeStatus missing node_id")
		http.Error(w, `{"msg": "node_id query parameter is required"}`, http.StatusBadRequest)
		return
	}

	nodeID, err := strconv.ParseInt(nodeIDStr, 10, 64)
	if err != nil {
		logger.Info(req.Context(), "handleGetBatchNodeStatus invalid node_id", mlog.Err(err))
		http.Error(w, `{"msg": "Invalid node_id parameter"}`, http.StatusBadRequest)
		return
	}

	// Call the gRPC method to check the node's status
	isSuspended, err := s.queryCoordServer.IsNodeSuspended(req.Context(), nodeID)
	if err != nil {
		logger.Info(req.Context(), "handleGetBatchNodeStatus queryCoord IsNodeSuspended", mlog.Err(err))
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
	logger.Info(req.Context(), "handleGetBatchNodeStatus success", mlog.Any("responseBody", responseBody))
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
		err = merr.WrapErrParameterInvalidMsg("%s", errMsg)
	}
	err = merr.CheckRPCCall(resp, err)
	if err != nil {
		err = merr.Wrap(err, errMsg)
	}
	return err
}

// handlePutBatchNodeStatus handles PUT requests to change the node status.
func (s *mixCoordImpl) handlePutBatchNodeStatus(w http.ResponseWriter, req *http.Request) {
	logger := mlog.With(mlog.String("Scope", "Rolling"))
	var requestBody struct {
		NodeID int64  `json:"node_id"`
		Status string `json:"status"`
	}

	if err := json.NewDecoder(req.Body).Decode(&requestBody); err != nil {
		logger.Info(req.Context(), "handlePutBatchNodeStatus json decoder failed", mlog.Err(err))
		http.Error(w, `{"msg": "Invalid request body"}`, http.StatusBadRequest)
		return
	}

	if requestBody.NodeID == 0 {
		logger.Info(req.Context(), "handlePutBatchNodeStatus missing node_id")
		http.Error(w, `{"msg": "node_id is required"}`, http.StatusBadRequest)
		return
	}

	// Call the new helper function
	if err := s.handleQueryNodeStatusUpdate(req.Context(), requestBody.NodeID, requestBody.Status); err != nil {
		logger.Info(req.Context(), "handlePutBatchNodeStatus queryCoord handleQueryNodeStatus", mlog.Err(err))
		// Handle errors returned by the helper function
		if strings.Contains(err.Error(), "invalid status value") {
			logger.Info(req.Context(), "handlePutBatchNodeStatus invalid status", mlog.Err(err))
			http.Error(w, `{"msg": "Invalid status value. Use 'suspended', 'resumed' or 'active'."}`, http.StatusBadRequest)
			return
		}
		logger.Info(req.Context(), "handlePutBatchNodeStatus queryCoord handleQueryNodeStatus", mlog.Err(err))
		http.Error(w, fmt.Sprintf(`{"msg": "%s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	logger.Info(req.Context(), "handlePutBatchNodeStatus success", mlog.Any("status", requestBody.Status))
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
	logger := mlog.With(mlog.String("Scope", "Rolling"))
	// Decodes the JSON from the request body.
	if err := json.NewDecoder(req.Body).Decode(&requestBody); err != nil {
		logger.Info(req.Context(), "TransferBatchSegment json decoder failed", mlog.Err(err))
		http.Error(w, fmt.Sprintf(`{"msg": "Invalid request body, %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	// Check if the mandatory field is provided.
	if requestBody.SourceNodeID == 0 {
		logger.Info(req.Context(), "TransferBatchSegment missing source_node_id")
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
		logger.Info(req.Context(), "TransferBatchSegment failed", mlog.Err(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to transfer segment, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	logger.Info(req.Context(), "TransferBatchSegment success")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}

// TransferStreamingChannel handles the transfer and defreeze operation.
func (s *mixCoordImpl) TransferStreamingChannel(w http.ResponseWriter, req *http.Request) {
	logger := mlog.With(mlog.String("Scope", "Rolling"))
	if req.Method != http.MethodPost {
		logger.Info(req.Context(), "TransferStreamingChannel invalid method")
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
		logger.Info(req.Context(), "TransferStreamingChannel json decoder failed", mlog.Err(err))
		http.Error(w, fmt.Sprintf(`{"msg": "Invalid request body, %s"}`, err.Error()), http.StatusBadRequest)
		return
	}

	if requestBody.SourceNodeID == 0 {
		logger.Info(req.Context(), "TransferStreamingChannel missing source_node_id")
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
		logger.Info(req.Context(), "TransferStreamingChannel failed", mlog.Err(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to transfer channel, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}
	logger.Info(req.Context(), "TransferStreamingChannel success")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}

// HandleAlterWAL handles POST requests to alter the Write-Ahead Log (WAL) implementation.
// This endpoint broadcasts an AlterWALMessage to all active pChannels to switch WAL across the cluster.
func (s *mixCoordImpl) HandleAlterWAL(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, `{"msg": "Method not allowed, use POST"}`, http.StatusMethodNotAllowed)
		return
	}

	logger := mlog.With(mlog.String("Scope", "WAL"))

	var requestBody struct {
		TargetWALName string            `json:"target_wal_name"`  // e.g., "woodpecker", "kafka", "pulsar", "rocksmq"
		Config        map[string]string `json:"config,omitempty"` // Optional config for target WAL
	}

	if err := json.NewDecoder(req.Body).Decode(&requestBody); err != nil {
		logger.Info(req.Context(), "HandleAlterWAL failed to decode request body", mlog.Err(err))
		http.Error(w, `{"msg": "Invalid request body"}`, http.StatusBadRequest)
		return
	}

	if requestBody.TargetWALName == "" {
		logger.Info(req.Context(), "HandleAlterWAL missing target_wal_name")
		http.Error(w, `{"msg": "target_wal_name is required"}`, http.StatusBadRequest)
		return
	}

	targetWAL := message.NewWALName(strings.ToLower(requestBody.TargetWALName))
	if targetWAL == message.WALNameUnknown {
		logger.Info(req.Context(), "HandleAlterWAL unknown target_wal_name")
		http.Error(w, `{"msg": "unknown target_wal_name"}`, http.StatusBadRequest)
		return
	}

	// Check if targetWALName is the same as current mq.type
	// GetValue() will automatically resolve from all config sources including etcd
	currentMQType := paramtable.Get().MQCfg.Type.GetValue()
	if currentMQType != "" && currentMQType != "default" {
		// Convert persisted mq.type string to WALName
		currentWALFromConfig := message.NewWALName(strings.ToLower(currentMQType))
		if currentWALFromConfig != message.WALNameUnknown && currentWALFromConfig == targetWAL {
			logger.Info(req.Context(), "HandleAlterWAL target WAL is same as current mq.type",
				mlog.String("currentMQType", currentMQType),
				mlog.String("targetWAL", requestBody.TargetWALName))
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `{"msg": "target WAL type '%s' is already configured, no change needed"}`, currentMQType)
			return
		}
	}

	logger.Info(req.Context(), "HandleAlterWAL start",
		mlog.String("targetWAL", requestBody.TargetWALName),
		mlog.Any("config", requestBody.Config))

	if err := s.broadcastAlterWALMessage(req.Context(), commonpb.WALName(targetWAL), requestBody.Config); err != nil {
		logger.Info(req.Context(), "HandleAlterWAL failed to broadcast AlterWALMessage",
			mlog.String("targetWAL", requestBody.TargetWALName),
			mlog.Err(err))
		http.Error(w, fmt.Sprintf(`{"msg": "failed to broadcast AlterWALMessage, %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	logger.Info(req.Context(), "HandleAlterWAL success", mlog.String("targetWAL", requestBody.TargetWALName))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"msg": "OK"}`))
}

// broadcastAlterWALMessage broadcasts an AlterWALMessage to all active pChannels.
func (s *mixCoordImpl) broadcastAlterWALMessage(ctx context.Context, targetWALName commonpb.WALName, config map[string]string) error {
	logger := mlog.With(mlog.String("Scope", "WAL"), mlog.Stringer("targetWAL", targetWALName))

	// Start broadcast with an exclusive cluster resource key to ensure only one WAL switch operation at a time
	broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusiveClusterResourceKey())
	if err != nil {
		if errors.Is(err, broadcast.ErrNotPrimary) {
			logger.Info(ctx, "broadcastAlterWALMessage failed, current cluster is not primary", mlog.Err(err))
			return errors.Wrap(err, "current cluster is not primary, cannot perform WAL switch")
		}
		logger.Info(ctx, "broadcastAlterWALMessage failed to start broadcast", mlog.Err(err))
		return errors.Wrap(err, "failed to start broadcast")
	}
	defer broadcaster.Close()

	// Create AlterWAL broadcast message
	broadcastMsg, err := message.NewAlterWALMessageBuilderV2().
		WithHeader(&message.AlterWALMessageHeader{
			TargetWalName: targetWALName,
			Config:        config,
		}).
		WithBody(&message.AlterWALMessageBody{}).
		WithClusterLevelBroadcast(channel.GetClusterChannels()).
		BuildBroadcast()
	if err != nil {
		logger.Info(ctx, "broadcastAlterWALMessage failed to build broadcast message", mlog.Err(err))
		return errors.Wrap(err, "failed to build broadcast message")
	}

	// Broadcast the message to all pChannels
	result, err := broadcaster.Broadcast(ctx, broadcastMsg)
	if err != nil {
		logger.Info(ctx, "broadcastAlterWALMessage failed to broadcast message", mlog.Err(err))
		return errors.Wrap(err, "failed to broadcast message")
	}

	logger.Info(ctx, "broadcastAlterWALMessage success",
		mlog.Int("pChannelCount", len(result.AppendResults)),
		mlog.Uint64("broadcastID", result.BroadcastID))

	return nil
}

// HandleAlterConfig handles POST requests to alter configuration items.
// Immutable configurations cannot be modified through this endpoint.
// For mqtype modifications, use the alterWAL endpoint instead.
//
// Each config entry has a key and an optional value pointer:
//   - value present (including empty string): set the config
//   - value absent (null/omitted): reset the config (delete from etcd, revert to default)
//
// Supported request formats:
//
//	Batch format: {"configs": [{"key": "k1", "value": "v1"}, {"key": "k2"}]}
//	Legacy single format: {"key": "config.key", "value": "value"}
func (s *mixCoordImpl) HandleAlterConfig(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writeJSONError(writer, "Method not allowed, use POST", http.StatusMethodNotAllowed)
		return
	}

	logger := mlog.With(mlog.String("Scope", "Config"))
	paramMgr := paramtable.GetBaseTable().Manager()

	type ConfigPair struct {
		Key   string  `json:"key"`
		Value *string `json:"value"` // nil means reset (delete from etcd)
	}

	var requestBody struct {
		// Batch format
		Configs []ConfigPair `json:"configs"`
		// Legacy single-key format (backward compatibility)
		Key   string  `json:"key"`
		Value *string `json:"value"`
	}

	if err := json.NewDecoder(request.Body).Decode(&requestBody); err != nil {
		logger.Info(request.Context(), "HandleAlterConfig failed to decode request body", mlog.Err(err))
		writeJSONError(writer, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Backward compatibility: if "configs" is empty but "key" is set, convert legacy format.
	if len(requestBody.Configs) == 0 && requestBody.Key != "" {
		requestBody.Configs = []ConfigPair{{Key: requestBody.Key, Value: requestBody.Value}}
	}

	if len(requestBody.Configs) == 0 {
		logger.Info(request.Context(), "HandleAlterConfig no configs provided")
		writeJSONError(writer, "configs array is required and cannot be empty", http.StatusBadRequest)
		return
	}

	// Classify into updates and deletes, and validate.
	seen := make(map[string]struct{}, len(requestBody.Configs))
	configsToUpdate := make(map[string]string)
	keysToDelete := make([]string, 0)

	for _, config := range requestBody.Configs {
		if config.Key == "" {
			logger.Info(request.Context(), "HandleAlterConfig config missing key")
			writeJSONError(writer, "all configs must have a non-empty key", http.StatusBadRequest)
			return
		}

		// Check for duplicate keys
		if _, exists := seen[config.Key]; exists {
			logger.Info(request.Context(), "HandleAlterConfig duplicate key found", mlog.String("key", config.Key))
			writeJSONError(writer, fmt.Sprintf("duplicate key found: %s", config.Key), http.StatusBadRequest)
			return
		}
		seen[config.Key] = struct{}{}

		// Check if it's mqtype configuration
		normalizedKey := strings.ToLower(strings.ReplaceAll(config.Key, "/", "."))
		if strings.Contains(normalizedKey, "mqtype") || strings.Contains(normalizedKey, "mq.type") {
			logger.Info(request.Context(), "HandleAlterConfig attempted to modify mqtype",
				mlog.String("key", config.Key))
			writeJSONError(writer, fmt.Sprintf("mqtype configuration cannot be modified through this endpoint. Please use the alterWAL endpoint instead. Invalid key: %s", config.Key), http.StatusBadRequest)
			return
		}

		// Check if the configuration is immutable - immutable keys cannot be modified
		if paramMgr.IsImmutable(config.Key) {
			logger.Info(request.Context(), "HandleAlterConfig attempted to modify immutable config",
				mlog.String("key", config.Key))
			writeJSONError(writer, fmt.Sprintf("immutable configuration cannot be modified through this endpoint. Invalid key: %s", config.Key), http.StatusBadRequest)
			return
		}

		if config.Value != nil {
			configsToUpdate[config.Key] = *config.Value
		} else {
			keysToDelete = append(keysToDelete, config.Key)
		}
	}

	// Get EtcdSource to save the configuration
	etcdSource, ok := paramMgr.GetEtcdSource()
	if !ok {
		logger.Info(request.Context(), "HandleAlterConfig failed, etcd source not enabled")
		writeJSONError(writer, "etcd source is not enabled", http.StatusInternalServerError)
		return
	}

	// Alter configuration(s) in etcd atomically (updates + deletes in one transaction).
	// AlterConfigsInEtcd also proactively refreshes the local EtcdSource so that the write
	// is immediately visible in this process before we return.
	if err := paramMgr.AlterConfigsInEtcd(etcdSource, configsToUpdate, keysToDelete); err != nil {
		logger.Info(request.Context(), "HandleAlterConfig failed to atomically alter configs in etcd",
			mlog.Any("updates", configsToUpdate),
			mlog.Strings("deletes", keysToDelete),
			mlog.Err(err))
		writeJSONError(writer, fmt.Sprintf("failed to atomically alter configurations in etcd: %s", err.Error()), http.StatusInternalServerError)
		return
	}

	logger.Info(request.Context(), "HandleAlterConfig success",
		mlog.Int("updates", len(configsToUpdate)),
		mlog.Int("deletes", len(keysToDelete)),
		mlog.Any("updated", configsToUpdate),
		mlog.Strings("deleted", keysToDelete))

	writeJSONResponse(writer, http.StatusOK, map[string]string{"msg": "OK"})
}

// HandleGetConfig handles GET requests to retrieve paramtable configuration.
//
// Query parameters:
//   - keys: comma-separated config keys to retrieve (required)
//
// Response: ordered list matching the input keys order.
//
//	{"configs": [{"key": "k1", "value": "v1", "source": "EtcdSource"}, {"key": "k2", "error": "key not found"}]}
func (s *mixCoordImpl) HandleGetConfig(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writeJSONError(writer, "Method not allowed, use GET", http.StatusMethodNotAllowed)
		return
	}

	keysParam := request.URL.Query().Get("keys")
	if keysParam == "" {
		writeJSONError(writer, "query parameter 'keys' is required", http.StatusBadRequest)
		return
	}

	paramMgr := paramtable.GetBaseTable().Manager()

	type configResult struct {
		Key    string `json:"key"`
		Value  string `json:"value,omitempty"`
		Source string `json:"source,omitempty"`
		Error  string `json:"error,omitempty"`
	}

	keys := strings.Split(keysParam, ",")
	results := make([]configResult, 0, len(keys))
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		// Redact sensitive config keys (passwords, secrets, tokens).
		normalizedKey := strings.ToLower(key)
		if strings.Contains(normalizedKey, "password") || strings.Contains(normalizedKey, "secret") ||
			strings.Contains(normalizedKey, "token") || strings.Contains(normalizedKey, "credential") {
			results = append(results, configResult{Key: key, Error: "access to sensitive config key is denied"})
			continue
		}
		source, value, err := paramMgr.GetConfig(key)
		if err != nil {
			results = append(results, configResult{Key: key, Error: err.Error()})
		} else {
			results = append(results, configResult{Key: key, Value: value, Source: source})
		}
	}

	if len(results) == 0 {
		writeJSONError(writer, "no valid keys provided", http.StatusBadRequest)
		return
	}

	writeJSONResponse(writer, http.StatusOK, map[string]interface{}{
		"configs": results,
	})
}

// writeJSONError writes a JSON error response with proper escaping.
func writeJSONError(w http.ResponseWriter, msg string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]string{"msg": msg})
}

// writeJSONResponse writes a JSON response with proper escaping.
func writeJSONResponse(w http.ResponseWriter, statusCode int, resp interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(resp)
}
