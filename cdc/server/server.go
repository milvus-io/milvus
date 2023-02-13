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

package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	modelrequest "github.com/milvus-io/milvus/cdc/server/model/request"
	"github.com/mitchellh/mapstructure"
	"github.com/samber/lo"
	"go.uber.org/zap"
)

type CdcServer struct {
	cdcApi          CDCApi
	cdcServerConfig *CdcServerConfig
}

func (c *CdcServer) Run(config *CdcServerConfig) {
	registerMetric()

	c.cdcServerConfig = config
	c.cdcApi = GetCDCApi(c.cdcServerConfig)
	c.cdcApi.ReloadTask()
	cdcHandler := c.getCdcHandler()
	http.Handle("/cdc", cdcHandler)
	err := http.ListenAndServe(c.cdcServerConfig.Address, nil)
	log.Fatal("cdc server down", zap.Error(err))
}

func (c *CdcServer) getCdcHandler() http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		startTime := time.Now()
		if request.Method != http.MethodPost {
			c.handleError(writer, "only support the POST method", http.StatusMethodNotAllowed,
				zap.String("method", request.Method))
			taskRequestCountVec.WithLabelValues(unknownTypeLabel, invalidMethodStatusLabel).Inc()
			return
		}
		bodyBytes, err := ioutil.ReadAll(request.Body)
		if err != nil {
			c.handleError(writer, "fail to read the request body, error: "+err.Error(), http.StatusInternalServerError)
			taskRequestCountVec.WithLabelValues(unknownTypeLabel, readErrorStatusLabel).Inc()
			return
		}
		cdcRequest := &modelrequest.CdcRequest{}
		err = json.Unmarshal(bodyBytes, cdcRequest)
		if err != nil {
			c.handleError(writer, "fail to unmarshal the request, error: "+err.Error(), http.StatusInternalServerError)
			taskRequestCountVec.WithLabelValues(unknownTypeLabel, unmarshalErrorStatusLabel).Inc()
			return
		}
		taskRequestCountVec.WithLabelValues(cdcRequest.RequestType, totalStatusLabel).Inc()

		response := c.handleRequest(cdcRequest, writer)

		if response != nil {
			_ = json.NewEncoder(writer).Encode(response)
			taskRequestCountVec.WithLabelValues(cdcRequest.RequestType, successStatusLabel).Inc()
			taskRequestLatencyVec.WithLabelValues(cdcRequest.RequestType).Observe(float64(time.Now().Sub(startTime).Milliseconds()))
		}
	})
}

func (c *CdcServer) handleError(w http.ResponseWriter, error string, code int, fields ...zap.Field) {
	log.Warn(error, fields...)
	http.Error(w, error, code)
}

func (c *CdcServer) handleRequest(cdcRequest *modelrequest.CdcRequest, writer http.ResponseWriter) any {
	requestType := cdcRequest.RequestType
	handler, ok := requestHandlers[requestType]
	if !ok {
		c.handleError(writer, fmt.Sprintf("invalid 'request_type' param, can be set %v", lo.Keys(requestHandlers)), http.StatusBadRequest,
			zap.String("type", requestType))
		return nil
	}
	requestModel := handler.generateModel()
	if err := mapstructure.Decode(cdcRequest.RequestData, requestModel); err != nil {
		c.handleError(writer, fmt.Sprintf("fail to decode the %s request, error: %s", requestType, err.Error()), http.StatusInternalServerError)
		return nil
	}
	response, err := handler.handle(c.cdcApi, requestModel)
	if err != nil {
		code := http.StatusInternalServerError
		if errors.Is(err, ClientErr) {
			code = http.StatusBadRequest
		}
		c.handleError(writer, fmt.Sprintf("fail to handle the %s request, error: %s", requestType, err.Error()), code, zap.Error(err))
		return nil
	}

	return response
}
