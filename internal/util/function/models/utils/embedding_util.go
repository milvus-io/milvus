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

package utils

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

const DefaultTimeout int64 = 30

func send(req *http.Request) ([]byte, error) {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("Call service failed, read response failed, errs:[%v]", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Call service failed, errs:[%s, %s]", resp.Status, body)
	}
	return body, nil
}

func RetrySend(ctx context.Context, data []byte, httpMethod string, url string, headers map[string]string, maxRetries int, retryDelay int) ([]byte, error) {
	var err error
	var body []byte
	for i := 0; i < maxRetries; i++ {
		req, reqErr := http.NewRequestWithContext(ctx, httpMethod, url, bytes.NewBuffer(data))
		if reqErr != nil {
			return nil, reqErr
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		body, err = send(req)
		if err == nil {
			return body, nil
		}
		time.Sleep(time.Duration(retryDelay) * time.Second)
	}
	return nil, err
}
