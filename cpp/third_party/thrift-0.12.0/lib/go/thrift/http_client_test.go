/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package thrift

import (
	"net/http"
	"testing"
)

func TestHttpClient(t *testing.T) {
	l, addr := HttpClientSetupForTest(t)
	if l != nil {
		defer l.Close()
	}
	trans, err := NewTHttpPostClient("http://" + addr.String())
	if err != nil {
		l.Close()
		t.Fatalf("Unable to connect to %s: %s", addr.String(), err)
	}
	TransportTest(t, trans, trans)
}

func TestHttpClientHeaders(t *testing.T) {
	l, addr := HttpClientSetupForTest(t)
	if l != nil {
		defer l.Close()
	}
	trans, err := NewTHttpPostClient("http://" + addr.String())
	if err != nil {
		l.Close()
		t.Fatalf("Unable to connect to %s: %s", addr.String(), err)
	}
	TransportHeaderTest(t, trans, trans)
}

func TestHttpCustomClient(t *testing.T) {
	l, addr := HttpClientSetupForTest(t)
	if l != nil {
		defer l.Close()
	}

	httpTransport := &customHttpTransport{}

	trans, err := NewTHttpPostClientWithOptions("http://"+addr.String(), THttpClientOptions{
		Client: &http.Client{
			Transport: httpTransport,
		},
	})
	if err != nil {
		l.Close()
		t.Fatalf("Unable to connect to %s: %s", addr.String(), err)
	}
	TransportHeaderTest(t, trans, trans)

	if !httpTransport.hit {
		t.Fatalf("Custom client was not used")
	}
}

func TestHttpCustomClientPackageScope(t *testing.T) {
	l, addr := HttpClientSetupForTest(t)
	if l != nil {
		defer l.Close()
	}
	httpTransport := &customHttpTransport{}
	DefaultHttpClient = &http.Client{
		Transport: httpTransport,
	}

	trans, err := NewTHttpPostClient("http://" + addr.String())
	if err != nil {
		l.Close()
		t.Fatalf("Unable to connect to %s: %s", addr.String(), err)
	}
	TransportHeaderTest(t, trans, trans)

	if !httpTransport.hit {
		t.Fatalf("Custom client was not used")
	}
}

type customHttpTransport struct {
	hit bool
}

func (c *customHttpTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	c.hit = true
	return http.DefaultTransport.RoundTrip(req)
}
