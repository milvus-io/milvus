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

package common

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"syscall"
	"testing"
	"thrift"
	"time"
)

type slowHttpHandler struct{}

func (slowHttpHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	time.Sleep(1 * time.Second)
}

func TestHttpContextTimeout(t *testing.T) {
	certPath = "../../../keys"

	unit := test_unit{"127.0.0.1", 9096, "", "http", "binary", false}

	server := &http.Server{Addr: unit.host + fmt.Sprintf(":%d", unit.port), Handler: slowHttpHandler{}}
	go server.ListenAndServe()

	client, trans, err := StartClient(unit.host, unit.port, unit.domain_socket, unit.transport, unit.protocol, unit.ssl)
	if err != nil {
		t.Errorf("Unable to start client: %v", err)
		return
	}
	defer trans.Close()

	unwrapErr := func(err error) error {
		for {
			switch err.(type) {
			case thrift.TTransportException:
				err = err.(thrift.TTransportException).Err()
			case *url.Error:
				err = err.(*url.Error).Err
			case *net.OpError:
				err = err.(*net.OpError).Err
			case *os.SyscallError:
				err = err.(*os.SyscallError).Err
			default:
				return err
			}
		}
	}

	serverStartupDeadline := time.Now().Add(5 * time.Second)
	for {
		ctx, _ := context.WithTimeout(context.Background(), 50*time.Millisecond)
		err = client.TestVoid(ctx)
		err = unwrapErr(err)
		if err != syscall.ECONNREFUSED || time.Now().After(serverStartupDeadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}

	if err == nil {
		t.Errorf("Request completed (should have timed out)")
		return
	}

	// We've got to switch on `err.Error()` here since go1.7 doesn't properly return
	// `context.DeadlineExceeded` error and `http.errRequestCanceled` is not exported.
	// See https://github.com/golang/go/issues/17711
	switch err.Error() {
	case context.DeadlineExceeded.Error(), "net/http: request canceled":
		// Expected error
	default:
		t.Errorf("Unexpected error: %s", err)
	}
}
