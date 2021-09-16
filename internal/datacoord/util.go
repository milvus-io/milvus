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
package datacoord

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
)

// Response response interface for verification
type Response interface {
	GetStatus() *commonpb.Status
}

// VerifyResponse verify grpc Response 1. check error is nil 2. check response.GetStatus() with status success
func VerifyResponse(response interface{}, err error) error {
	if err != nil {
		return err
	}
	if response == nil {
		return errNilResponse
	}
	switch resp := response.(type) {
	case Response:
		// note that resp will not be nil here, since it's still a interface
		if resp.GetStatus() == nil {
			return errNilStatusResponse
		}
		if resp.GetStatus().GetErrorCode() != commonpb.ErrorCode_Success {
			return errors.New(resp.GetStatus().GetReason())
		}
	case *commonpb.Status:
		if resp == nil {
			return errNilResponse
		}
		if resp.ErrorCode != commonpb.ErrorCode_Success {
			return errors.New(resp.GetReason())
		}
	default:
		return errUnknownResponseType
	}
	return nil
}

// LongTermChecker checks we receive at least one msg in d duration. If not, checker
// will print a warn message.
type LongTermChecker struct {
	d    time.Duration
	t    *time.Ticker
	ch   chan struct{}
	warn string
	name string
}

// NewLongTermChecker create a long term checker specified name, checking interval and warning string to print
func NewLongTermChecker(ctx context.Context, name string, d time.Duration, warn string) *LongTermChecker {
	c := &LongTermChecker{
		name: name,
		d:    d,
		warn: warn,
		ch:   make(chan struct{}),
	}
	return c
}

// Start starts the check process
func (c *LongTermChecker) Start() {
	c.t = time.NewTicker(c.d)
	go func() {
		for {
			select {
			case <-c.ch:
				log.Warn(fmt.Sprintf("long term checker [%s] shutdown", c.name))
				return
			case <-c.t.C:
				log.Warn(c.warn)
			}
		}
	}()
}

// Check reset the time ticker
func (c *LongTermChecker) Check() {
	c.t.Reset(c.d)
}

// Stop stop the checker
func (c *LongTermChecker) Stop() {
	c.t.Stop()
	close(c.ch)
}
