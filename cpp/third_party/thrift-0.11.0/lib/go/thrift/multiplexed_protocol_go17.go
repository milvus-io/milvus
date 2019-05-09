// +build go1.7

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
	"context"
	"fmt"
	"strings"
)

func (t *TMultiplexedProcessor) Process(ctx context.Context, in, out TProtocol) (bool, TException) {
	name, typeId, seqid, err := in.ReadMessageBegin()
	if err != nil {
		return false, err
	}
	if typeId != CALL && typeId != ONEWAY {
		return false, fmt.Errorf("Unexpected message type %v", typeId)
	}
	//extract the service name
	v := strings.SplitN(name, MULTIPLEXED_SEPARATOR, 2)
	if len(v) != 2 {
		if t.DefaultProcessor != nil {
			smb := NewStoredMessageProtocol(in, name, typeId, seqid)
			return t.DefaultProcessor.Process(ctx, smb, out)
		}
		return false, fmt.Errorf("Service name not found in message name: %s.  Did you forget to use a TMultiplexProtocol in your client?", name)
	}
	actualProcessor, ok := t.serviceProcessorMap[v[0]]
	if !ok {
		return false, fmt.Errorf("Service name not found: %s.  Did you forget to call registerProcessor()?", v[0])
	}
	smb := NewStoredMessageProtocol(in, v[1], typeId, seqid)
	return actualProcessor.Process(ctx, smb, out)
}
