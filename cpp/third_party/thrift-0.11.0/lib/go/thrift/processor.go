// +build !go1.7

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

import "golang.org/x/net/context"

// A processor is a generic object which operates upon an input stream and
// writes to some output stream.
type TProcessor interface {
	Process(ctx context.Context, in, out TProtocol) (bool, TException)
}

type TProcessorFunction interface {
	Process(ctx context.Context, seqId int32, in, out TProtocol) (bool, TException)
}
