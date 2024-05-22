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

package mqwrapper

// MessageID is the interface that provides operations of message is
type MessageID interface {
	// Serialize the message id into a sequence of bytes that can be stored somewhere else
	Serialize() []byte

	AtEarliestPosition() bool

	// TODO: remove in future.
	LessOrEqualThan(msgID []byte) (bool, error)

	// TODO: remove in future.
	Equal(msgID []byte) (bool, error)

	// MessageID should be comparable.
	// Comparable operation should must be used between the same implementation of message id.
	// Panic if not.

	// LT less than.
	LT(MessageID) bool

	// LTE less than or equal to.
	LTE(MessageID) bool

	// EQ Equal to.
	EQ(MessageID) bool
}
