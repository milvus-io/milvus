// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Thrift compiler auto-generated support.
//!
//!
//! Types and functions used internally by the Thrift compiler's Rust plugin
//! to implement required functionality. Users should never have to use code
//! in this module directly.

use protocol::{TInputProtocol, TOutputProtocol};

/// Specifies the minimum functionality an auto-generated client should provide
/// to communicate with a Thrift server.
pub trait TThriftClient {
    /// Returns the input protocol used to read serialized Thrift messages
    /// from the Thrift server.
    fn i_prot_mut(&mut self) -> &mut TInputProtocol;
    /// Returns the output protocol used to write serialized Thrift messages
    /// to the Thrift server.
    fn o_prot_mut(&mut self) -> &mut TOutputProtocol;
    /// Returns the sequence number of the last message written to the Thrift
    /// server. Returns `0` if no messages have been written. Sequence
    /// numbers should *never* be negative, and this method returns an `i32`
    /// simply because the Thrift protocol encodes sequence numbers as `i32` on
    /// the wire.
    fn sequence_number(&self) -> i32; // FIXME: consider returning a u32
    /// Increments the sequence number, indicating that a message with that
    /// number has been sent to the Thrift server.
    fn increment_sequence_number(&mut self) -> i32;
}
