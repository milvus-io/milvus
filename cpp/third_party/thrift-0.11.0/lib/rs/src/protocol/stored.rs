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

use std::convert::Into;

use ProtocolErrorKind;
use super::{TFieldIdentifier, TInputProtocol, TListIdentifier, TMapIdentifier, TMessageIdentifier,
            TSetIdentifier, TStructIdentifier};

/// `TInputProtocol` required to use a `TMultiplexedProcessor`.
///
/// A `TMultiplexedProcessor` reads incoming message identifiers to determine to
/// which `TProcessor` requests should be forwarded. However, once read, those
/// message identifier bytes are no longer on the wire. Since downstream
/// processors expect to read message identifiers from the given input protocol
/// we need some way of supplying a `TMessageIdentifier` with the service-name
/// stripped. This implementation stores the received `TMessageIdentifier`
/// (without the service name) and passes it to the wrapped `TInputProtocol`
/// when `TInputProtocol::read_message_begin(...)` is called. It delegates all
/// other calls directly to the wrapped `TInputProtocol`.
///
/// This type **should not** be used by application code.
///
/// # Examples
///
/// Create and use a `TStoredInputProtocol`.
///
/// ```no_run
/// use thrift;
/// use thrift::protocol::{TInputProtocol, TMessageIdentifier, TMessageType, TOutputProtocol};
/// use thrift::protocol::{TBinaryInputProtocol, TBinaryOutputProtocol, TStoredInputProtocol};
/// use thrift::server::TProcessor;
/// use thrift::transport::{TIoChannel, TTcpChannel};
///
/// // sample processor
/// struct ActualProcessor;
/// impl TProcessor for ActualProcessor {
///     fn process(
///         &self,
///         _: &mut TInputProtocol,
///         _: &mut TOutputProtocol
///     ) -> thrift::Result<()> {
///         unimplemented!()
///     }
/// }
/// let processor = ActualProcessor {};
///
/// // construct the shared transport
/// let mut channel = TTcpChannel::new();
/// channel.open("localhost:9090").unwrap();
///
/// let (i_chan, o_chan) = channel.split().unwrap();
///
/// // construct the actual input and output protocols
/// let mut i_prot = TBinaryInputProtocol::new(i_chan, true);
/// let mut o_prot = TBinaryOutputProtocol::new(o_chan, true);
///
/// // message identifier received from remote and modified to remove the service name
/// let new_msg_ident = TMessageIdentifier::new("service_call", TMessageType::Call, 1);
///
/// // construct the proxy input protocol
/// let mut proxy_i_prot = TStoredInputProtocol::new(&mut i_prot, new_msg_ident);
/// let res = processor.process(&mut proxy_i_prot, &mut o_prot);
/// ```
// FIXME: implement Debug
pub struct TStoredInputProtocol<'a> {
    inner: &'a mut TInputProtocol,
    message_ident: Option<TMessageIdentifier>,
}

impl<'a> TStoredInputProtocol<'a> {
    /// Create a `TStoredInputProtocol` that delegates all calls other than
    /// `TInputProtocol::read_message_begin(...)` to a `wrapped`
    /// `TInputProtocol`. `message_ident` is the modified message identifier -
    /// with service name stripped - that will be passed to
    /// `wrapped.read_message_begin(...)`.
    pub fn new(
        wrapped: &mut TInputProtocol,
        message_ident: TMessageIdentifier,
    ) -> TStoredInputProtocol {
        TStoredInputProtocol {
            inner: wrapped,
            message_ident: message_ident.into(),
        }
    }
}

impl<'a> TInputProtocol for TStoredInputProtocol<'a> {
    fn read_message_begin(&mut self) -> ::Result<TMessageIdentifier> {
        self.message_ident
            .take()
            .ok_or_else(
                || {
                    ::errors::new_protocol_error(
                        ProtocolErrorKind::Unknown,
                        "message identifier already read",
                    )
                },
            )
    }

    fn read_message_end(&mut self) -> ::Result<()> {
        self.inner.read_message_end()
    }

    fn read_struct_begin(&mut self) -> ::Result<Option<TStructIdentifier>> {
        self.inner.read_struct_begin()
    }

    fn read_struct_end(&mut self) -> ::Result<()> {
        self.inner.read_struct_end()
    }

    fn read_field_begin(&mut self) -> ::Result<TFieldIdentifier> {
        self.inner.read_field_begin()
    }

    fn read_field_end(&mut self) -> ::Result<()> {
        self.inner.read_field_end()
    }

    fn read_bytes(&mut self) -> ::Result<Vec<u8>> {
        self.inner.read_bytes()
    }

    fn read_bool(&mut self) -> ::Result<bool> {
        self.inner.read_bool()
    }

    fn read_i8(&mut self) -> ::Result<i8> {
        self.inner.read_i8()
    }

    fn read_i16(&mut self) -> ::Result<i16> {
        self.inner.read_i16()
    }

    fn read_i32(&mut self) -> ::Result<i32> {
        self.inner.read_i32()
    }

    fn read_i64(&mut self) -> ::Result<i64> {
        self.inner.read_i64()
    }

    fn read_double(&mut self) -> ::Result<f64> {
        self.inner.read_double()
    }

    fn read_string(&mut self) -> ::Result<String> {
        self.inner.read_string()
    }

    fn read_list_begin(&mut self) -> ::Result<TListIdentifier> {
        self.inner.read_list_begin()
    }

    fn read_list_end(&mut self) -> ::Result<()> {
        self.inner.read_list_end()
    }

    fn read_set_begin(&mut self) -> ::Result<TSetIdentifier> {
        self.inner.read_set_begin()
    }

    fn read_set_end(&mut self) -> ::Result<()> {
        self.inner.read_set_end()
    }

    fn read_map_begin(&mut self) -> ::Result<TMapIdentifier> {
        self.inner.read_map_begin()
    }

    fn read_map_end(&mut self) -> ::Result<()> {
        self.inner.read_map_end()
    }

    // utility
    //

    fn read_byte(&mut self) -> ::Result<u8> {
        self.inner.read_byte()
    }
}
