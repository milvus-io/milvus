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

//! Types used to implement a Thrift server.

use {ApplicationError, ApplicationErrorKind};
use protocol::{TInputProtocol, TMessageIdentifier, TMessageType, TOutputProtocol};

mod multiplexed;
mod threaded;

pub use self::multiplexed::TMultiplexedProcessor;
pub use self::threaded::TServer;

/// Handles incoming Thrift messages and dispatches them to the user-defined
/// handler functions.
///
/// An implementation is auto-generated for each Thrift service. When used by a
/// server (for example, a `TSimpleServer`), it will demux incoming service
/// calls and invoke the corresponding user-defined handler function.
///
/// # Examples
///
/// Create and start a server using the auto-generated `TProcessor` for
/// a Thrift service `SimpleService`.
///
/// ```no_run
/// use thrift;
/// use thrift::protocol::{TInputProtocol, TOutputProtocol};
/// use thrift::server::TProcessor;
///
/// //
/// // auto-generated
/// //
///
/// // processor for `SimpleService`
/// struct SimpleServiceSyncProcessor;
/// impl SimpleServiceSyncProcessor {
///     fn new<H: SimpleServiceSyncHandler>(processor: H) -> SimpleServiceSyncProcessor {
///         unimplemented!();
///     }
/// }
///
/// // `TProcessor` implementation for `SimpleService`
/// impl TProcessor for SimpleServiceSyncProcessor {
///     fn process(&self, i: &mut TInputProtocol, o: &mut TOutputProtocol) -> thrift::Result<()> {
///         unimplemented!();
///     }
/// }
///
/// // service functions for SimpleService
/// trait SimpleServiceSyncHandler {
///     fn service_call(&self) -> thrift::Result<()>;
/// }
///
/// //
/// // user-code follows
/// //
///
/// // define a handler that will be invoked when `service_call` is received
/// struct SimpleServiceHandlerImpl;
/// impl SimpleServiceSyncHandler for SimpleServiceHandlerImpl {
///     fn service_call(&self) -> thrift::Result<()> {
///         unimplemented!();
///     }
/// }
///
/// // instantiate the processor
/// let processor = SimpleServiceSyncProcessor::new(SimpleServiceHandlerImpl {});
///
/// // at this point you can pass the processor to the server
/// // let server = TServer::new(..., processor);
/// ```
pub trait TProcessor {
    /// Process a Thrift service call.
    ///
    /// Reads arguments from `i`, executes the user's handler code, and writes
    /// the response to `o`.
    ///
    /// Returns `()` if the handler was executed; `Err` otherwise.
    fn process(&self, i: &mut TInputProtocol, o: &mut TOutputProtocol) -> ::Result<()>;
}

/// Convenience function used in generated `TProcessor` implementations to
/// return an `ApplicationError` if thrift message processing failed.
pub fn handle_process_result(
    msg_ident: &TMessageIdentifier,
    res: ::Result<()>,
    o_prot: &mut TOutputProtocol,
) -> ::Result<()> {
    if let Err(e) = res {
        let e = match e {
            ::Error::Application(a) => a,
            _ => ApplicationError::new(ApplicationErrorKind::Unknown, format!("{:?}", e)),
        };

        let ident = TMessageIdentifier::new(
            msg_ident.name.clone(),
            TMessageType::Exception,
            msg_ident.sequence_number,
        );

        o_prot.write_message_begin(&ident)?;
        ::Error::write_application_error_to_out_protocol(&e, o_prot)?;
        o_prot.write_message_end()?;
        o_prot.flush()
    } else {
        Ok(())
    }
}
