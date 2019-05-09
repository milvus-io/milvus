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

use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::convert::Into;
use std::sync::{Arc, Mutex};

use protocol::{TInputProtocol, TMessageIdentifier, TOutputProtocol, TStoredInputProtocol};

use super::{TProcessor, handle_process_result};

const MISSING_SEPARATOR_AND_NO_DEFAULT: &'static str = "missing service separator and no default processor set";
type ThreadSafeProcessor = Box<TProcessor + Send + Sync>;

/// A `TProcessor` that can demux service calls to multiple underlying
/// Thrift services.
///
/// Users register service-specific `TProcessor` instances with a
/// `TMultiplexedProcessor`, and then register that processor with a server
/// implementation. Following that, all incoming service calls are automatically
/// routed to the service-specific `TProcessor`.
///
/// A `TMultiplexedProcessor` can only handle messages sent by a
/// `TMultiplexedOutputProtocol`.
#[derive(Default)]
pub struct TMultiplexedProcessor {
    stored: Mutex<StoredProcessors>,
}

#[derive(Default)]
struct StoredProcessors {
    processors: HashMap<String, Arc<ThreadSafeProcessor>>,
    default_processor: Option<Arc<ThreadSafeProcessor>>,
}

impl TMultiplexedProcessor {
    /// Create a new `TMultiplexedProcessor` with no registered service-specific
    /// processors.
    pub fn new() -> TMultiplexedProcessor {
        TMultiplexedProcessor {
            stored: Mutex::new(
                StoredProcessors {
                    processors: HashMap::new(),
                    default_processor: None,
                },
            ),
        }
    }

    /// Register a service-specific `processor` for the service named
    /// `service_name`. This implementation is also backwards-compatible with
    /// non-multiplexed clients. Set `as_default` to `true` to allow
    /// non-namespaced requests to be dispatched to a default processor.
    ///
    /// Returns success if a new entry was inserted. Returns an error if:
    /// * A processor exists for `service_name`
    /// * You attempt to register a processor as default, and an existing default exists
    #[cfg_attr(feature = "cargo-clippy", allow(map_entry))]
    pub fn register<S: Into<String>>(
        &mut self,
        service_name: S,
        processor: Box<TProcessor + Send + Sync>,
        as_default: bool,
    ) -> ::Result<()> {
        let mut stored = self.stored.lock().unwrap();

        let name = service_name.into();
        if !stored.processors.contains_key(&name) {
            let processor = Arc::new(processor);

            if as_default {
                if stored.default_processor.is_none() {
                    stored.processors.insert(name, processor.clone());
                    stored.default_processor = Some(processor.clone());
                    Ok(())
                } else {
                    Err("cannot reset default processor".into())
                }
            } else {
                stored.processors.insert(name, processor);
                Ok(())
            }
        } else {
            Err(format!("cannot overwrite existing processor for service {}", name).into(),)
        }
    }

    fn process_message(
        &self,
        msg_ident: &TMessageIdentifier,
        i_prot: &mut TInputProtocol,
        o_prot: &mut TOutputProtocol,
    ) -> ::Result<()> {
        let (svc_name, svc_call) = split_ident_name(&msg_ident.name);
        debug!("routing svc_name {:?} svc_call {}", &svc_name, &svc_call);

        let processor: Option<Arc<ThreadSafeProcessor>> = {
            let stored = self.stored.lock().unwrap();
            if let Some(name) = svc_name {
                stored.processors.get(name).cloned()
            } else {
                stored.default_processor.clone()
            }
        };

        match processor {
            Some(arc) => {
                let new_msg_ident = TMessageIdentifier::new(
                    svc_call,
                    msg_ident.message_type,
                    msg_ident.sequence_number,
                );
                let mut proxy_i_prot = TStoredInputProtocol::new(i_prot, new_msg_ident);
                (*arc).process(&mut proxy_i_prot, o_prot)
            }
            None => Err(missing_processor_message(svc_name).into()),
        }
    }
}

impl TProcessor for TMultiplexedProcessor {
    fn process(&self, i_prot: &mut TInputProtocol, o_prot: &mut TOutputProtocol) -> ::Result<()> {
        let msg_ident = i_prot.read_message_begin()?;

        debug!("process incoming msg id:{:?}", &msg_ident);
        let res = self.process_message(&msg_ident, i_prot, o_prot);

        handle_process_result(&msg_ident, res, o_prot)
    }
}

impl Debug for TMultiplexedProcessor {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let stored = self.stored.lock().unwrap();
        write!(
            f,
            "TMultiplexedProcess {{ registered_count: {:?} default: {:?} }}",
            stored.processors.keys().len(),
            stored.default_processor.is_some()
        )
    }
}

fn split_ident_name(ident_name: &str) -> (Option<&str>, &str) {
    ident_name
        .find(':')
        .map(
            |pos| {
                let (svc_name, svc_call) = ident_name.split_at(pos);
                let (_, svc_call) = svc_call.split_at(1); // remove colon from service call name
                (Some(svc_name), svc_call)
            },
        )
        .or_else(|| Some((None, ident_name)))
        .unwrap()
}

fn missing_processor_message(svc_name: Option<&str>) -> String {
    match svc_name {
        Some(name) => format!("no processor found for service {}", name),
        None => MISSING_SEPARATOR_AND_NO_DEFAULT.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use std::convert::Into;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use {ApplicationError, ApplicationErrorKind};
    use protocol::{TBinaryInputProtocol, TBinaryOutputProtocol, TMessageIdentifier, TMessageType};
    use transport::{ReadHalf, TBufferChannel, TIoChannel, WriteHalf};

    use super::*;

    #[test]
    fn should_split_name_into_proper_separator_and_service_call() {
        let ident_name = "foo:bar_call";
        let (serv, call) = split_ident_name(&ident_name);
        assert_eq!(serv, Some("foo"));
        assert_eq!(call, "bar_call");
    }

    #[test]
    fn should_return_full_ident_if_no_separator_exists() {
        let ident_name = "bar_call";
        let (serv, call) = split_ident_name(&ident_name);
        assert_eq!(serv, None);
        assert_eq!(call, "bar_call");
    }

    #[test]
    fn should_write_error_if_no_separator_found_and_no_default_processor_exists() {
        let (mut i, mut o) = build_objects();

        let sent_ident = TMessageIdentifier::new("foo", TMessageType::Call, 10);
        o.write_message_begin(&sent_ident).unwrap();
        o.flush().unwrap();
        o.transport.copy_write_buffer_to_read_buffer();
        o.transport.empty_write_buffer();

        let p = TMultiplexedProcessor::new();
        p.process(&mut i, &mut o).unwrap(); // at this point an error should be written out

        i.transport
            .set_readable_bytes(&o.transport.write_bytes());
        let rcvd_ident = i.read_message_begin().unwrap();
        let expected_ident = TMessageIdentifier::new("foo", TMessageType::Exception, 10);
        assert_eq!(rcvd_ident, expected_ident);
        let rcvd_err = ::Error::read_application_error_from_in_protocol(&mut i).unwrap();
        let expected_err = ApplicationError::new(
            ApplicationErrorKind::Unknown,
            MISSING_SEPARATOR_AND_NO_DEFAULT,
        );
        assert_eq!(rcvd_err, expected_err);
    }

    #[test]
    fn should_write_error_if_separator_exists_and_no_processor_found() {
        let (mut i, mut o) = build_objects();

        let sent_ident = TMessageIdentifier::new("missing:call", TMessageType::Call, 10);
        o.write_message_begin(&sent_ident).unwrap();
        o.flush().unwrap();
        o.transport.copy_write_buffer_to_read_buffer();
        o.transport.empty_write_buffer();

        let p = TMultiplexedProcessor::new();
        p.process(&mut i, &mut o).unwrap(); // at this point an error should be written out

        i.transport
            .set_readable_bytes(&o.transport.write_bytes());
        let rcvd_ident = i.read_message_begin().unwrap();
        let expected_ident = TMessageIdentifier::new("missing:call", TMessageType::Exception, 10);
        assert_eq!(rcvd_ident, expected_ident);
        let rcvd_err = ::Error::read_application_error_from_in_protocol(&mut i).unwrap();
        let expected_err = ApplicationError::new(
            ApplicationErrorKind::Unknown,
            missing_processor_message(Some("missing")),
        );
        assert_eq!(rcvd_err, expected_err);
    }

    #[derive(Default)]
    struct Service {
        pub invoked: Arc<AtomicBool>,
    }

    impl TProcessor for Service {
        fn process(&self, _: &mut TInputProtocol, _: &mut TOutputProtocol) -> ::Result<()> {
            let res = self.invoked
                .compare_and_swap(false, true, Ordering::Relaxed);
            if res {
                Ok(())
            } else {
                Err("failed swap".into())
            }
        }
    }

    #[test]
    fn should_route_call_to_correct_processor() {
        let (mut i, mut o) = build_objects();

        // build the services
        let svc_1 = Service { invoked: Arc::new(AtomicBool::new(false)) };
        let atm_1 = svc_1.invoked.clone();
        let svc_2 = Service { invoked: Arc::new(AtomicBool::new(false)) };
        let atm_2 = svc_2.invoked.clone();

        // register them
        let mut p = TMultiplexedProcessor::new();
        p.register("service_1", Box::new(svc_1), false).unwrap();
        p.register("service_2", Box::new(svc_2), false).unwrap();

        // make the service call
        let sent_ident = TMessageIdentifier::new("service_1:call", TMessageType::Call, 10);
        o.write_message_begin(&sent_ident).unwrap();
        o.flush().unwrap();
        o.transport.copy_write_buffer_to_read_buffer();
        o.transport.empty_write_buffer();

        p.process(&mut i, &mut o).unwrap();

        // service 1 should have been invoked, not service 2
        assert_eq!(atm_1.load(Ordering::Relaxed), true);
        assert_eq!(atm_2.load(Ordering::Relaxed), false);
    }

    #[test]
    fn should_route_call_to_correct_processor_if_no_separator_exists_and_default_processor_set() {
        let (mut i, mut o) = build_objects();

        // build the services
        let svc_1 = Service { invoked: Arc::new(AtomicBool::new(false)) };
        let atm_1 = svc_1.invoked.clone();
        let svc_2 = Service { invoked: Arc::new(AtomicBool::new(false)) };
        let atm_2 = svc_2.invoked.clone();

        // register them
        let mut p = TMultiplexedProcessor::new();
        p.register("service_1", Box::new(svc_1), false).unwrap();
        p.register("service_2", Box::new(svc_2), true).unwrap(); // second processor is default

        // make the service call (it's an old client, so we have to be backwards compatible)
        let sent_ident = TMessageIdentifier::new("old_call", TMessageType::Call, 10);
        o.write_message_begin(&sent_ident).unwrap();
        o.flush().unwrap();
        o.transport.copy_write_buffer_to_read_buffer();
        o.transport.empty_write_buffer();

        p.process(&mut i, &mut o).unwrap();

        // service 2 should have been invoked, not service 1
        assert_eq!(atm_1.load(Ordering::Relaxed), false);
        assert_eq!(atm_2.load(Ordering::Relaxed), true);
    }

    fn build_objects()
        -> (TBinaryInputProtocol<ReadHalf<TBufferChannel>>,
            TBinaryOutputProtocol<WriteHalf<TBufferChannel>>)
    {
        let c = TBufferChannel::with_capacity(128, 128);
        let (r_c, w_c) = c.split().unwrap();
        (TBinaryInputProtocol::new(r_c, true), TBinaryOutputProtocol::new(w_c, true))
    }
}
