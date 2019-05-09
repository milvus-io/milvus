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

use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use threadpool::ThreadPool;

use {ApplicationError, ApplicationErrorKind};
use protocol::{TInputProtocol, TInputProtocolFactory, TOutputProtocol, TOutputProtocolFactory};
use transport::{TIoChannel, TReadTransportFactory, TTcpChannel, TWriteTransportFactory};

use super::TProcessor;

/// Fixed-size thread-pool blocking Thrift server.
///
/// A `TServer` listens on a given address and submits accepted connections
/// to an **unbounded** queue. Connections from this queue are serviced by
/// the first available worker thread from a **fixed-size** thread pool. Each
/// accepted connection is handled by that worker thread, and communication
/// over this thread occurs sequentially and synchronously (i.e. calls block).
/// Accepted connections have an input half and an output half, each of which
/// uses a `TTransport` and `TInputProtocol`/`TOutputProtocol` to translate
/// messages to and from byes. Any combination of `TInputProtocol`, `TOutputProtocol`
/// and `TTransport` may be used.
///
/// # Examples
///
/// Creating and running a `TServer` using Thrift-compiler-generated
/// service code.
///
/// ```no_run
/// use thrift;
/// use thrift::protocol::{TInputProtocolFactory, TOutputProtocolFactory};
/// use thrift::protocol::{TBinaryInputProtocolFactory, TBinaryOutputProtocolFactory};
/// use thrift::protocol::{TInputProtocol, TOutputProtocol};
/// use thrift::transport::{TBufferedReadTransportFactory, TBufferedWriteTransportFactory,
///                         TReadTransportFactory, TWriteTransportFactory};
/// use thrift::server::{TProcessor, TServer};
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
/// // instantiate the server
/// let i_tr_fact: Box<TReadTransportFactory> = Box::new(TBufferedReadTransportFactory::new());
/// let i_pr_fact: Box<TInputProtocolFactory> = Box::new(TBinaryInputProtocolFactory::new());
/// let o_tr_fact: Box<TWriteTransportFactory> = Box::new(TBufferedWriteTransportFactory::new());
/// let o_pr_fact: Box<TOutputProtocolFactory> = Box::new(TBinaryOutputProtocolFactory::new());
///
/// let mut server = TServer::new(
///     i_tr_fact,
///     i_pr_fact,
///     o_tr_fact,
///     o_pr_fact,
///     processor,
///     10
/// );
///
/// // start listening for incoming connections
/// match server.listen("127.0.0.1:8080") {
///   Ok(_)  => println!("listen completed"),
///   Err(e) => println!("listen failed with error {:?}", e),
/// }
/// ```
#[derive(Debug)]
pub struct TServer<PRC, RTF, IPF, WTF, OPF>
where
    PRC: TProcessor + Send + Sync + 'static,
    RTF: TReadTransportFactory + 'static,
    IPF: TInputProtocolFactory + 'static,
    WTF: TWriteTransportFactory + 'static,
    OPF: TOutputProtocolFactory + 'static,
{
    r_trans_factory: RTF,
    i_proto_factory: IPF,
    w_trans_factory: WTF,
    o_proto_factory: OPF,
    processor: Arc<PRC>,
    worker_pool: ThreadPool,
}

impl<PRC, RTF, IPF, WTF, OPF> TServer<PRC, RTF, IPF, WTF, OPF>
    where PRC: TProcessor + Send + Sync + 'static,
          RTF: TReadTransportFactory + 'static,
          IPF: TInputProtocolFactory + 'static,
          WTF: TWriteTransportFactory + 'static,
          OPF: TOutputProtocolFactory + 'static {
    /// Create a `TServer`.
    ///
    /// Each accepted connection has an input and output half, each of which
    /// requires a `TTransport` and `TProtocol`. `TServer` uses
    /// `read_transport_factory` and `input_protocol_factory` to create
    /// implementations for the input, and `write_transport_factory` and
    /// `output_protocol_factory` to create implementations for the output.
    pub fn new(
        read_transport_factory: RTF,
        input_protocol_factory: IPF,
        write_transport_factory: WTF,
        output_protocol_factory: OPF,
        processor: PRC,
        num_workers: usize,
    ) -> TServer<PRC, RTF, IPF, WTF, OPF> {
        TServer {
            r_trans_factory: read_transport_factory,
            i_proto_factory: input_protocol_factory,
            w_trans_factory: write_transport_factory,
            o_proto_factory: output_protocol_factory,
            processor: Arc::new(processor),
            worker_pool: ThreadPool::with_name(
                "Thrift service processor".to_owned(),
                num_workers,
            ),
        }
    }

    /// Listen for incoming connections on `listen_address`.
    ///
    /// `listen_address` should be in the form `host:port`,
    /// for example: `127.0.0.1:8080`.
    ///
    /// Return `()` if successful.
    ///
    /// Return `Err` when the server cannot bind to `listen_address` or there
    /// is an unrecoverable error.
    pub fn listen(&mut self, listen_address: &str) -> ::Result<()> {
        let listener = TcpListener::bind(listen_address)?;
        for stream in listener.incoming() {
            match stream {
                Ok(s) => {
                    let (i_prot, o_prot) = self.new_protocols_for_connection(s)?;
                    let processor = self.processor.clone();
                    self.worker_pool
                        .execute(move || handle_incoming_connection(processor, i_prot, o_prot),);
                }
                Err(e) => {
                    warn!("failed to accept remote connection with error {:?}", e);
                }
            }
        }

        Err(
            ::Error::Application(
                ApplicationError {
                    kind: ApplicationErrorKind::Unknown,
                    message: "aborted listen loop".into(),
                },
            ),
        )
    }


    fn new_protocols_for_connection(
        &mut self,
        stream: TcpStream,
    ) -> ::Result<(Box<TInputProtocol + Send>, Box<TOutputProtocol + Send>)> {
        // create the shared tcp stream
        let channel = TTcpChannel::with_stream(stream);

        // split it into two - one to be owned by the
        // input tran/proto and the other by the output
        let (r_chan, w_chan) = channel.split()?;

        // input protocol and transport
        let r_tran = self.r_trans_factory.create(Box::new(r_chan));
        let i_prot = self.i_proto_factory.create(r_tran);

        // output protocol and transport
        let w_tran = self.w_trans_factory.create(Box::new(w_chan));
        let o_prot = self.o_proto_factory.create(w_tran);

        Ok((i_prot, o_prot))
    }
}

fn handle_incoming_connection<PRC>(
    processor: Arc<PRC>,
    i_prot: Box<TInputProtocol>,
    o_prot: Box<TOutputProtocol>,
) where
    PRC: TProcessor,
{
    let mut i_prot = i_prot;
    let mut o_prot = o_prot;
    loop {
        let r = processor.process(&mut *i_prot, &mut *o_prot);
        if let Err(e) = r {
            warn!("processor completed with error: {:?}", e);
            break;
        }
    }
}
