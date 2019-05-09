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

#[macro_use]
extern crate clap;

extern crate thrift;
extern crate thrift_tutorial;

use thrift::protocol::{TCompactInputProtocol, TCompactOutputProtocol};
use thrift::transport::{ReadHalf, TFramedReadTransport, TFramedWriteTransport, TIoChannel,
                        TTcpChannel, WriteHalf};

use thrift_tutorial::shared::TSharedServiceSyncClient;
use thrift_tutorial::tutorial::{CalculatorSyncClient, Operation, TCalculatorSyncClient, Work};

fn main() {
    match run() {
        Ok(()) => println!("tutorial client ran successfully"),
        Err(e) => {
            println!("tutorial client failed with error {:?}", e);
            std::process::exit(1);
        }
    }
}

fn run() -> thrift::Result<()> {
    let options = clap_app!(rust_tutorial_client =>
        (version: "0.1.0")
        (author: "Apache Thrift Developers <dev@thrift.apache.org>")
        (about: "Thrift Rust tutorial client")
        (@arg host: --host +takes_value "host on which the tutorial server listens")
        (@arg port: --port +takes_value "port on which the tutorial server listens")
    );
    let matches = options.get_matches();

    // get any passed-in args or the defaults
    let host = matches.value_of("host").unwrap_or("127.0.0.1");
    let port = value_t!(matches, "port", u16).unwrap_or(9090);

    // build our client and connect to the host:port
    let mut client = new_client(host, port)?;

    // alright!
    // let's start making some calls

    // let's start with a ping; the server should respond
    println!("ping!");
    client.ping()?;

    // simple add
    println!("add");
    let res = client.add(1, 2)?;
    println!("added 1, 2 and got {}", res);

    let logid = 32;

    // let's do...a multiply!
    let res = client
        .calculate(logid, Work::new(7, 8, Operation::MULTIPLY, None))?;
    println!("multiplied 7 and 8 and got {}", res);

    // let's get the log for it
    let res = client.get_struct(32)?;
    println!("got log {:?} for operation {}", res, logid);

    // ok - let's be bad :(
    // do a divide by 0
    // logid doesn't matter; won't be recorded
    let res = client.calculate(77, Work::new(2, 0, Operation::DIVIDE, "we bad".to_owned()));

    // we should have gotten an exception back
    match res {
        Ok(v) => panic!("should not have succeeded with result {}", v),
        Err(e) => println!("divide by zero failed with error {:?}", e),
    }

    // let's do a one-way call
    println!("zip");
    client.zip()?;

    // and then close out with a final ping
    println!("ping!");
    client.ping()?;

    Ok(())
}

type ClientInputProtocol = TCompactInputProtocol<TFramedReadTransport<ReadHalf<TTcpChannel>>>;
type ClientOutputProtocol = TCompactOutputProtocol<TFramedWriteTransport<WriteHalf<TTcpChannel>>>;

fn new_client
    (
    host: &str,
    port: u16,
) -> thrift::Result<CalculatorSyncClient<ClientInputProtocol, ClientOutputProtocol>> {
    let mut c = TTcpChannel::new();

    // open the underlying TCP stream
    println!("connecting to tutorial server on {}:{}", host, port);
    c.open(&format!("{}:{}", host, port))?;

    // clone the TCP channel into two halves, one which
    // we'll use for reading, the other for writing
    let (i_chan, o_chan) = c.split()?;

    // wrap the raw sockets (slow) with a buffered transport of some kind
    let i_tran = TFramedReadTransport::new(i_chan);
    let o_tran = TFramedWriteTransport::new(o_chan);

    // now create the protocol implementations
    let i_prot = TCompactInputProtocol::new(i_tran);
    let o_prot = TCompactOutputProtocol::new(o_tran);

    // we're done!
    Ok(CalculatorSyncClient::new(i_prot, o_prot))
}
