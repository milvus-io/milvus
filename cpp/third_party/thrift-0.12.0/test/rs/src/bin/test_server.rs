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
extern crate log;
extern crate env_logger;

#[macro_use]
extern crate clap;
extern crate ordered_float;
extern crate thrift;
extern crate thrift_test;

use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, BTreeSet};
use std::thread;
use std::time::Duration;

use thrift::protocol::{TBinaryInputProtocolFactory, TBinaryOutputProtocolFactory,
                       TCompactInputProtocolFactory, TCompactOutputProtocolFactory,
                       TInputProtocolFactory, TOutputProtocolFactory};
use thrift::server::{TMultiplexedProcessor, TServer};
use thrift::transport::{TBufferedReadTransportFactory, TBufferedWriteTransportFactory,
                        TFramedReadTransportFactory, TFramedWriteTransportFactory,
                        TReadTransportFactory, TWriteTransportFactory};
use thrift_test::*;

fn main() {
    env_logger::init().expect("logger setup failed");

    debug!("initialized logger - running cross-test server");

    match run() {
        Ok(()) => info!("cross-test server succeeded"),
        Err(e) => {
            info!("cross-test server failed with error {:?}", e);
            std::process::exit(1);
        }
    }
}

fn run() -> thrift::Result<()> {

    // unsupported options:
    // --domain-socket
    // --named-pipe
    // --ssl
    let matches = clap_app!(rust_test_client =>
        (version: "1.0")
        (author: "Apache Thrift Developers <dev@thrift.apache.org>")
        (about: "Rust Thrift test server")
        (@arg port: --port +takes_value "port on which the test server listens")
        (@arg transport: --transport +takes_value "transport implementation to use (\"buffered\", \"framed\")")
        (@arg protocol: --protocol +takes_value "protocol implementation to use (\"binary\", \"compact\")")
        (@arg server_type: --server_type +takes_value "type of server instantiated (\"simple\", \"thread-pool\")")
        (@arg workers: -n --workers +takes_value "number of thread-pool workers (\"4\")")
    )
            .get_matches();

    let port = value_t!(matches, "port", u16).unwrap_or(9090);
    let transport = matches.value_of("transport").unwrap_or("buffered");
    let protocol = matches.value_of("protocol").unwrap_or("binary");
    let server_type = matches.value_of("server_type").unwrap_or("thread-pool");
    let workers = value_t!(matches, "workers", usize).unwrap_or(4);
    let listen_address = format!("127.0.0.1:{}", port);

    info!("binding to {}", listen_address);

    let (i_transport_factory, o_transport_factory): (Box<TReadTransportFactory>,
                                                     Box<TWriteTransportFactory>) =
        match &*transport {
            "buffered" => {
                (Box::new(TBufferedReadTransportFactory::new()),
                 Box::new(TBufferedWriteTransportFactory::new()))
            }
            "framed" => {
                (Box::new(TFramedReadTransportFactory::new()),
                 Box::new(TFramedWriteTransportFactory::new()))
            }
            unknown => {
                return Err(format!("unsupported transport type {}", unknown).into());
            }
        };

    let (i_protocol_factory, o_protocol_factory): (Box<TInputProtocolFactory>,
                                                   Box<TOutputProtocolFactory>) =
        match &*protocol {
            "binary" | "multi" | "multi:binary" => {
                (Box::new(TBinaryInputProtocolFactory::new()),
                 Box::new(TBinaryOutputProtocolFactory::new()))
            }
            "compact" | "multic" | "multi:compact" => {
                (Box::new(TCompactInputProtocolFactory::new()),
                 Box::new(TCompactOutputProtocolFactory::new()))
            }
            unknown => {
                return Err(format!("unsupported transport type {}", unknown).into());
            }
        };

    let test_processor = ThriftTestSyncProcessor::new(ThriftTestSyncHandlerImpl {});

    match &*server_type {
        "simple" | "thread-pool" => {
            if protocol == "multi" || protocol == "multic" {
                let second_service_processor = SecondServiceSyncProcessor::new(SecondServiceSyncHandlerImpl {},);

                let mut multiplexed_processor = TMultiplexedProcessor::new();
                multiplexed_processor
                    .register("ThriftTest", Box::new(test_processor), true)?;
                multiplexed_processor
                    .register("SecondService", Box::new(second_service_processor), false)?;

                let mut server = TServer::new(
                    i_transport_factory,
                    i_protocol_factory,
                    o_transport_factory,
                    o_protocol_factory,
                    multiplexed_processor,
                    workers,
                );

                server.listen(&listen_address)
            } else {
                let mut server = TServer::new(
                    i_transport_factory,
                    i_protocol_factory,
                    o_transport_factory,
                    o_protocol_factory,
                    test_processor,
                    workers,
                );

                server.listen(&listen_address)
            }
        }
        unknown => Err(format!("unsupported server type {}", unknown).into()),
    }
}

struct ThriftTestSyncHandlerImpl;
impl ThriftTestSyncHandler for ThriftTestSyncHandlerImpl {
    fn handle_test_void(&self) -> thrift::Result<()> {
        info!("testVoid()");
        Ok(())
    }

    fn handle_test_string(&self, thing: String) -> thrift::Result<String> {
        info!("testString({})", &thing);
        Ok(thing)
    }

    fn handle_test_bool(&self, thing: bool) -> thrift::Result<bool> {
        info!("testBool({})", thing);
        Ok(thing)
    }

    fn handle_test_byte(&self, thing: i8) -> thrift::Result<i8> {
        info!("testByte({})", thing);
        Ok(thing)
    }

    fn handle_test_i32(&self, thing: i32) -> thrift::Result<i32> {
        info!("testi32({})", thing);
        Ok(thing)
    }

    fn handle_test_i64(&self, thing: i64) -> thrift::Result<i64> {
        info!("testi64({})", thing);
        Ok(thing)
    }

    fn handle_test_double(&self, thing: OrderedFloat<f64>) -> thrift::Result<OrderedFloat<f64>> {
        info!("testDouble({})", thing);
        Ok(thing)
    }

    fn handle_test_binary(&self, thing: Vec<u8>) -> thrift::Result<Vec<u8>> {
        info!("testBinary({:?})", thing);
        Ok(thing)
    }

    fn handle_test_struct(&self, thing: Xtruct) -> thrift::Result<Xtruct> {
        info!("testStruct({:?})", thing);
        Ok(thing)
    }

    fn handle_test_nest(&self, thing: Xtruct2) -> thrift::Result<Xtruct2> {
        info!("testNest({:?})", thing);
        Ok(thing)
    }

    fn handle_test_map(&self, thing: BTreeMap<i32, i32>) -> thrift::Result<BTreeMap<i32, i32>> {
        info!("testMap({:?})", thing);
        Ok(thing)
    }

    fn handle_test_string_map(
        &self,
        thing: BTreeMap<String, String>,
    ) -> thrift::Result<BTreeMap<String, String>> {
        info!("testStringMap({:?})", thing);
        Ok(thing)
    }

    fn handle_test_set(&self, thing: BTreeSet<i32>) -> thrift::Result<BTreeSet<i32>> {
        info!("testSet({:?})", thing);
        Ok(thing)
    }

    fn handle_test_list(&self, thing: Vec<i32>) -> thrift::Result<Vec<i32>> {
        info!("testList({:?})", thing);
        Ok(thing)
    }

    fn handle_test_enum(&self, thing: Numberz) -> thrift::Result<Numberz> {
        info!("testEnum({:?})", thing);
        Ok(thing)
    }

    fn handle_test_typedef(&self, thing: UserId) -> thrift::Result<UserId> {
        info!("testTypedef({})", thing);
        Ok(thing)
    }

    /// @return map<i32,map<i32,i32>> - returns a dictionary with these values:
    /// {-4 => {-4 => -4, -3 => -3, -2 => -2, -1 => -1, }, 4 => {1 => 1, 2 =>
    /// 2, 3 => 3, 4 => 4, }, }
    fn handle_test_map_map(&self, hello: i32) -> thrift::Result<BTreeMap<i32, BTreeMap<i32, i32>>> {
        info!("testMapMap({})", hello);

        let mut inner_map_0: BTreeMap<i32, i32> = BTreeMap::new();
        for i in -4..(0 as i32) {
            inner_map_0.insert(i, i);
        }

        let mut inner_map_1: BTreeMap<i32, i32> = BTreeMap::new();
        for i in 1..5 {
            inner_map_1.insert(i, i);
        }

        let mut ret_map: BTreeMap<i32, BTreeMap<i32, i32>> = BTreeMap::new();
        ret_map.insert(-4, inner_map_0);
        ret_map.insert(4, inner_map_1);

        Ok(ret_map)
    }

    /// Creates a the returned map with these values and prints it out:
    ///     { 1 => { 2 => argument,
    ///              3 => argument,
    ///            },
    ///       2 => { 6 => <empty Insanity struct>, },
    ///     }
    /// return map<UserId, map<Numberz,Insanity>> - a map with the above values
    fn handle_test_insanity(
        &self,
        argument: Insanity,
    ) -> thrift::Result<BTreeMap<UserId, BTreeMap<Numberz, Insanity>>> {
        info!("testInsanity({:?})", argument);
        let mut map_0: BTreeMap<Numberz, Insanity> = BTreeMap::new();
        map_0.insert(Numberz::Two, argument.clone());
        map_0.insert(Numberz::Three, argument.clone());

        let mut map_1: BTreeMap<Numberz, Insanity> = BTreeMap::new();
        let insanity = Insanity {
            user_map: None,
            xtructs: None,
        };
        map_1.insert(Numberz::Six, insanity);

        let mut ret: BTreeMap<UserId, BTreeMap<Numberz, Insanity>> = BTreeMap::new();
        ret.insert(1, map_0);
        ret.insert(2, map_1);

        Ok(ret)
    }

    /// returns an Xtruct with:
    /// string_thing = "Hello2", byte_thing = arg0, i32_thing = arg1 and
    /// i64_thing = arg2
    fn handle_test_multi(
        &self,
        arg0: i8,
        arg1: i32,
        arg2: i64,
        _: BTreeMap<i16, String>,
        _: Numberz,
        _: UserId,
    ) -> thrift::Result<Xtruct> {
        let x_ret = Xtruct {
            string_thing: Some("Hello2".to_owned()),
            byte_thing: Some(arg0),
            i32_thing: Some(arg1),
            i64_thing: Some(arg2),
        };

        Ok(x_ret)
    }

    /// if arg == "Xception" throw Xception with errorCode = 1001 and message =
    /// arg
    /// else if arg == "TException" throw TException
    /// else do not throw anything
    fn handle_test_exception(&self, arg: String) -> thrift::Result<()> {
        info!("testException({})", arg);

        match &*arg {
            "Xception" => {
                Err(
                    (Xception {
                             error_code: Some(1001),
                             message: Some(arg),
                         })
                        .into(),
                )
            }
            "TException" => Err("this is a random error".into()),
            _ => Ok(()),
        }
    }

    /// if arg0 == "Xception":
    /// throw Xception with errorCode = 1001 and message = "This is an
    /// Xception"
    /// else if arg0 == "Xception2":
    /// throw Xception2 with errorCode = 2002 and struct_thing.string_thing =
    /// "This is an Xception2"
    // else:
    //   do not throw anything and return Xtruct with string_thing = arg1
    fn handle_test_multi_exception(&self, arg0: String, arg1: String) -> thrift::Result<Xtruct> {
        match &*arg0 {
            "Xception" => {
                Err(
                    (Xception {
                             error_code: Some(1001),
                             message: Some("This is an Xception".to_owned()),
                         })
                        .into(),
                )
            }
            "Xception2" => {
                Err(
                    (Xception2 {
                             error_code: Some(2002),
                             struct_thing: Some(
                            Xtruct {
                                string_thing: Some("This is an Xception2".to_owned()),
                                byte_thing: None,
                                i32_thing: None,
                                i64_thing: None,
                            },
                        ),
                         })
                        .into(),
                )
            }
            _ => {
                Ok(
                    Xtruct {
                        string_thing: Some(arg1),
                        byte_thing: None,
                        i32_thing: None,
                        i64_thing: None,
                    },
                )
            }
        }
    }

    fn handle_test_oneway(&self, seconds_to_sleep: i32) -> thrift::Result<()> {
        thread::sleep(Duration::from_secs(seconds_to_sleep as u64));
        Ok(())
    }
}

struct SecondServiceSyncHandlerImpl;
impl SecondServiceSyncHandler for SecondServiceSyncHandlerImpl {
    fn handle_secondtest_string(&self, thing: String) -> thrift::Result<String> {
        info!("(second)testString({})", &thing);
        let ret = format!("testString(\"{}\")", &thing);
        Ok(ret)
    }
}
