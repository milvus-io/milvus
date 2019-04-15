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

extern crate kitchen_sink;
extern crate thrift;

use std::convert::Into;

use kitchen_sink::base_two::{TNapkinServiceSyncClient, TRamenServiceSyncClient};
use kitchen_sink::midlayer::{MealServiceSyncClient, TMealServiceSyncClient};
use kitchen_sink::recursive;
use kitchen_sink::recursive::{CoRec, CoRec2, RecList, RecTree, TTestServiceSyncClient};
use kitchen_sink::ultimate::{FullMealServiceSyncClient, TFullMealServiceSyncClient};
use thrift::protocol::{
    TBinaryInputProtocol, TBinaryOutputProtocol, TCompactInputProtocol, TCompactOutputProtocol,
    TInputProtocol, TOutputProtocol,
};
use thrift::transport::{
    ReadHalf, TFramedReadTransport, TFramedWriteTransport, TIoChannel, TTcpChannel, WriteHalf,
};

fn main() {
    match run() {
        Ok(()) => println!("kitchen sink client completed successfully"),
        Err(e) => {
            println!("kitchen sink client failed with error {:?}", e);
            std::process::exit(1);
        }
    }
}

fn run() -> thrift::Result<()> {
    let matches = clap_app!(rust_kitchen_sink_client =>
        (version: "0.1.0")
        (author: "Apache Thrift Developers <dev@thrift.apache.org>")
        (about: "Thrift Rust kitchen sink client")
        (@arg host: --host +takes_value "Host on which the Thrift test server is located")
        (@arg port: --port +takes_value "Port on which the Thrift test server is listening")
        (@arg protocol: --protocol +takes_value "Thrift protocol implementation to use (\"binary\", \"compact\")")
        (@arg service: --service +takes_value "Service type to contact (\"part\", \"full\", \"recursive\")")
    )
            .get_matches();

    let host = matches.value_of("host").unwrap_or("127.0.0.1");
    let port = value_t!(matches, "port", u16).unwrap_or(9090);
    let protocol = matches.value_of("protocol").unwrap_or("compact");
    let service = matches.value_of("service").unwrap_or("part");

    let (i_chan, o_chan) = tcp_channel(host, port)?;
    let (i_tran, o_tran) = (
        TFramedReadTransport::new(i_chan),
        TFramedWriteTransport::new(o_chan),
    );

    let (i_prot, o_prot): (Box<TInputProtocol>, Box<TOutputProtocol>) = match protocol {
        "binary" => (
            Box::new(TBinaryInputProtocol::new(i_tran, true)),
            Box::new(TBinaryOutputProtocol::new(o_tran, true)),
        ),
        "compact" => (
            Box::new(TCompactInputProtocol::new(i_tran)),
            Box::new(TCompactOutputProtocol::new(o_tran)),
        ),
        unmatched => return Err(format!("unsupported protocol {}", unmatched).into()),
    };

    run_client(service, i_prot, o_prot)
}

fn run_client(
    service: &str,
    i_prot: Box<TInputProtocol>,
    o_prot: Box<TOutputProtocol>,
) -> thrift::Result<()> {
    match service {
        "full" => exec_full_meal_client(i_prot, o_prot),
        "part" => exec_meal_client(i_prot, o_prot),
        "recursive" => exec_recursive_client(i_prot, o_prot),
        _ => Err(thrift::Error::from(format!(
            "unknown service type {}",
            service
        ))),
    }
}

fn tcp_channel(
    host: &str,
    port: u16,
) -> thrift::Result<(ReadHalf<TTcpChannel>, WriteHalf<TTcpChannel>)> {
    let mut c = TTcpChannel::new();
    c.open(&format!("{}:{}", host, port))?;
    c.split()
}

fn exec_meal_client(
    i_prot: Box<TInputProtocol>,
    o_prot: Box<TOutputProtocol>,
) -> thrift::Result<()> {
    let mut client = MealServiceSyncClient::new(i_prot, o_prot);

    // client.full_meal(); // <-- IMPORTANT: if you uncomment this, compilation *should* fail
    // this is because the MealService struct does not contain the appropriate service marker

    // only the following three calls work
    execute_call("part", "ramen", || client.ramen(50)).map(|_| ())?;
    execute_call("part", "meal", || client.meal()).map(|_| ())?;
    execute_call("part", "napkin", || client.napkin()).map(|_| ())?;

    Ok(())
}

fn exec_full_meal_client(
    i_prot: Box<TInputProtocol>,
    o_prot: Box<TOutputProtocol>,
) -> thrift::Result<()> {
    let mut client = FullMealServiceSyncClient::new(i_prot, o_prot);

    execute_call("full", "ramen", || client.ramen(100)).map(|_| ())?;
    execute_call("full", "meal", || client.meal()).map(|_| ())?;
    execute_call("full", "napkin", || client.napkin()).map(|_| ())?;
    execute_call("full", "full meal", || client.full_meal()).map(|_| ())?;

    Ok(())
}

fn exec_recursive_client(
    i_prot: Box<TInputProtocol>,
    o_prot: Box<TOutputProtocol>,
) -> thrift::Result<()> {
    let mut client = recursive::TestServiceSyncClient::new(i_prot, o_prot);

    let tree = RecTree {
        children: Some(vec![Box::new(RecTree {
            children: Some(vec![
                Box::new(RecTree {
                    children: None,
                    item: Some(3),
                }),
                Box::new(RecTree {
                    children: None,
                    item: Some(4),
                }),
            ]),
            item: Some(2),
        })]),
        item: Some(1),
    };

    let expected_tree = RecTree {
        children: Some(vec![Box::new(RecTree {
            children: Some(vec![
                Box::new(RecTree {
                    children: Some(Vec::new()), // remote returns an empty list
                    item: Some(3),
                }),
                Box::new(RecTree {
                    children: Some(Vec::new()), // remote returns an empty list
                    item: Some(4),
                }),
            ]),
            item: Some(2),
        })]),
        item: Some(1),
    };

    let returned_tree = execute_call("recursive", "echo_tree", || client.echo_tree(tree.clone()))?;
    if returned_tree != expected_tree {
        return Err(format!(
            "mismatched recursive tree {:?} {:?}",
            expected_tree, returned_tree
        )
        .into());
    }

    let list = RecList {
        nextitem: Some(Box::new(RecList {
            nextitem: Some(Box::new(RecList {
                nextitem: None,
                item: Some(3),
            })),
            item: Some(2),
        })),
        item: Some(1),
    };
    let returned_list = execute_call("recursive", "echo_list", || client.echo_list(list.clone()))?;
    if returned_list != list {
        return Err(format!("mismatched recursive list {:?} {:?}", list, returned_list).into());
    }

    let co_rec = CoRec {
        other: Some(Box::new(CoRec2 {
            other: Some(CoRec {
                other: Some(Box::new(CoRec2 { other: None })),
            }),
        })),
    };
    let returned_co_rec = execute_call("recursive", "echo_co_rec", || {
        client.echo_co_rec(co_rec.clone())
    })?;
    if returned_co_rec != co_rec {
        return Err(format!("mismatched co_rec {:?} {:?}", co_rec, returned_co_rec).into());
    }

    Ok(())
}

fn execute_call<F, R>(service_type: &str, call_name: &str, mut f: F) -> thrift::Result<R>
where
    F: FnMut() -> thrift::Result<R>,
{
    let res = f();

    match res {
        Ok(_) => println!("{}: completed {} call", service_type, call_name),
        Err(ref e) => println!(
            "{}: failed {} call with error {:?}",
            service_type, call_name, e
        ),
    }

    res
}
