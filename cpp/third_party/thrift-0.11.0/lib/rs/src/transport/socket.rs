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

use std::convert::From;
use std::io;
use std::io::{ErrorKind, Read, Write};
use std::net::{Shutdown, TcpStream};

use {TransportErrorKind, new_transport_error};
use super::{ReadHalf, TIoChannel, WriteHalf};

/// Bidirectional TCP/IP channel.
///
/// # Examples
///
/// Create a `TTcpChannel`.
///
/// ```no_run
/// use std::io::{Read, Write};
/// use thrift::transport::TTcpChannel;
///
/// let mut c = TTcpChannel::new();
/// c.open("localhost:9090").unwrap();
///
/// let mut buf = vec![0u8; 4];
/// c.read(&mut buf).unwrap();
/// c.write(&vec![0, 1, 2]).unwrap();
/// ```
///
/// Create a `TTcpChannel` by wrapping an existing `TcpStream`.
///
/// ```no_run
/// use std::io::{Read, Write};
/// use std::net::TcpStream;
/// use thrift::transport::TTcpChannel;
///
/// let stream = TcpStream::connect("127.0.0.1:9189").unwrap();
///
/// // no need to call c.open() since we've already connected above
/// let mut c = TTcpChannel::with_stream(stream);
///
/// let mut buf = vec![0u8; 4];
/// c.read(&mut buf).unwrap();
/// c.write(&vec![0, 1, 2]).unwrap();
/// ```
#[derive(Debug, Default)]
pub struct TTcpChannel {
    stream: Option<TcpStream>,
}

impl TTcpChannel {
    /// Create an uninitialized `TTcpChannel`.
    ///
    /// The returned instance must be opened using `TTcpChannel::open(...)`
    /// before it can be used.
    pub fn new() -> TTcpChannel {
        TTcpChannel { stream: None }
    }

    /// Create a `TTcpChannel` that wraps an existing `TcpStream`.
    ///
    /// The passed-in stream is assumed to have been opened before being wrapped
    /// by the created `TTcpChannel` instance.
    pub fn with_stream(stream: TcpStream) -> TTcpChannel {
        TTcpChannel { stream: Some(stream) }
    }

    /// Connect to `remote_address`, which should have the form `host:port`.
    pub fn open(&mut self, remote_address: &str) -> ::Result<()> {
        if self.stream.is_some() {
            Err(
                new_transport_error(
                    TransportErrorKind::AlreadyOpen,
                    "tcp connection previously opened",
                ),
            )
        } else {
            match TcpStream::connect(&remote_address) {
                Ok(s) => {
                    self.stream = Some(s);
                    Ok(())
                }
                Err(e) => Err(From::from(e)),
            }
        }
    }

    /// Shut down this channel.
    ///
    /// Both send and receive halves are closed, and this instance can no
    /// longer be used to communicate with another endpoint.
    pub fn close(&mut self) -> ::Result<()> {
        self.if_set(|s| s.shutdown(Shutdown::Both))
            .map_err(From::from)
    }

    fn if_set<F, T>(&mut self, mut stream_operation: F) -> io::Result<T>
    where
        F: FnMut(&mut TcpStream) -> io::Result<T>,
    {

        if let Some(ref mut s) = self.stream {
            stream_operation(s)
        } else {
            Err(io::Error::new(ErrorKind::NotConnected, "tcp endpoint not connected"),)
        }
    }
}

impl TIoChannel for TTcpChannel {
    fn split(self) -> ::Result<(ReadHalf<Self>, WriteHalf<Self>)>
    where
        Self: Sized,
    {
        let mut s = self;

        s.stream
            .as_mut()
            .and_then(|s| s.try_clone().ok())
            .map(
                |cloned| {
                    (ReadHalf { handle: TTcpChannel { stream: s.stream.take() } },
                     WriteHalf { handle: TTcpChannel { stream: Some(cloned) } })
                },
            )
            .ok_or_else(
                || {
                    new_transport_error(
                        TransportErrorKind::Unknown,
                        "cannot clone underlying tcp stream",
                    )
                },
            )
    }
}

impl Read for TTcpChannel {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        self.if_set(|s| s.read(b))
    }
}

impl Write for TTcpChannel {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.if_set(|s| s.write_all(b)).map(|_| b.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.if_set(|s| s.flush())
    }
}
