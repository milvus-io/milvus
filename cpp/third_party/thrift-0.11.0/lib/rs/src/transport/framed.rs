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

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::cmp;
use std::io;
use std::io::{ErrorKind, Read, Write};

use super::{TReadTransport, TReadTransportFactory, TWriteTransport, TWriteTransportFactory};

/// Default capacity of the read buffer in bytes.
const READ_CAPACITY: usize = 4096;

/// Default capacity of the write buffer in bytes.
const WRITE_CAPACITY: usize = 4096;

/// Transport that reads framed messages.
///
/// A `TFramedReadTransport` maintains a fixed-size internal read buffer.
/// On a call to `TFramedReadTransport::read(...)` one full message - both
/// fixed-length header and bytes - is read from the wrapped channel and
/// buffered. Subsequent read calls are serviced from the internal buffer
/// until it is exhausted, at which point the next full message is read
/// from the wrapped channel.
///
/// # Examples
///
/// Create and use a `TFramedReadTransport`.
///
/// ```no_run
/// use std::io::Read;
/// use thrift::transport::{TFramedReadTransport, TTcpChannel};
///
/// let mut c = TTcpChannel::new();
/// c.open("localhost:9090").unwrap();
///
/// let mut t = TFramedReadTransport::new(c);
///
/// t.read(&mut vec![0u8; 1]).unwrap();
/// ```
#[derive(Debug)]
pub struct TFramedReadTransport<C>
where
    C: Read,
{
    buf: Box<[u8]>,
    pos: usize,
    cap: usize,
    chan: C,
}

impl<C> TFramedReadTransport<C>
where
    C: Read,
{
    /// Create a `TFramedTransport` with default-sized internal read and
    /// write buffers that wraps the given `TIoChannel`.
    pub fn new(channel: C) -> TFramedReadTransport<C> {
        TFramedReadTransport::with_capacity(READ_CAPACITY, channel)
    }

    /// Create a `TFramedTransport` with an internal read buffer of size
    /// `read_capacity` and an internal write buffer of size
    /// `write_capacity` that wraps the given `TIoChannel`.
    pub fn with_capacity(read_capacity: usize, channel: C) -> TFramedReadTransport<C> {
        TFramedReadTransport {
            buf: vec![0; read_capacity].into_boxed_slice(),
            pos: 0,
            cap: 0,
            chan: channel,
        }
    }
}

impl<C> Read for TFramedReadTransport<C>
where
    C: Read,
{
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        if self.cap - self.pos == 0 {
            let message_size = self.chan.read_i32::<BigEndian>()? as usize;
            if message_size > self.buf.len() {
                return Err(
                    io::Error::new(
                        ErrorKind::Other,
                        format!(
                            "bytes to be read ({}) exceeds buffer \
                                                   capacity ({})",
                            message_size,
                            self.buf.len()
                        ),
                    ),
                );
            }
            self.chan.read_exact(&mut self.buf[..message_size])?;
            self.pos = 0;
            self.cap = message_size as usize;
        }

        let nread = cmp::min(b.len(), self.cap - self.pos);
        b[..nread].clone_from_slice(&self.buf[self.pos..self.pos + nread]);
        self.pos += nread;

        Ok(nread)
    }
}

/// Factory for creating instances of `TFramedReadTransport`.
#[derive(Default)]
pub struct TFramedReadTransportFactory;

impl TFramedReadTransportFactory {
    pub fn new() -> TFramedReadTransportFactory {
        TFramedReadTransportFactory {}
    }
}

impl TReadTransportFactory for TFramedReadTransportFactory {
    /// Create a `TFramedReadTransport`.
    fn create(&self, channel: Box<Read + Send>) -> Box<TReadTransport + Send> {
        Box::new(TFramedReadTransport::new(channel))
    }
}

/// Transport that writes framed messages.
///
/// A `TFramedWriteTransport` maintains a fixed-size internal write buffer. All
/// writes are made to this buffer and are sent to the wrapped channel only
/// when `TFramedWriteTransport::flush()` is called. On a flush a fixed-length
/// header with a count of the buffered bytes is written, followed by the bytes
/// themselves.
///
/// # Examples
///
/// Create and use a `TFramedWriteTransport`.
///
/// ```no_run
/// use std::io::Write;
/// use thrift::transport::{TFramedWriteTransport, TTcpChannel};
///
/// let mut c = TTcpChannel::new();
/// c.open("localhost:9090").unwrap();
///
/// let mut t = TFramedWriteTransport::new(c);
///
/// t.write(&[0x00]).unwrap();
/// t.flush().unwrap();
/// ```
#[derive(Debug)]
pub struct TFramedWriteTransport<C>
where
    C: Write,
{
    buf: Box<[u8]>,
    pos: usize,
    channel: C,
}

impl<C> TFramedWriteTransport<C>
where
    C: Write,
{
    /// Create a `TFramedTransport` with default-sized internal read and
    /// write buffers that wraps the given `TIoChannel`.
    pub fn new(channel: C) -> TFramedWriteTransport<C> {
        TFramedWriteTransport::with_capacity(WRITE_CAPACITY, channel)
    }

    /// Create a `TFramedTransport` with an internal read buffer of size
    /// `read_capacity` and an internal write buffer of size
    /// `write_capacity` that wraps the given `TIoChannel`.
    pub fn with_capacity(write_capacity: usize, channel: C) -> TFramedWriteTransport<C> {
        TFramedWriteTransport {
            buf: vec![0; write_capacity].into_boxed_slice(),
            pos: 0,
            channel: channel,
        }
    }
}

impl<C> Write for TFramedWriteTransport<C>
where
    C: Write,
{
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        if b.len() > (self.buf.len() - self.pos) {
            return Err(
                io::Error::new(
                    ErrorKind::Other,
                    format!(
                        "bytes to be written ({}) exceeds buffer \
                                               capacity ({})",
                        b.len(),
                        self.buf.len() - self.pos
                    ),
                ),
            );
        }

        let nwrite = b.len(); // always less than available write buffer capacity
        self.buf[self.pos..(self.pos + nwrite)].clone_from_slice(b);
        self.pos += nwrite;
        Ok(nwrite)
    }

    fn flush(&mut self) -> io::Result<()> {
        let message_size = self.pos;

        if let 0 = message_size {
            return Ok(());
        } else {
            self.channel
                .write_i32::<BigEndian>(message_size as i32)?;
        }

        let mut byte_index = 0;
        while byte_index < self.pos {
            let nwrite = self.channel.write(&self.buf[byte_index..self.pos])?;
            byte_index = cmp::min(byte_index + nwrite, self.pos);
        }

        self.pos = 0;
        self.channel.flush()
    }
}

/// Factory for creating instances of `TFramedWriteTransport`.
#[derive(Default)]
pub struct TFramedWriteTransportFactory;

impl TFramedWriteTransportFactory {
    pub fn new() -> TFramedWriteTransportFactory {
        TFramedWriteTransportFactory {}
    }
}

impl TWriteTransportFactory for TFramedWriteTransportFactory {
    /// Create a `TFramedWriteTransport`.
    fn create(&self, channel: Box<Write + Send>) -> Box<TWriteTransport + Send> {
        Box::new(TFramedWriteTransport::new(channel))
    }
}

#[cfg(test)]
mod tests {
    //    use std::io::{Read, Write};
    //
    //    use super::*;
    //    use ::transport::mem::TBufferChannel;
}
