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

//! Types used to send and receive bytes over an I/O channel.
//!
//! The core types are the `TReadTransport`, `TWriteTransport` and the
//! `TIoChannel` traits, through which `TInputProtocol` or
//! `TOutputProtocol` can receive and send primitives over the wire. While
//! `TInputProtocol` and `TOutputProtocol` instances deal with language primitives
//! the types in this module understand only bytes.

use std::io;
use std::io::{Read, Write};
use std::ops::{Deref, DerefMut};

#[cfg(test)]
macro_rules! assert_eq_transport_num_written_bytes {
    ($transport:ident, $num_written_bytes:expr) => {
        {
            assert_eq!($transport.channel.write_bytes().len(), $num_written_bytes);
        }
    };
}


#[cfg(test)]
macro_rules! assert_eq_transport_written_bytes {
    ($transport:ident, $expected_bytes:ident) => {
        {
            assert_eq!($transport.channel.write_bytes(), &$expected_bytes);
        }
    };
}

mod buffered;
mod framed;
mod socket;
mod mem;

pub use self::buffered::{TBufferedReadTransport, TBufferedReadTransportFactory,
                         TBufferedWriteTransport, TBufferedWriteTransportFactory};
pub use self::framed::{TFramedReadTransport, TFramedReadTransportFactory, TFramedWriteTransport,
                       TFramedWriteTransportFactory};
pub use self::mem::TBufferChannel;
pub use self::socket::TTcpChannel;

/// Identifies a transport used by a `TInputProtocol` to receive bytes.
pub trait TReadTransport: Read {}

/// Helper type used by a server to create `TReadTransport` instances for
/// accepted client connections.
pub trait TReadTransportFactory {
    /// Create a `TTransport` that wraps a channel over which bytes are to be read.
    fn create(&self, channel: Box<Read + Send>) -> Box<TReadTransport + Send>;
}

/// Identifies a transport used by `TOutputProtocol` to send bytes.
pub trait TWriteTransport: Write {}

/// Helper type used by a server to create `TWriteTransport` instances for
/// accepted client connections.
pub trait TWriteTransportFactory {
    /// Create a `TTransport` that wraps a channel over which bytes are to be sent.
    fn create(&self, channel: Box<Write + Send>) -> Box<TWriteTransport + Send>;
}

impl<T> TReadTransport for T
where
    T: Read,
{
}

impl<T> TWriteTransport for T
where
    T: Write,
{
}

// FIXME: implement the Debug trait for boxed transports

impl<T> TReadTransportFactory for Box<T>
where
    T: TReadTransportFactory + ?Sized,
{
    fn create(&self, channel: Box<Read + Send>) -> Box<TReadTransport + Send> {
        (**self).create(channel)
    }
}

impl<T> TWriteTransportFactory for Box<T>
where
    T: TWriteTransportFactory + ?Sized,
{
    fn create(&self, channel: Box<Write + Send>) -> Box<TWriteTransport + Send> {
        (**self).create(channel)
    }
}

/// Identifies a splittable bidirectional I/O channel used to send and receive bytes.
pub trait TIoChannel: Read + Write {
    /// Split the channel into a readable half and a writable half, where the
    /// readable half implements `io::Read` and the writable half implements
    /// `io::Write`. Returns `None` if the channel was not initialized, or if it
    /// cannot be split safely.
    ///
    /// Returned halves may share the underlying OS channel or buffer resources.
    /// Implementations **should ensure** that these two halves can be safely
    /// used independently by concurrent threads.
    fn split(self) -> ::Result<(::transport::ReadHalf<Self>, ::transport::WriteHalf<Self>)>
    where
        Self: Sized;
}

/// The readable half of an object returned from `TIoChannel::split`.
#[derive(Debug)]
pub struct ReadHalf<C>
where
    C: Read,
{
    handle: C,
}

/// The writable half of an object returned from `TIoChannel::split`.
#[derive(Debug)]
pub struct WriteHalf<C>
where
    C: Write,
{
    handle: C,
}

impl<C> Read for ReadHalf<C>
where
    C: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.handle.read(buf)
    }
}

impl<C> Write for WriteHalf<C>
where
    C: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.handle.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.handle.flush()
    }
}

impl<C> Deref for ReadHalf<C>
where
    C: Read,
{
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl<C> DerefMut for ReadHalf<C>
where
    C: Read,
{
    fn deref_mut(&mut self) -> &mut C {
        &mut self.handle
    }
}

impl<C> Deref for WriteHalf<C>
where
    C: Write,
{
    type Target = C;

    fn deref(&self) -> &Self::Target {
        &self.handle
    }
}

impl<C> DerefMut for WriteHalf<C>
where
    C: Write,
{
    fn deref_mut(&mut self) -> &mut C {
        &mut self.handle
    }
}

#[cfg(test)]
mod tests {

    use std::io::Cursor;

    use super::*;

    #[test]
    fn must_create_usable_read_channel_from_concrete_read_type() {
        let r = Cursor::new([0, 1, 2]);
        let _ = TBufferedReadTransport::new(r);
    }

    #[test]
    fn must_create_usable_read_channel_from_boxed_read() {
        let r: Box<Read> = Box::new(Cursor::new([0, 1, 2]));
        let _ = TBufferedReadTransport::new(r);
    }

    #[test]
    fn must_create_usable_write_channel_from_concrete_write_type() {
        let w = vec![0u8; 10];
        let _ = TBufferedWriteTransport::new(w);
    }

    #[test]
    fn must_create_usable_write_channel_from_boxed_write() {
        let w: Box<Write> = Box::new(vec![0u8; 10]);
        let _ = TBufferedWriteTransport::new(w);
    }

    #[test]
    fn must_create_usable_read_transport_from_concrete_read_transport() {
        let r = Cursor::new([0, 1, 2]);
        let mut t = TBufferedReadTransport::new(r);
        takes_read_transport(&mut t)
    }

    #[test]
    fn must_create_usable_read_transport_from_boxed_read() {
        let r = Cursor::new([0, 1, 2]);
        let mut t: Box<TReadTransport> = Box::new(TBufferedReadTransport::new(r));
        takes_read_transport(&mut t)
    }

    #[test]
    fn must_create_usable_write_transport_from_concrete_write_transport() {
        let w = vec![0u8; 10];
        let mut t = TBufferedWriteTransport::new(w);
        takes_write_transport(&mut t)
    }

    #[test]
    fn must_create_usable_write_transport_from_boxed_write() {
        let w = vec![0u8; 10];
        let mut t: Box<TWriteTransport> = Box::new(TBufferedWriteTransport::new(w));
        takes_write_transport(&mut t)
    }

    fn takes_read_transport<R>(t: &mut R)
    where
        R: TReadTransport,
    {
        t.bytes();
    }

    fn takes_write_transport<W>(t: &mut W)
    where
        W: TWriteTransport,
    {
        t.flush().unwrap();
    }
}
