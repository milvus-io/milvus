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

use std::cmp;
use std::io;
use std::io::{Read, Write};

use super::{TReadTransport, TReadTransportFactory, TWriteTransport, TWriteTransportFactory};

/// Default capacity of the read buffer in bytes.
const READ_CAPACITY: usize = 4096;

/// Default capacity of the write buffer in bytes..
const WRITE_CAPACITY: usize = 4096;

/// Transport that reads messages via an internal buffer.
///
/// A `TBufferedReadTransport` maintains a fixed-size internal read buffer.
/// On a call to `TBufferedReadTransport::read(...)` one full message - both
/// fixed-length header and bytes - is read from the wrapped channel and buffered.
/// Subsequent read calls are serviced from the internal buffer until it is
/// exhausted, at which point the next full message is read from the wrapped
/// channel.
///
/// # Examples
///
/// Create and use a `TBufferedReadTransport`.
///
/// ```no_run
/// use std::io::Read;
/// use thrift::transport::{TBufferedReadTransport, TTcpChannel};
///
/// let mut c = TTcpChannel::new();
/// c.open("localhost:9090").unwrap();
///
/// let mut t = TBufferedReadTransport::new(c);
///
/// t.read(&mut vec![0u8; 1]).unwrap();
/// ```
#[derive(Debug)]
pub struct TBufferedReadTransport<C>
where
    C: Read,
{
    buf: Box<[u8]>,
    pos: usize,
    cap: usize,
    chan: C,
}

impl<C> TBufferedReadTransport<C>
where
    C: Read,
{
    /// Create a `TBufferedTransport` with default-sized internal read and
    /// write buffers that wraps the given `TIoChannel`.
    pub fn new(channel: C) -> TBufferedReadTransport<C> {
        TBufferedReadTransport::with_capacity(READ_CAPACITY, channel)
    }

    /// Create a `TBufferedTransport` with an internal read buffer of size
    /// `read_capacity` and an internal write buffer of size
    /// `write_capacity` that wraps the given `TIoChannel`.
    pub fn with_capacity(read_capacity: usize, channel: C) -> TBufferedReadTransport<C> {
        TBufferedReadTransport {
            buf: vec![0; read_capacity].into_boxed_slice(),
            pos: 0,
            cap: 0,
            chan: channel,
        }
    }

    fn get_bytes(&mut self) -> io::Result<&[u8]> {
        if self.cap - self.pos == 0 {
            self.pos = 0;
            self.cap = self.chan.read(&mut self.buf)?;
        }

        Ok(&self.buf[self.pos..self.cap])
    }

    fn consume(&mut self, consumed: usize) {
        // TODO: was a bug here += <-- test somehow
        self.pos = cmp::min(self.cap, self.pos + consumed);
    }
}

impl<C> Read for TBufferedReadTransport<C>
where
    C: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut bytes_read = 0;

        loop {
            let nread = {
                let avail_bytes = self.get_bytes()?;
                let avail_space = buf.len() - bytes_read;
                let nread = cmp::min(avail_space, avail_bytes.len());
                buf[bytes_read..(bytes_read + nread)].copy_from_slice(&avail_bytes[..nread]);
                nread
            };

            self.consume(nread);
            bytes_read += nread;

            if bytes_read == buf.len() || nread == 0 {
                break;
            }
        }

        Ok(bytes_read)
    }
}

/// Factory for creating instances of `TBufferedReadTransport`.
#[derive(Default)]
pub struct TBufferedReadTransportFactory;

impl TBufferedReadTransportFactory {
    pub fn new() -> TBufferedReadTransportFactory {
        TBufferedReadTransportFactory {}
    }
}

impl TReadTransportFactory for TBufferedReadTransportFactory {
    /// Create a `TBufferedReadTransport`.
    fn create(&self, channel: Box<Read + Send>) -> Box<TReadTransport + Send> {
        Box::new(TBufferedReadTransport::new(channel))
    }
}

/// Transport that writes messages via an internal buffer.
///
/// A `TBufferedWriteTransport` maintains a fixed-size internal write buffer.
/// All writes are made to this buffer and are sent to the wrapped channel only
/// when `TBufferedWriteTransport::flush()` is called. On a flush a fixed-length
/// header with a count of the buffered bytes is written, followed by the bytes
/// themselves.
///
/// # Examples
///
/// Create and use a `TBufferedWriteTransport`.
///
/// ```no_run
/// use std::io::Write;
/// use thrift::transport::{TBufferedWriteTransport, TTcpChannel};
///
/// let mut c = TTcpChannel::new();
/// c.open("localhost:9090").unwrap();
///
/// let mut t = TBufferedWriteTransport::new(c);
///
/// t.write(&[0x00]).unwrap();
/// t.flush().unwrap();
/// ```
#[derive(Debug)]
pub struct TBufferedWriteTransport<C>
where
    C: Write,
{
    buf: Vec<u8>,
    channel: C,
}

impl<C> TBufferedWriteTransport<C>
where
    C: Write,
{
    /// Create a `TBufferedTransport` with default-sized internal read and
    /// write buffers that wraps the given `TIoChannel`.
    pub fn new(channel: C) -> TBufferedWriteTransport<C> {
        TBufferedWriteTransport::with_capacity(WRITE_CAPACITY, channel)
    }

    /// Create a `TBufferedTransport` with an internal read buffer of size
    /// `read_capacity` and an internal write buffer of size
    /// `write_capacity` that wraps the given `TIoChannel`.
    pub fn with_capacity(write_capacity: usize, channel: C) -> TBufferedWriteTransport<C> {
        TBufferedWriteTransport {
            buf: Vec::with_capacity(write_capacity),
            channel: channel,
        }
    }
}

impl<C> Write for TBufferedWriteTransport<C>
where
    C: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let avail_bytes = cmp::min(buf.len(), self.buf.capacity() - self.buf.len());
        self.buf.extend_from_slice(&buf[..avail_bytes]);
        assert!(
            self.buf.len() <= self.buf.capacity(),
            "copy overflowed buffer"
        );
        Ok(avail_bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.channel.write_all(&self.buf)?;
        self.channel.flush()?;
        self.buf.clear();
        Ok(())
    }
}

/// Factory for creating instances of `TBufferedWriteTransport`.
#[derive(Default)]
pub struct TBufferedWriteTransportFactory;

impl TBufferedWriteTransportFactory {
    pub fn new() -> TBufferedWriteTransportFactory {
        TBufferedWriteTransportFactory {}
    }
}

impl TWriteTransportFactory for TBufferedWriteTransportFactory {
    /// Create a `TBufferedWriteTransport`.
    fn create(&self, channel: Box<Write + Send>) -> Box<TWriteTransport + Send> {
        Box::new(TBufferedWriteTransport::new(channel))
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};

    use super::*;
    use transport::TBufferChannel;

    #[test]
    fn must_return_zero_if_read_buffer_is_empty() {
        let mem = TBufferChannel::with_capacity(10, 0);
        let mut t = TBufferedReadTransport::with_capacity(10, mem);

        let mut b = vec![0; 10];
        let read_result = t.read(&mut b);

        assert_eq!(read_result.unwrap(), 0);
    }

    #[test]
    fn must_return_zero_if_caller_reads_into_zero_capacity_buffer() {
        let mem = TBufferChannel::with_capacity(10, 0);
        let mut t = TBufferedReadTransport::with_capacity(10, mem);

        let read_result = t.read(&mut []);

        assert_eq!(read_result.unwrap(), 0);
    }

    #[test]
    fn must_return_zero_if_nothing_more_can_be_read() {
        let mem = TBufferChannel::with_capacity(4, 0);
        let mut t = TBufferedReadTransport::with_capacity(4, mem);

        t.chan.set_readable_bytes(&[0, 1, 2, 3]);

        // read buffer is exactly the same size as bytes available
        let mut buf = vec![0u8; 4];
        let read_result = t.read(&mut buf);

        // we've read exactly 4 bytes
        assert_eq!(read_result.unwrap(), 4);
        assert_eq!(&buf, &[0, 1, 2, 3]);

        // try read again
        let buf_again = vec![0u8; 4];
        let read_result = t.read(&mut buf);

        // this time, 0 bytes and we haven't changed the buffer
        assert_eq!(read_result.unwrap(), 0);
        assert_eq!(&buf_again, &[0, 0, 0, 0])
    }

    #[test]
    fn must_fill_user_buffer_with_only_as_many_bytes_as_available() {
        let mem = TBufferChannel::with_capacity(4, 0);
        let mut t = TBufferedReadTransport::with_capacity(4, mem);

        t.chan.set_readable_bytes(&[0, 1, 2, 3]);

        // read buffer is much larger than the bytes available
        let mut buf = vec![0u8; 8];
        let read_result = t.read(&mut buf);

        // we've read exactly 4 bytes
        assert_eq!(read_result.unwrap(), 4);
        assert_eq!(&buf[..4], &[0, 1, 2, 3]);

        // try read again
        let read_result = t.read(&mut buf[4..]);

        // this time, 0 bytes and we haven't changed the buffer
        assert_eq!(read_result.unwrap(), 0);
        assert_eq!(&buf, &[0, 1, 2, 3, 0, 0, 0, 0])
    }

    #[test]
    fn must_read_successfully() {
        // this test involves a few loops within the buffered transport
        // itself where it has to drain the underlying transport in order
        // to service a read

        // we have a much smaller buffer than the
        // underlying transport has bytes available
        let mem = TBufferChannel::with_capacity(10, 0);
        let mut t = TBufferedReadTransport::with_capacity(2, mem);

        // fill the underlying transport's byte buffer
        let mut readable_bytes = [0u8; 10];
        for i in 0..10 {
            readable_bytes[i] = i as u8;
        }

        t.chan.set_readable_bytes(&readable_bytes);

        // we ask to read into a buffer that's much larger
        // than the one the buffered transport has; as a result
        // it's going to have to keep asking the underlying
        // transport for more bytes
        let mut buf = [0u8; 8];
        let read_result = t.read(&mut buf);

        // we should have read 8 bytes
        assert_eq!(read_result.unwrap(), 8);
        assert_eq!(&buf, &[0, 1, 2, 3, 4, 5, 6, 7]);

        // let's clear out the buffer and try read again
        for i in 0..8 {
            buf[i] = 0;
        }
        let read_result = t.read(&mut buf);

        // this time we were only able to read 2 bytes
        // (all that's remaining from the underlying transport)
        // let's also check that the remaining bytes are untouched
        assert_eq!(read_result.unwrap(), 2);
        assert_eq!(&buf[0..2], &[8, 9]);
        assert_eq!(&buf[2..], &[0, 0, 0, 0, 0, 0]);

        // try read again (we should get 0)
        // and all the existing bytes were untouched
        let read_result = t.read(&mut buf);
        assert_eq!(read_result.unwrap(), 0);
        assert_eq!(&buf[0..2], &[8, 9]);
        assert_eq!(&buf[2..], &[0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn must_return_zero_if_nothing_can_be_written() {
        let mem = TBufferChannel::with_capacity(0, 0);
        let mut t = TBufferedWriteTransport::with_capacity(0, mem);

        let b = vec![0; 10];
        let r = t.write(&b);

        assert_eq!(r.unwrap(), 0);
    }

    #[test]
    fn must_return_zero_if_caller_calls_write_with_empty_buffer() {
        let mem = TBufferChannel::with_capacity(0, 10);
        let mut t = TBufferedWriteTransport::with_capacity(10, mem);

        let r = t.write(&[]);
        let expected: [u8; 0] = [];

        assert_eq!(r.unwrap(), 0);
        assert_eq_transport_written_bytes!(t, expected);
    }

    #[test]
    fn must_return_zero_if_write_buffer_full() {
        let mem = TBufferChannel::with_capacity(0, 0);
        let mut t = TBufferedWriteTransport::with_capacity(4, mem);

        let b = [0x00, 0x01, 0x02, 0x03];

        // we've now filled the write buffer
        let r = t.write(&b);
        assert_eq!(r.unwrap(), 4);

        // try write the same bytes again - nothing should be writable
        let r = t.write(&b);
        assert_eq!(r.unwrap(), 0);
    }

    #[test]
    fn must_only_write_to_inner_transport_on_flush() {
        let mem = TBufferChannel::with_capacity(10, 10);
        let mut t = TBufferedWriteTransport::new(mem);

        let b: [u8; 5] = [0, 1, 2, 3, 4];
        assert_eq!(t.write(&b).unwrap(), 5);
        assert_eq_transport_num_written_bytes!(t, 0);

        assert!(t.flush().is_ok());

        assert_eq_transport_written_bytes!(t, b);
    }

    #[test]
    fn must_write_successfully_after_flush() {
        let mem = TBufferChannel::with_capacity(0, 5);
        let mut t = TBufferedWriteTransport::with_capacity(5, mem);

        // write and flush
        let b: [u8; 5] = [0, 1, 2, 3, 4];
        assert_eq!(t.write(&b).unwrap(), 5);
        assert!(t.flush().is_ok());

        // check the flushed bytes
        assert_eq_transport_written_bytes!(t, b);

        // reset our underlying transport
        t.channel.empty_write_buffer();

        // write and flush again
        assert_eq!(t.write(&b).unwrap(), 5);
        assert!(t.flush().is_ok());

        // check the flushed bytes
        assert_eq_transport_written_bytes!(t, b);
    }
}
