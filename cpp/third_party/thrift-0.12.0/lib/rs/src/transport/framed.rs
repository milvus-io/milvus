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
use std::io::{Read, Write};

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
    buf: Vec<u8>,
    pos: usize,
    cap: usize,
    chan: C,
}

impl<C> TFramedReadTransport<C>
where
    C: Read,
{
    /// Create a `TFramedReadTransport` with a default-sized
    /// internal read buffer that wraps the given `TIoChannel`.
    pub fn new(channel: C) -> TFramedReadTransport<C> {
        TFramedReadTransport::with_capacity(READ_CAPACITY, channel)
    }

    /// Create a `TFramedTransport` with an internal read buffer
    /// of size `read_capacity` that wraps the given `TIoChannel`.
    pub fn with_capacity(read_capacity: usize, channel: C) -> TFramedReadTransport<C> {
        TFramedReadTransport {
            buf: vec![0; read_capacity], // FIXME: do I actually have to do this?
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

            let buf_capacity = cmp::max(message_size, READ_CAPACITY);
            self.buf.resize(buf_capacity, 0);

            self.chan.read_exact(&mut self.buf[..message_size])?;
            self.cap = message_size as usize;
            self.pos = 0;
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
    buf: Vec<u8>,
    channel: C,
}

impl<C> TFramedWriteTransport<C>
where
    C: Write,
{
    /// Create a `TFramedWriteTransport` with default-sized internal
    /// write buffer that wraps the given `TIoChannel`.
    pub fn new(channel: C) -> TFramedWriteTransport<C> {
        TFramedWriteTransport::with_capacity(WRITE_CAPACITY, channel)
    }

    /// Create a `TFramedWriteTransport` with an internal write buffer
    /// of size `write_capacity` that wraps the given `TIoChannel`.
    pub fn with_capacity(write_capacity: usize, channel: C) -> TFramedWriteTransport<C> {
        TFramedWriteTransport {
            buf: Vec::with_capacity(write_capacity),
            channel,
        }
    }
}

impl<C> Write for TFramedWriteTransport<C>
where
    C: Write,
{
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        let current_capacity = self.buf.capacity();
        let available_space = current_capacity - self.buf.len();
        if b.len() > available_space {
            let additional_space = cmp::max(b.len() - available_space, current_capacity);
            self.buf.reserve(additional_space);
        }

        self.buf.extend_from_slice(b);
        Ok(b.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        let message_size = self.buf.len();

        if let 0 = message_size {
            return Ok(());
        } else {
            self.channel.write_i32::<BigEndian>(message_size as i32)?;
        }

        // will spin if the underlying channel can't be written to
        let mut byte_index = 0;
        while byte_index < message_size {
            let nwrite = self.channel.write(&self.buf[byte_index..message_size])?;
            byte_index = cmp::min(byte_index + nwrite, message_size);
        }

        let buf_capacity = cmp::min(self.buf.capacity(), WRITE_CAPACITY);
        self.buf.resize(buf_capacity, 0);
        self.buf.clear();

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
    use super::*;
    use transport::mem::TBufferChannel;

    // FIXME: test a forced reserve

    #[test]
    fn must_read_message_smaller_than_initial_buffer_size() {
        let c = TBufferChannel::with_capacity(10, 10);
        let mut t = TFramedReadTransport::with_capacity(8, c);

        t.chan.set_readable_bytes(&[
            0x00, 0x00, 0x00, 0x04, /* message size */
            0x00, 0x01, 0x02, 0x03, /* message body */
        ]);

        let mut buf = vec![0; 8];

        // we've read exactly 4 bytes
        assert_eq!(t.read(&mut buf).unwrap(), 4);
        assert_eq!(&buf[..4], &[0x00, 0x01, 0x02, 0x03]);
    }

    #[test]
    fn must_read_message_greater_than_initial_buffer_size() {
        let c = TBufferChannel::with_capacity(10, 10);
        let mut t = TFramedReadTransport::with_capacity(2, c);

        t.chan.set_readable_bytes(&[
            0x00, 0x00, 0x00, 0x04, /* message size */
            0x00, 0x01, 0x02, 0x03, /* message body */
        ]);

        let mut buf = vec![0; 8];

        // we've read exactly 4 bytes
        assert_eq!(t.read(&mut buf).unwrap(), 4);
        assert_eq!(&buf[..4], &[0x00, 0x01, 0x02, 0x03]);
    }

    #[test]
    fn must_read_multiple_messages_in_sequence_correctly() {
        let c = TBufferChannel::with_capacity(10, 10);
        let mut t = TFramedReadTransport::with_capacity(2, c);

        //
        // 1st message
        //

        t.chan.set_readable_bytes(&[
            0x00, 0x00, 0x00, 0x04, /* message size */
            0x00, 0x01, 0x02, 0x03, /* message body */
        ]);

        let mut buf = vec![0; 8];

        // we've read exactly 4 bytes
        assert_eq!(t.read(&mut buf).unwrap(), 4);
        assert_eq!(&buf, &[0x00, 0x01, 0x02, 0x03, 0x00, 0x00, 0x00, 0x00]);

        //
        // 2nd message
        //

        t.chan.set_readable_bytes(&[
            0x00, 0x00, 0x00, 0x01, /* message size */
            0x04, /* message body */
        ]);

        let mut buf = vec![0; 8];

        // we've read exactly 1 byte
        assert_eq!(t.read(&mut buf).unwrap(), 1);
        assert_eq!(&buf, &[0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn must_write_message_smaller_than_buffer_size() {
        let mem = TBufferChannel::with_capacity(0, 0);
        let mut t = TFramedWriteTransport::with_capacity(20, mem);

        let b = vec![0; 10];

        // should have written 10 bytes
        assert_eq!(t.write(&b).unwrap(), 10);
    }

    #[test]
    fn must_return_zero_if_caller_calls_write_with_empty_buffer() {
        let mem = TBufferChannel::with_capacity(0, 10);
        let mut t = TFramedWriteTransport::with_capacity(10, mem);

        let expected: [u8; 0] = [];

        assert_eq!(t.write(&[]).unwrap(), 0);
        assert_eq_transport_written_bytes!(t, expected);
    }

    #[test]
    fn must_write_to_inner_transport_on_flush() {
        let mem = TBufferChannel::with_capacity(10, 10);
        let mut t = TFramedWriteTransport::new(mem);

        let b: [u8; 5] = [0x00, 0x01, 0x02, 0x03, 0x04];
        assert_eq!(t.write(&b).unwrap(), 5);
        assert_eq_transport_num_written_bytes!(t, 0);

        assert!(t.flush().is_ok());

        let expected_bytes = [
            0x00, 0x00, 0x00, 0x05, /* message size */
            0x00, 0x01, 0x02, 0x03, 0x04, /* message body */
        ];

        assert_eq_transport_written_bytes!(t, expected_bytes);
    }

    #[test]
    fn must_write_message_greater_than_buffer_size_00() {
        let mem = TBufferChannel::with_capacity(0, 10);

        // IMPORTANT: DO **NOT** CHANGE THE WRITE_CAPACITY OR THE NUMBER OF BYTES TO BE WRITTEN!
        // these lengths were chosen to be just long enough
        // that doubling the capacity is a **worse** choice than
        // simply resizing the buffer to b.len()

        let mut t = TFramedWriteTransport::with_capacity(1, mem);
        let b = [0x00, 0x01, 0x02];

        // should have written 3 bytes
        assert_eq!(t.write(&b).unwrap(), 3);
        assert_eq_transport_num_written_bytes!(t, 0);

        assert!(t.flush().is_ok());

        let expected_bytes = [
            0x00, 0x00, 0x00, 0x03, /* message size */
            0x00, 0x01, 0x02, /* message body */
        ];

        assert_eq_transport_written_bytes!(t, expected_bytes);
    }

    #[test]
    fn must_write_message_greater_than_buffer_size_01() {
        let mem = TBufferChannel::with_capacity(0, 10);

        // IMPORTANT: DO **NOT** CHANGE THE WRITE_CAPACITY OR THE NUMBER OF BYTES TO BE WRITTEN!
        // these lengths were chosen to be just long enough
        // that doubling the capacity is a **better** choice than
        // simply resizing the buffer to b.len()

        let mut t = TFramedWriteTransport::with_capacity(2, mem);
        let b = [0x00, 0x01, 0x02];

        // should have written 3 bytes
        assert_eq!(t.write(&b).unwrap(), 3);
        assert_eq_transport_num_written_bytes!(t, 0);

        assert!(t.flush().is_ok());

        let expected_bytes = [
            0x00, 0x00, 0x00, 0x03, /* message size */
            0x00, 0x01, 0x02, /* message body */
        ];

        assert_eq_transport_written_bytes!(t, expected_bytes);
    }

    #[test]
    fn must_return_error_if_nothing_can_be_written_to_inner_transport_on_flush() {
        let mem = TBufferChannel::with_capacity(0, 0);
        let mut t = TFramedWriteTransport::with_capacity(1, mem);

        let b = vec![0; 10];

        // should have written 10 bytes
        assert_eq!(t.write(&b).unwrap(), 10);

        // let's flush
        let r = t.flush();

        // this time we'll error out because the flush can't write to the underlying channel
        assert!(r.is_err());
    }

    #[test]
    fn must_write_successfully_after_flush() {
        // IMPORTANT: write capacity *MUST* be greater
        // than message sizes used in this test + 4-byte frame header
        let mem = TBufferChannel::with_capacity(0, 10);
        let mut t = TFramedWriteTransport::with_capacity(5, mem);

        // write and flush
        let first_message: [u8; 5] = [0x00, 0x01, 0x02, 0x03, 0x04];
        assert_eq!(t.write(&first_message).unwrap(), 5);
        assert!(t.flush().is_ok());

        let mut expected = Vec::new();
        expected.write_all(&[0x00, 0x00, 0x00, 0x05]).unwrap(); // message size
        expected.extend_from_slice(&first_message);

        // check the flushed bytes
        assert_eq!(t.channel.write_bytes(), expected);

        // reset our underlying transport
        t.channel.empty_write_buffer();

        let second_message: [u8; 3] = [0x05, 0x06, 0x07];
        assert_eq!(t.write(&second_message).unwrap(), 3);
        assert!(t.flush().is_ok());

        expected.clear();
        expected.write_all(&[0x00, 0x00, 0x00, 0x03]).unwrap(); // message size
        expected.extend_from_slice(&second_message);

        // check the flushed bytes
        assert_eq!(t.channel.write_bytes(), expected);
    }
}
