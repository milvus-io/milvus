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
use integer_encoding::{VarIntReader, VarIntWriter};
use std::convert::From;
use try_from::TryFrom;

use transport::{TReadTransport, TWriteTransport};
use super::{TFieldIdentifier, TInputProtocol, TInputProtocolFactory, TListIdentifier,
            TMapIdentifier, TMessageIdentifier, TMessageType};
use super::{TOutputProtocol, TOutputProtocolFactory, TSetIdentifier, TStructIdentifier, TType};

const COMPACT_PROTOCOL_ID: u8 = 0x82;
const COMPACT_VERSION: u8 = 0x01;
const COMPACT_VERSION_MASK: u8 = 0x1F;

/// Read messages encoded in the Thrift compact protocol.
///
/// # Examples
///
/// Create and use a `TCompactInputProtocol`.
///
/// ```no_run
/// use thrift::protocol::{TCompactInputProtocol, TInputProtocol};
/// use thrift::transport::TTcpChannel;
///
/// let mut channel = TTcpChannel::new();
/// channel.open("localhost:9090").unwrap();
///
/// let mut protocol = TCompactInputProtocol::new(channel);
///
/// let recvd_bool = protocol.read_bool().unwrap();
/// let recvd_string = protocol.read_string().unwrap();
/// ```
#[derive(Debug)]
pub struct TCompactInputProtocol<T>
where
    T: TReadTransport,
{
    // Identifier of the last field deserialized for a struct.
    last_read_field_id: i16,
    // Stack of the last read field ids (a new entry is added each time a nested struct is read).
    read_field_id_stack: Vec<i16>,
    // Boolean value for a field.
    // Saved because boolean fields and their value are encoded in a single byte,
    // and reading the field only occurs after the field id is read.
    pending_read_bool_value: Option<bool>,
    // Underlying transport used for byte-level operations.
    transport: T,
}

impl<T> TCompactInputProtocol<T>
where
    T: TReadTransport,
{
    /// Create a `TCompactInputProtocol` that reads bytes from `transport`.
    pub fn new(transport: T) -> TCompactInputProtocol<T> {
        TCompactInputProtocol {
            last_read_field_id: 0,
            read_field_id_stack: Vec::new(),
            pending_read_bool_value: None,
            transport: transport,
        }
    }

    fn read_list_set_begin(&mut self) -> ::Result<(TType, i32)> {
        let header = self.read_byte()?;
        let element_type = collection_u8_to_type(header & 0x0F)?;

        let element_count;
        let possible_element_count = (header & 0xF0) >> 4;
        if possible_element_count != 15 {
            // high bits set high if count and type encoded separately
            element_count = possible_element_count as i32;
        } else {
            element_count = self.transport.read_varint::<u32>()? as i32;
        }

        Ok((element_type, element_count))
    }
}

impl<T> TInputProtocol for TCompactInputProtocol<T>
where
    T: TReadTransport,
{
    fn read_message_begin(&mut self) -> ::Result<TMessageIdentifier> {
        let compact_id = self.read_byte()?;
        if compact_id != COMPACT_PROTOCOL_ID {
            Err(
                ::Error::Protocol(
                    ::ProtocolError {
                        kind: ::ProtocolErrorKind::BadVersion,
                        message: format!("invalid compact protocol header {:?}", compact_id),
                    },
                ),
            )
        } else {
            Ok(())
        }?;

        let type_and_byte = self.read_byte()?;
        let received_version = type_and_byte & COMPACT_VERSION_MASK;
        if received_version != COMPACT_VERSION {
            Err(
                ::Error::Protocol(
                    ::ProtocolError {
                        kind: ::ProtocolErrorKind::BadVersion,
                        message: format!(
                            "cannot process compact protocol version {:?}",
                            received_version
                        ),
                    },
                ),
            )
        } else {
            Ok(())
        }?;

        // NOTE: unsigned right shift will pad with 0s
        let message_type: TMessageType = TMessageType::try_from(type_and_byte >> 5)?;
        let sequence_number = self.read_i32()?;
        let service_call_name = self.read_string()?;

        self.last_read_field_id = 0;

        Ok(TMessageIdentifier::new(service_call_name, message_type, sequence_number),)
    }

    fn read_message_end(&mut self) -> ::Result<()> {
        Ok(())
    }

    fn read_struct_begin(&mut self) -> ::Result<Option<TStructIdentifier>> {
        self.read_field_id_stack.push(self.last_read_field_id);
        self.last_read_field_id = 0;
        Ok(None)
    }

    fn read_struct_end(&mut self) -> ::Result<()> {
        self.last_read_field_id = self.read_field_id_stack
            .pop()
            .expect("should have previous field ids");
        Ok(())
    }

    fn read_field_begin(&mut self) -> ::Result<TFieldIdentifier> {
        // we can read at least one byte, which is:
        // - the type
        // - the field delta and the type
        let field_type = self.read_byte()?;
        let field_delta = (field_type & 0xF0) >> 4;
        let field_type = match field_type & 0x0F {
            0x01 => {
                self.pending_read_bool_value = Some(true);
                Ok(TType::Bool)
            }
            0x02 => {
                self.pending_read_bool_value = Some(false);
                Ok(TType::Bool)
            }
            ttu8 => u8_to_type(ttu8),
        }?;

        match field_type {
            TType::Stop => {
                Ok(
                    TFieldIdentifier::new::<Option<String>, String, Option<i16>>(
                        None,
                        TType::Stop,
                        None,
                    ),
                )
            }
            _ => {
                if field_delta != 0 {
                    self.last_read_field_id += field_delta as i16;
                } else {
                    self.last_read_field_id = self.read_i16()?;
                };

                Ok(
                    TFieldIdentifier {
                        name: None,
                        field_type: field_type,
                        id: Some(self.last_read_field_id),
                    },
                )
            }
        }
    }

    fn read_field_end(&mut self) -> ::Result<()> {
        Ok(())
    }

    fn read_bool(&mut self) -> ::Result<bool> {
        match self.pending_read_bool_value.take() {
            Some(b) => Ok(b),
            None => {
                let b = self.read_byte()?;
                match b {
                    0x01 => Ok(true),
                    0x02 => Ok(false),
                    unkn => {
                        Err(
                            ::Error::Protocol(
                                ::ProtocolError {
                                    kind: ::ProtocolErrorKind::InvalidData,
                                    message: format!("cannot convert {} into bool", unkn),
                                },
                            ),
                        )
                    }
                }
            }
        }
    }

    fn read_bytes(&mut self) -> ::Result<Vec<u8>> {
        let len = self.transport.read_varint::<u32>()?;
        let mut buf = vec![0u8; len as usize];
        self.transport
            .read_exact(&mut buf)
            .map_err(From::from)
            .map(|_| buf)
    }

    fn read_i8(&mut self) -> ::Result<i8> {
        self.read_byte().map(|i| i as i8)
    }

    fn read_i16(&mut self) -> ::Result<i16> {
        self.transport.read_varint::<i16>().map_err(From::from)
    }

    fn read_i32(&mut self) -> ::Result<i32> {
        self.transport.read_varint::<i32>().map_err(From::from)
    }

    fn read_i64(&mut self) -> ::Result<i64> {
        self.transport.read_varint::<i64>().map_err(From::from)
    }

    fn read_double(&mut self) -> ::Result<f64> {
        self.transport
            .read_f64::<BigEndian>()
            .map_err(From::from)
    }

    fn read_string(&mut self) -> ::Result<String> {
        let bytes = self.read_bytes()?;
        String::from_utf8(bytes).map_err(From::from)
    }

    fn read_list_begin(&mut self) -> ::Result<TListIdentifier> {
        let (element_type, element_count) = self.read_list_set_begin()?;
        Ok(TListIdentifier::new(element_type, element_count))
    }

    fn read_list_end(&mut self) -> ::Result<()> {
        Ok(())
    }

    fn read_set_begin(&mut self) -> ::Result<TSetIdentifier> {
        let (element_type, element_count) = self.read_list_set_begin()?;
        Ok(TSetIdentifier::new(element_type, element_count))
    }

    fn read_set_end(&mut self) -> ::Result<()> {
        Ok(())
    }

    fn read_map_begin(&mut self) -> ::Result<TMapIdentifier> {
        let element_count = self.transport.read_varint::<u32>()? as i32;
        if element_count == 0 {
            Ok(TMapIdentifier::new(None, None, 0))
        } else {
            let type_header = self.read_byte()?;
            let key_type = collection_u8_to_type((type_header & 0xF0) >> 4)?;
            let val_type = collection_u8_to_type(type_header & 0x0F)?;
            Ok(TMapIdentifier::new(key_type, val_type, element_count))
        }
    }

    fn read_map_end(&mut self) -> ::Result<()> {
        Ok(())
    }

    // utility
    //

    fn read_byte(&mut self) -> ::Result<u8> {
        let mut buf = [0u8; 1];
        self.transport
            .read_exact(&mut buf)
            .map_err(From::from)
            .map(|_| buf[0])
    }
}

/// Factory for creating instances of `TCompactInputProtocol`.
#[derive(Default)]
pub struct TCompactInputProtocolFactory;

impl TCompactInputProtocolFactory {
    /// Create a `TCompactInputProtocolFactory`.
    pub fn new() -> TCompactInputProtocolFactory {
        TCompactInputProtocolFactory {}
    }
}

impl TInputProtocolFactory for TCompactInputProtocolFactory {
    fn create(&self, transport: Box<TReadTransport + Send>) -> Box<TInputProtocol + Send> {
        Box::new(TCompactInputProtocol::new(transport))
    }
}

/// Write messages using the Thrift compact protocol.
///
/// # Examples
///
/// Create and use a `TCompactOutputProtocol`.
///
/// ```no_run
/// use thrift::protocol::{TCompactOutputProtocol, TOutputProtocol};
/// use thrift::transport::TTcpChannel;
///
/// let mut channel = TTcpChannel::new();
/// channel.open("localhost:9090").unwrap();
///
/// let mut protocol = TCompactOutputProtocol::new(channel);
///
/// protocol.write_bool(true).unwrap();
/// protocol.write_string("test_string").unwrap();
/// ```
#[derive(Debug)]
pub struct TCompactOutputProtocol<T>
where
    T: TWriteTransport,
{
    // Identifier of the last field serialized for a struct.
    last_write_field_id: i16,
    // Stack of the last written field ids (new entry added each time a nested struct is written).
    write_field_id_stack: Vec<i16>,
    // Field identifier of the boolean field to be written.
    // Saved because boolean fields and their value are encoded in a single byte
    pending_write_bool_field_identifier: Option<TFieldIdentifier>,
    // Underlying transport used for byte-level operations.
    transport: T,
}

impl<T> TCompactOutputProtocol<T>
where
    T: TWriteTransport,
{
    /// Create a `TCompactOutputProtocol` that writes bytes to `transport`.
    pub fn new(transport: T) -> TCompactOutputProtocol<T> {
        TCompactOutputProtocol {
            last_write_field_id: 0,
            write_field_id_stack: Vec::new(),
            pending_write_bool_field_identifier: None,
            transport: transport,
        }
    }

    // FIXME: field_type as unconstrained u8 is bad
    fn write_field_header(&mut self, field_type: u8, field_id: i16) -> ::Result<()> {
        let field_delta = field_id - self.last_write_field_id;
        if field_delta > 0 && field_delta < 15 {
            self.write_byte(((field_delta as u8) << 4) | field_type)?;
        } else {
            self.write_byte(field_type)?;
            self.write_i16(field_id)?;
        }
        self.last_write_field_id = field_id;
        Ok(())
    }

    fn write_list_set_begin(&mut self, element_type: TType, element_count: i32) -> ::Result<()> {
        let elem_identifier = collection_type_to_u8(element_type);
        if element_count <= 14 {
            let header = (element_count as u8) << 4 | elem_identifier;
            self.write_byte(header)
        } else {
            let header = 0xF0 | elem_identifier;
            self.write_byte(header)?;
            self.transport
                .write_varint(element_count as u32)
                .map_err(From::from)
                .map(|_| ())
        }
    }

    fn assert_no_pending_bool_write(&self) {
        if let Some(ref f) = self.pending_write_bool_field_identifier {
            panic!("pending bool field {:?} not written", f)
        }
    }
}

impl<T> TOutputProtocol for TCompactOutputProtocol<T>
where
    T: TWriteTransport,
{
    fn write_message_begin(&mut self, identifier: &TMessageIdentifier) -> ::Result<()> {
        self.write_byte(COMPACT_PROTOCOL_ID)?;
        self.write_byte((u8::from(identifier.message_type) << 5) | COMPACT_VERSION)?;
        self.write_i32(identifier.sequence_number)?;
        self.write_string(&identifier.name)?;
        Ok(())
    }

    fn write_message_end(&mut self) -> ::Result<()> {
        self.assert_no_pending_bool_write();
        Ok(())
    }

    fn write_struct_begin(&mut self, _: &TStructIdentifier) -> ::Result<()> {
        self.write_field_id_stack.push(self.last_write_field_id);
        self.last_write_field_id = 0;
        Ok(())
    }

    fn write_struct_end(&mut self) -> ::Result<()> {
        self.assert_no_pending_bool_write();
        self.last_write_field_id = self.write_field_id_stack
            .pop()
            .expect("should have previous field ids");
        Ok(())
    }

    fn write_field_begin(&mut self, identifier: &TFieldIdentifier) -> ::Result<()> {
        match identifier.field_type {
            TType::Bool => {
                if self.pending_write_bool_field_identifier.is_some() {
                    panic!(
                        "should not have a pending bool while writing another bool with id: \
                            {:?}",
                        identifier
                    )
                }
                self.pending_write_bool_field_identifier = Some(identifier.clone());
                Ok(())
            }
            _ => {
                let field_type = type_to_u8(identifier.field_type);
                let field_id = identifier
                    .id
                    .expect("non-stop field should have field id");
                self.write_field_header(field_type, field_id)
            }
        }
    }

    fn write_field_end(&mut self) -> ::Result<()> {
        self.assert_no_pending_bool_write();
        Ok(())
    }

    fn write_field_stop(&mut self) -> ::Result<()> {
        self.assert_no_pending_bool_write();
        self.write_byte(type_to_u8(TType::Stop))
    }

    fn write_bool(&mut self, b: bool) -> ::Result<()> {
        match self.pending_write_bool_field_identifier.take() {
            Some(pending) => {
                let field_id = pending.id.expect("bool field should have a field id");
                let field_type_as_u8 = if b { 0x01 } else { 0x02 };
                self.write_field_header(field_type_as_u8, field_id)
            }
            None => {
                if b {
                    self.write_byte(0x01)
                } else {
                    self.write_byte(0x02)
                }
            }
        }
    }

    fn write_bytes(&mut self, b: &[u8]) -> ::Result<()> {
        self.transport.write_varint(b.len() as u32)?;
        self.transport.write_all(b).map_err(From::from)
    }

    fn write_i8(&mut self, i: i8) -> ::Result<()> {
        self.write_byte(i as u8)
    }

    fn write_i16(&mut self, i: i16) -> ::Result<()> {
        self.transport
            .write_varint(i)
            .map_err(From::from)
            .map(|_| ())
    }

    fn write_i32(&mut self, i: i32) -> ::Result<()> {
        self.transport
            .write_varint(i)
            .map_err(From::from)
            .map(|_| ())
    }

    fn write_i64(&mut self, i: i64) -> ::Result<()> {
        self.transport
            .write_varint(i)
            .map_err(From::from)
            .map(|_| ())
    }

    fn write_double(&mut self, d: f64) -> ::Result<()> {
        self.transport
            .write_f64::<BigEndian>(d)
            .map_err(From::from)
    }

    fn write_string(&mut self, s: &str) -> ::Result<()> {
        self.write_bytes(s.as_bytes())
    }

    fn write_list_begin(&mut self, identifier: &TListIdentifier) -> ::Result<()> {
        self.write_list_set_begin(identifier.element_type, identifier.size)
    }

    fn write_list_end(&mut self) -> ::Result<()> {
        Ok(())
    }

    fn write_set_begin(&mut self, identifier: &TSetIdentifier) -> ::Result<()> {
        self.write_list_set_begin(identifier.element_type, identifier.size)
    }

    fn write_set_end(&mut self) -> ::Result<()> {
        Ok(())
    }

    fn write_map_begin(&mut self, identifier: &TMapIdentifier) -> ::Result<()> {
        if identifier.size == 0 {
            self.write_byte(0)
        } else {
            self.transport.write_varint(identifier.size as u32)?;

            let key_type = identifier
                .key_type
                .expect("map identifier to write should contain key type");
            let key_type_byte = collection_type_to_u8(key_type) << 4;

            let val_type = identifier
                .value_type
                .expect("map identifier to write should contain value type");
            let val_type_byte = collection_type_to_u8(val_type);

            let map_type_header = key_type_byte | val_type_byte;
            self.write_byte(map_type_header)
        }
    }

    fn write_map_end(&mut self) -> ::Result<()> {
        Ok(())
    }

    fn flush(&mut self) -> ::Result<()> {
        self.transport.flush().map_err(From::from)
    }

    // utility
    //

    fn write_byte(&mut self, b: u8) -> ::Result<()> {
        self.transport
            .write(&[b])
            .map_err(From::from)
            .map(|_| ())
    }
}

/// Factory for creating instances of `TCompactOutputProtocol`.
#[derive(Default)]
pub struct TCompactOutputProtocolFactory;

impl TCompactOutputProtocolFactory {
    /// Create a `TCompactOutputProtocolFactory`.
    pub fn new() -> TCompactOutputProtocolFactory {
        TCompactOutputProtocolFactory {}
    }
}

impl TOutputProtocolFactory for TCompactOutputProtocolFactory {
    fn create(&self, transport: Box<TWriteTransport + Send>) -> Box<TOutputProtocol + Send> {
        Box::new(TCompactOutputProtocol::new(transport))
    }
}

fn collection_type_to_u8(field_type: TType) -> u8 {
    match field_type {
        TType::Bool => 0x01,
        f => type_to_u8(f),
    }
}

fn type_to_u8(field_type: TType) -> u8 {
    match field_type {
        TType::Stop => 0x00,
        TType::I08 => 0x03, // equivalent to TType::Byte
        TType::I16 => 0x04,
        TType::I32 => 0x05,
        TType::I64 => 0x06,
        TType::Double => 0x07,
        TType::String => 0x08,
        TType::List => 0x09,
        TType::Set => 0x0A,
        TType::Map => 0x0B,
        TType::Struct => 0x0C,
        _ => panic!(format!("should not have attempted to convert {} to u8", field_type)),
    }
}

fn collection_u8_to_type(b: u8) -> ::Result<TType> {
    match b {
        0x01 => Ok(TType::Bool),
        o => u8_to_type(o),
    }
}

fn u8_to_type(b: u8) -> ::Result<TType> {
    match b {
        0x00 => Ok(TType::Stop),
        0x03 => Ok(TType::I08), // equivalent to TType::Byte
        0x04 => Ok(TType::I16),
        0x05 => Ok(TType::I32),
        0x06 => Ok(TType::I64),
        0x07 => Ok(TType::Double),
        0x08 => Ok(TType::String),
        0x09 => Ok(TType::List),
        0x0A => Ok(TType::Set),
        0x0B => Ok(TType::Map),
        0x0C => Ok(TType::Struct),
        unkn => {
            Err(
                ::Error::Protocol(
                    ::ProtocolError {
                        kind: ::ProtocolErrorKind::InvalidData,
                        message: format!("cannot convert {} into TType", unkn),
                    },
                ),
            )
        }
    }
}

#[cfg(test)]
mod tests {

    use protocol::{TFieldIdentifier, TInputProtocol, TListIdentifier, TMapIdentifier,
                   TMessageIdentifier, TMessageType, TOutputProtocol, TSetIdentifier,
                   TStructIdentifier, TType};
    use transport::{ReadHalf, TBufferChannel, TIoChannel, WriteHalf};

    use super::*;

    #[test]
    fn must_write_message_begin_0() {
        let (_, mut o_prot) = test_objects();

        assert_success!(o_prot.write_message_begin(&TMessageIdentifier::new("foo", TMessageType::Call, 431)));

        let expected: [u8; 8] = [
            0x82, /* protocol ID */
            0x21, /* message type | protocol version */
            0xDE,
            0x06, /* zig-zag varint sequence number */
            0x03, /* message-name length */
            0x66,
            0x6F,
            0x6F /* "foo" */,
        ];

        assert_eq_written_bytes!(o_prot, expected);
    }

    #[test]
    fn must_write_message_begin_1() {
        let (_, mut o_prot) = test_objects();

        assert_success!(
            o_prot.write_message_begin(&TMessageIdentifier::new("bar", TMessageType::Reply, 991828))
        );

        let expected: [u8; 9] = [
            0x82, /* protocol ID */
            0x41, /* message type | protocol version */
            0xA8,
            0x89,
            0x79, /* zig-zag varint sequence number */
            0x03, /* message-name length */
            0x62,
            0x61,
            0x72 /* "bar" */,
        ];

        assert_eq_written_bytes!(o_prot, expected);
    }

    #[test]
    fn must_round_trip_message_begin() {
        let (mut i_prot, mut o_prot) = test_objects();

        let ident = TMessageIdentifier::new("service_call", TMessageType::Call, 1283948);

        assert_success!(o_prot.write_message_begin(&ident));

        copy_write_buffer_to_read_buffer!(o_prot);

        let res = assert_success!(i_prot.read_message_begin());
        assert_eq!(&res, &ident);
    }

    #[test]
    fn must_write_message_end() {
        assert_no_write(|o| o.write_message_end());
    }

    // NOTE: structs and fields are tested together
    //

    #[test]
    fn must_write_struct_with_delta_fields() {
        let (_, mut o_prot) = test_objects();

        // no bytes should be written however
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // write three fields with tiny field ids
        // since they're small the field ids will be encoded as deltas

        // since this is the first field (and it's zero) it gets the full varint write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::I08, 0)));
        assert_success!(o_prot.write_field_end());

        // since this delta > 0 and < 15 it can be encoded as a delta
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::I16, 4)));
        assert_success!(o_prot.write_field_end());

        // since this delta > 0 and < 15 it can be encoded as a delta
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::List, 9)));
        assert_success!(o_prot.write_field_end());

        // now, finish the struct off
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        let expected: [u8; 5] = [
            0x03, /* field type */
            0x00, /* first field id */
            0x44, /* field delta (4) | field type */
            0x59, /* field delta (5) | field type */
            0x00 /* field stop */,
        ];

        assert_eq_written_bytes!(o_prot, expected);
    }

    #[test]
    fn must_round_trip_struct_with_delta_fields() {
        let (mut i_prot, mut o_prot) = test_objects();

        // no bytes should be written however
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // write three fields with tiny field ids
        // since they're small the field ids will be encoded as deltas

        // since this is the first field (and it's zero) it gets the full varint write
        let field_ident_1 = TFieldIdentifier::new("foo", TType::I08, 0);
        assert_success!(o_prot.write_field_begin(&field_ident_1));
        assert_success!(o_prot.write_field_end());

        // since this delta > 0 and < 15 it can be encoded as a delta
        let field_ident_2 = TFieldIdentifier::new("foo", TType::I16, 4);
        assert_success!(o_prot.write_field_begin(&field_ident_2));
        assert_success!(o_prot.write_field_end());

        // since this delta > 0 and < 15 it can be encoded as a delta
        let field_ident_3 = TFieldIdentifier::new("foo", TType::List, 9);
        assert_success!(o_prot.write_field_begin(&field_ident_3));
        assert_success!(o_prot.write_field_end());

        // now, finish the struct off
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        copy_write_buffer_to_read_buffer!(o_prot);

        // read the struct back
        assert_success!(i_prot.read_struct_begin());

        let read_ident_1 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_1,
            TFieldIdentifier {
                name: None,
                ..field_ident_1
            }
        );
        assert_success!(i_prot.read_field_end());

        let read_ident_2 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_2,
            TFieldIdentifier {
                name: None,
                ..field_ident_2
            }
        );
        assert_success!(i_prot.read_field_end());

        let read_ident_3 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_3,
            TFieldIdentifier {
                name: None,
                ..field_ident_3
            }
        );
        assert_success!(i_prot.read_field_end());

        let read_ident_4 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_4,
            TFieldIdentifier {
                name: None,
                field_type: TType::Stop,
                id: None,
            }
        );

        assert_success!(i_prot.read_struct_end());
    }

    #[test]
    fn must_write_struct_with_non_zero_initial_field_and_delta_fields() {
        let (_, mut o_prot) = test_objects();

        // no bytes should be written however
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // write three fields with tiny field ids
        // since they're small the field ids will be encoded as deltas

        // gets a delta write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::I32, 1)));
        assert_success!(o_prot.write_field_end());

        // since this delta > 0 and < 15 it can be encoded as a delta
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::Set, 2)));
        assert_success!(o_prot.write_field_end());

        // since this delta > 0 and < 15 it can be encoded as a delta
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::String, 6)));
        assert_success!(o_prot.write_field_end());

        // now, finish the struct off
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        let expected: [u8; 4] = [
            0x15, /* field delta (1) | field type */
            0x1A, /* field delta (1) | field type */
            0x48, /* field delta (4) | field type */
            0x00 /* field stop */,
        ];

        assert_eq_written_bytes!(o_prot, expected);
    }

    #[test]
    fn must_round_trip_struct_with_non_zero_initial_field_and_delta_fields() {
        let (mut i_prot, mut o_prot) = test_objects();

        // no bytes should be written however
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // write three fields with tiny field ids
        // since they're small the field ids will be encoded as deltas

        // gets a delta write
        let field_ident_1 = TFieldIdentifier::new("foo", TType::I32, 1);
        assert_success!(o_prot.write_field_begin(&field_ident_1));
        assert_success!(o_prot.write_field_end());

        // since this delta > 0 and < 15 it can be encoded as a delta
        let field_ident_2 = TFieldIdentifier::new("foo", TType::Set, 2);
        assert_success!(o_prot.write_field_begin(&field_ident_2));
        assert_success!(o_prot.write_field_end());

        // since this delta > 0 and < 15 it can be encoded as a delta
        let field_ident_3 = TFieldIdentifier::new("foo", TType::String, 6);
        assert_success!(o_prot.write_field_begin(&field_ident_3));
        assert_success!(o_prot.write_field_end());

        // now, finish the struct off
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        copy_write_buffer_to_read_buffer!(o_prot);

        // read the struct back
        assert_success!(i_prot.read_struct_begin());

        let read_ident_1 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_1,
            TFieldIdentifier {
                name: None,
                ..field_ident_1
            }
        );
        assert_success!(i_prot.read_field_end());

        let read_ident_2 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_2,
            TFieldIdentifier {
                name: None,
                ..field_ident_2
            }
        );
        assert_success!(i_prot.read_field_end());

        let read_ident_3 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_3,
            TFieldIdentifier {
                name: None,
                ..field_ident_3
            }
        );
        assert_success!(i_prot.read_field_end());

        let read_ident_4 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_4,
            TFieldIdentifier {
                name: None,
                field_type: TType::Stop,
                id: None,
            }
        );

        assert_success!(i_prot.read_struct_end());
    }

    #[test]
    fn must_write_struct_with_long_fields() {
        let (_, mut o_prot) = test_objects();

        // no bytes should be written however
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // write three fields with field ids that cannot be encoded as deltas

        // since this is the first field (and it's zero) it gets the full varint write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::I32, 0)));
        assert_success!(o_prot.write_field_end());

        // since this delta is > 15 it is encoded as a zig-zag varint
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::I64, 16)));
        assert_success!(o_prot.write_field_end());

        // since this delta is > 15 it is encoded as a zig-zag varint
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::Set, 99)));
        assert_success!(o_prot.write_field_end());

        // now, finish the struct off
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        let expected: [u8; 8] = [
            0x05, /* field type */
            0x00, /* first field id */
            0x06, /* field type */
            0x20, /* zig-zag varint field id */
            0x0A, /* field type */
            0xC6,
            0x01, /* zig-zag varint field id */
            0x00 /* field stop */,
        ];

        assert_eq_written_bytes!(o_prot, expected);
    }

    #[test]
    fn must_round_trip_struct_with_long_fields() {
        let (mut i_prot, mut o_prot) = test_objects();

        // no bytes should be written however
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // write three fields with field ids that cannot be encoded as deltas

        // since this is the first field (and it's zero) it gets the full varint write
        let field_ident_1 = TFieldIdentifier::new("foo", TType::I32, 0);
        assert_success!(o_prot.write_field_begin(&field_ident_1));
        assert_success!(o_prot.write_field_end());

        // since this delta is > 15 it is encoded as a zig-zag varint
        let field_ident_2 = TFieldIdentifier::new("foo", TType::I64, 16);
        assert_success!(o_prot.write_field_begin(&field_ident_2));
        assert_success!(o_prot.write_field_end());

        // since this delta is > 15 it is encoded as a zig-zag varint
        let field_ident_3 = TFieldIdentifier::new("foo", TType::Set, 99);
        assert_success!(o_prot.write_field_begin(&field_ident_3));
        assert_success!(o_prot.write_field_end());

        // now, finish the struct off
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        copy_write_buffer_to_read_buffer!(o_prot);

        // read the struct back
        assert_success!(i_prot.read_struct_begin());

        let read_ident_1 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_1,
            TFieldIdentifier {
                name: None,
                ..field_ident_1
            }
        );
        assert_success!(i_prot.read_field_end());

        let read_ident_2 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_2,
            TFieldIdentifier {
                name: None,
                ..field_ident_2
            }
        );
        assert_success!(i_prot.read_field_end());

        let read_ident_3 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_3,
            TFieldIdentifier {
                name: None,
                ..field_ident_3
            }
        );
        assert_success!(i_prot.read_field_end());

        let read_ident_4 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_4,
            TFieldIdentifier {
                name: None,
                field_type: TType::Stop,
                id: None,
            }
        );

        assert_success!(i_prot.read_struct_end());
    }

    #[test]
    fn must_write_struct_with_mix_of_long_and_delta_fields() {
        let (_, mut o_prot) = test_objects();

        // no bytes should be written however
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // write three fields with field ids that cannot be encoded as deltas

        // since the delta is > 0 and < 15 it gets a delta write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::I64, 1)));
        assert_success!(o_prot.write_field_end());

        // since this delta > 0 and < 15 it gets a delta write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::I32, 9)));
        assert_success!(o_prot.write_field_end());

        // since this delta is > 15 it is encoded as a zig-zag varint
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::Set, 1000)));
        assert_success!(o_prot.write_field_end());

        // since this delta is > 15 it is encoded as a zig-zag varint
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::Set, 2001)));
        assert_success!(o_prot.write_field_end());

        // since this is only 3 up from the previous it is recorded as a delta
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::Set, 2004)));
        assert_success!(o_prot.write_field_end());

        // now, finish the struct off
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        let expected: [u8; 10] = [
            0x16, /* field delta (1) | field type */
            0x85, /* field delta (8) | field type */
            0x0A, /* field type */
            0xD0,
            0x0F, /* zig-zag varint field id */
            0x0A, /* field type */
            0xA2,
            0x1F, /* zig-zag varint field id */
            0x3A, /* field delta (3) | field type */
            0x00 /* field stop */,
        ];

        assert_eq_written_bytes!(o_prot, expected);
    }

    #[test]
    fn must_round_trip_struct_with_mix_of_long_and_delta_fields() {
        let (mut i_prot, mut o_prot) = test_objects();

        // no bytes should be written however
        let struct_ident = TStructIdentifier::new("foo");
        assert_success!(o_prot.write_struct_begin(&struct_ident));

        // write three fields with field ids that cannot be encoded as deltas

        // since the delta is > 0 and < 15 it gets a delta write
        let field_ident_1 = TFieldIdentifier::new("foo", TType::I64, 1);
        assert_success!(o_prot.write_field_begin(&field_ident_1));
        assert_success!(o_prot.write_field_end());

        // since this delta > 0 and < 15 it gets a delta write
        let field_ident_2 = TFieldIdentifier::new("foo", TType::I32, 9);
        assert_success!(o_prot.write_field_begin(&field_ident_2));
        assert_success!(o_prot.write_field_end());

        // since this delta is > 15 it is encoded as a zig-zag varint
        let field_ident_3 = TFieldIdentifier::new("foo", TType::Set, 1000);
        assert_success!(o_prot.write_field_begin(&field_ident_3));
        assert_success!(o_prot.write_field_end());

        // since this delta is > 15 it is encoded as a zig-zag varint
        let field_ident_4 = TFieldIdentifier::new("foo", TType::Set, 2001);
        assert_success!(o_prot.write_field_begin(&field_ident_4));
        assert_success!(o_prot.write_field_end());

        // since this is only 3 up from the previous it is recorded as a delta
        let field_ident_5 = TFieldIdentifier::new("foo", TType::Set, 2004);
        assert_success!(o_prot.write_field_begin(&field_ident_5));
        assert_success!(o_prot.write_field_end());

        // now, finish the struct off
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        copy_write_buffer_to_read_buffer!(o_prot);

        // read the struct back
        assert_success!(i_prot.read_struct_begin());

        let read_ident_1 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_1,
            TFieldIdentifier {
                name: None,
                ..field_ident_1
            }
        );
        assert_success!(i_prot.read_field_end());

        let read_ident_2 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_2,
            TFieldIdentifier {
                name: None,
                ..field_ident_2
            }
        );
        assert_success!(i_prot.read_field_end());

        let read_ident_3 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_3,
            TFieldIdentifier {
                name: None,
                ..field_ident_3
            }
        );
        assert_success!(i_prot.read_field_end());

        let read_ident_4 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_4,
            TFieldIdentifier {
                name: None,
                ..field_ident_4
            }
        );
        assert_success!(i_prot.read_field_end());

        let read_ident_5 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_5,
            TFieldIdentifier {
                name: None,
                ..field_ident_5
            }
        );
        assert_success!(i_prot.read_field_end());

        let read_ident_6 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_6,
            TFieldIdentifier {
                name: None,
                field_type: TType::Stop,
                id: None,
            }
        );

        assert_success!(i_prot.read_struct_end());
    }

    #[test]
    fn must_write_nested_structs_0() {
        // last field of the containing struct is a delta
        // first field of the the contained struct is a delta

        let (_, mut o_prot) = test_objects();

        // start containing struct
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // containing struct
        // since the delta is > 0 and < 15 it gets a delta write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::I64, 1)));
        assert_success!(o_prot.write_field_end());

        // containing struct
        // since this delta > 0 and < 15 it gets a delta write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::I32, 9)));
        assert_success!(o_prot.write_field_end());

        // start contained struct
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // contained struct
        // since the delta is > 0 and < 15 it gets a delta write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::I08, 7)));
        assert_success!(o_prot.write_field_end());

        // contained struct
        // since this delta > 15 it gets a full write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::Double, 24)));
        assert_success!(o_prot.write_field_end());

        // end contained struct
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        // end containing struct
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        let expected: [u8; 7] = [
            0x16, /* field delta (1) | field type */
            0x85, /* field delta (8) | field type */
            0x73, /* field delta (7) | field type */
            0x07, /* field type */
            0x30, /* zig-zag varint field id */
            0x00, /* field stop - contained */
            0x00 /* field stop - containing */,
        ];

        assert_eq_written_bytes!(o_prot, expected);
    }

    #[test]
    fn must_round_trip_nested_structs_0() {
        // last field of the containing struct is a delta
        // first field of the the contained struct is a delta

        let (mut i_prot, mut o_prot) = test_objects();

        // start containing struct
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // containing struct
        // since the delta is > 0 and < 15 it gets a delta write
        let field_ident_1 = TFieldIdentifier::new("foo", TType::I64, 1);
        assert_success!(o_prot.write_field_begin(&field_ident_1));
        assert_success!(o_prot.write_field_end());

        // containing struct
        // since this delta > 0 and < 15 it gets a delta write
        let field_ident_2 = TFieldIdentifier::new("foo", TType::I32, 9);
        assert_success!(o_prot.write_field_begin(&field_ident_2));
        assert_success!(o_prot.write_field_end());

        // start contained struct
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // contained struct
        // since the delta is > 0 and < 15 it gets a delta write
        let field_ident_3 = TFieldIdentifier::new("foo", TType::I08, 7);
        assert_success!(o_prot.write_field_begin(&field_ident_3));
        assert_success!(o_prot.write_field_end());

        // contained struct
        // since this delta > 15 it gets a full write
        let field_ident_4 = TFieldIdentifier::new("foo", TType::Double, 24);
        assert_success!(o_prot.write_field_begin(&field_ident_4));
        assert_success!(o_prot.write_field_end());

        // end contained struct
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        // end containing struct
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        copy_write_buffer_to_read_buffer!(o_prot);

        // read containing struct back
        assert_success!(i_prot.read_struct_begin());

        let read_ident_1 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_1,
            TFieldIdentifier {
                name: None,
                ..field_ident_1
            }
        );
        assert_success!(i_prot.read_field_end());

        let read_ident_2 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_2,
            TFieldIdentifier {
                name: None,
                ..field_ident_2
            }
        );
        assert_success!(i_prot.read_field_end());

        // read contained struct back
        assert_success!(i_prot.read_struct_begin());

        let read_ident_3 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_3,
            TFieldIdentifier {
                name: None,
                ..field_ident_3
            }
        );
        assert_success!(i_prot.read_field_end());

        let read_ident_4 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_4,
            TFieldIdentifier {
                name: None,
                ..field_ident_4
            }
        );
        assert_success!(i_prot.read_field_end());

        // end contained struct
        let read_ident_6 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_6,
            TFieldIdentifier {
                name: None,
                field_type: TType::Stop,
                id: None,
            }
        );
        assert_success!(i_prot.read_struct_end());

        // end containing struct
        let read_ident_7 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_7,
            TFieldIdentifier {
                name: None,
                field_type: TType::Stop,
                id: None,
            }
        );
        assert_success!(i_prot.read_struct_end());
    }

    #[test]
    fn must_write_nested_structs_1() {
        // last field of the containing struct is a delta
        // first field of the the contained struct is a full write

        let (_, mut o_prot) = test_objects();

        // start containing struct
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // containing struct
        // since the delta is > 0 and < 15 it gets a delta write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::I64, 1)));
        assert_success!(o_prot.write_field_end());

        // containing struct
        // since this delta > 0 and < 15 it gets a delta write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::I32, 9)));
        assert_success!(o_prot.write_field_end());

        // start contained struct
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // contained struct
        // since this delta > 15 it gets a full write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::Double, 24)));
        assert_success!(o_prot.write_field_end());

        // contained struct
        // since the delta is > 0 and < 15 it gets a delta write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::I08, 27)));
        assert_success!(o_prot.write_field_end());

        // end contained struct
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        // end containing struct
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        let expected: [u8; 7] = [
            0x16, /* field delta (1) | field type */
            0x85, /* field delta (8) | field type */
            0x07, /* field type */
            0x30, /* zig-zag varint field id */
            0x33, /* field delta (3) | field type */
            0x00, /* field stop - contained */
            0x00 /* field stop - containing */,
        ];

        assert_eq_written_bytes!(o_prot, expected);
    }

    #[test]
    fn must_round_trip_nested_structs_1() {
        // last field of the containing struct is a delta
        // first field of the the contained struct is a full write

        let (mut i_prot, mut o_prot) = test_objects();

        // start containing struct
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // containing struct
        // since the delta is > 0 and < 15 it gets a delta write
        let field_ident_1 = TFieldIdentifier::new("foo", TType::I64, 1);
        assert_success!(o_prot.write_field_begin(&field_ident_1));
        assert_success!(o_prot.write_field_end());

        // containing struct
        // since this delta > 0 and < 15 it gets a delta write
        let field_ident_2 = TFieldIdentifier::new("foo", TType::I32, 9);
        assert_success!(o_prot.write_field_begin(&field_ident_2));
        assert_success!(o_prot.write_field_end());

        // start contained struct
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // contained struct
        // since this delta > 15 it gets a full write
        let field_ident_3 = TFieldIdentifier::new("foo", TType::Double, 24);
        assert_success!(o_prot.write_field_begin(&field_ident_3));
        assert_success!(o_prot.write_field_end());

        // contained struct
        // since the delta is > 0 and < 15 it gets a delta write
        let field_ident_4 = TFieldIdentifier::new("foo", TType::I08, 27);
        assert_success!(o_prot.write_field_begin(&field_ident_4));
        assert_success!(o_prot.write_field_end());

        // end contained struct
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        // end containing struct
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        copy_write_buffer_to_read_buffer!(o_prot);

        // read containing struct back
        assert_success!(i_prot.read_struct_begin());

        let read_ident_1 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_1,
            TFieldIdentifier {
                name: None,
                ..field_ident_1
            }
        );
        assert_success!(i_prot.read_field_end());

        let read_ident_2 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_2,
            TFieldIdentifier {
                name: None,
                ..field_ident_2
            }
        );
        assert_success!(i_prot.read_field_end());

        // read contained struct back
        assert_success!(i_prot.read_struct_begin());

        let read_ident_3 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_3,
            TFieldIdentifier {
                name: None,
                ..field_ident_3
            }
        );
        assert_success!(i_prot.read_field_end());

        let read_ident_4 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_4,
            TFieldIdentifier {
                name: None,
                ..field_ident_4
            }
        );
        assert_success!(i_prot.read_field_end());

        // end contained struct
        let read_ident_6 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_6,
            TFieldIdentifier {
                name: None,
                field_type: TType::Stop,
                id: None,
            }
        );
        assert_success!(i_prot.read_struct_end());

        // end containing struct
        let read_ident_7 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_7,
            TFieldIdentifier {
                name: None,
                field_type: TType::Stop,
                id: None,
            }
        );
        assert_success!(i_prot.read_struct_end());
    }

    #[test]
    fn must_write_nested_structs_2() {
        // last field of the containing struct is a full write
        // first field of the the contained struct is a delta write

        let (_, mut o_prot) = test_objects();

        // start containing struct
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // containing struct
        // since the delta is > 0 and < 15 it gets a delta write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::I64, 1)));
        assert_success!(o_prot.write_field_end());

        // containing struct
        // since this delta > 15 it gets a full write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::String, 21)));
        assert_success!(o_prot.write_field_end());

        // start contained struct
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // contained struct
        // since this delta > 0 and < 15 it gets a delta write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::Double, 7)));
        assert_success!(o_prot.write_field_end());

        // contained struct
        // since the delta is > 0 and < 15 it gets a delta write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::I08, 10)));
        assert_success!(o_prot.write_field_end());

        // end contained struct
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        // end containing struct
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        let expected: [u8; 7] = [
            0x16, /* field delta (1) | field type */
            0x08, /* field type */
            0x2A, /* zig-zag varint field id */
            0x77, /* field delta(7) | field type */
            0x33, /* field delta (3) | field type */
            0x00, /* field stop - contained */
            0x00 /* field stop - containing */,
        ];

        assert_eq_written_bytes!(o_prot, expected);
    }

    #[test]
    fn must_round_trip_nested_structs_2() {
        let (mut i_prot, mut o_prot) = test_objects();

        // start containing struct
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // containing struct
        // since the delta is > 0 and < 15 it gets a delta write
        let field_ident_1 = TFieldIdentifier::new("foo", TType::I64, 1);
        assert_success!(o_prot.write_field_begin(&field_ident_1));
        assert_success!(o_prot.write_field_end());

        // containing struct
        // since this delta > 15 it gets a full write
        let field_ident_2 = TFieldIdentifier::new("foo", TType::String, 21);
        assert_success!(o_prot.write_field_begin(&field_ident_2));
        assert_success!(o_prot.write_field_end());

        // start contained struct
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // contained struct
        // since this delta > 0 and < 15 it gets a delta write
        let field_ident_3 = TFieldIdentifier::new("foo", TType::Double, 7);
        assert_success!(o_prot.write_field_begin(&field_ident_3));
        assert_success!(o_prot.write_field_end());

        // contained struct
        // since the delta is > 0 and < 15 it gets a delta write
        let field_ident_4 = TFieldIdentifier::new("foo", TType::I08, 10);
        assert_success!(o_prot.write_field_begin(&field_ident_4));
        assert_success!(o_prot.write_field_end());

        // end contained struct
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        // end containing struct
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        copy_write_buffer_to_read_buffer!(o_prot);

        // read containing struct back
        assert_success!(i_prot.read_struct_begin());

        let read_ident_1 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_1,
            TFieldIdentifier {
                name: None,
                ..field_ident_1
            }
        );
        assert_success!(i_prot.read_field_end());

        let read_ident_2 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_2,
            TFieldIdentifier {
                name: None,
                ..field_ident_2
            }
        );
        assert_success!(i_prot.read_field_end());

        // read contained struct back
        assert_success!(i_prot.read_struct_begin());

        let read_ident_3 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_3,
            TFieldIdentifier {
                name: None,
                ..field_ident_3
            }
        );
        assert_success!(i_prot.read_field_end());

        let read_ident_4 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_4,
            TFieldIdentifier {
                name: None,
                ..field_ident_4
            }
        );
        assert_success!(i_prot.read_field_end());

        // end contained struct
        let read_ident_6 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_6,
            TFieldIdentifier {
                name: None,
                field_type: TType::Stop,
                id: None,
            }
        );
        assert_success!(i_prot.read_struct_end());

        // end containing struct
        let read_ident_7 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_7,
            TFieldIdentifier {
                name: None,
                field_type: TType::Stop,
                id: None,
            }
        );
        assert_success!(i_prot.read_struct_end());
    }

    #[test]
    fn must_write_nested_structs_3() {
        // last field of the containing struct is a full write
        // first field of the the contained struct is a full write

        let (_, mut o_prot) = test_objects();

        // start containing struct
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // containing struct
        // since the delta is > 0 and < 15 it gets a delta write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::I64, 1)));
        assert_success!(o_prot.write_field_end());

        // containing struct
        // since this delta > 15 it gets a full write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::String, 21)));
        assert_success!(o_prot.write_field_end());

        // start contained struct
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // contained struct
        // since this delta > 15 it gets a full write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::Double, 21)));
        assert_success!(o_prot.write_field_end());

        // contained struct
        // since the delta is > 0 and < 15 it gets a delta write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::I08, 27)));
        assert_success!(o_prot.write_field_end());

        // end contained struct
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        // end containing struct
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        let expected: [u8; 8] = [
            0x16, /* field delta (1) | field type */
            0x08, /* field type */
            0x2A, /* zig-zag varint field id */
            0x07, /* field type */
            0x2A, /* zig-zag varint field id */
            0x63, /* field delta (6) | field type */
            0x00, /* field stop - contained */
            0x00 /* field stop - containing */,
        ];

        assert_eq_written_bytes!(o_prot, expected);
    }

    #[test]
    fn must_round_trip_nested_structs_3() {
        // last field of the containing struct is a full write
        // first field of the the contained struct is a full write

        let (mut i_prot, mut o_prot) = test_objects();

        // start containing struct
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // containing struct
        // since the delta is > 0 and < 15 it gets a delta write
        let field_ident_1 = TFieldIdentifier::new("foo", TType::I64, 1);
        assert_success!(o_prot.write_field_begin(&field_ident_1));
        assert_success!(o_prot.write_field_end());

        // containing struct
        // since this delta > 15 it gets a full write
        let field_ident_2 = TFieldIdentifier::new("foo", TType::String, 21);
        assert_success!(o_prot.write_field_begin(&field_ident_2));
        assert_success!(o_prot.write_field_end());

        // start contained struct
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // contained struct
        // since this delta > 15 it gets a full write
        let field_ident_3 = TFieldIdentifier::new("foo", TType::Double, 21);
        assert_success!(o_prot.write_field_begin(&field_ident_3));
        assert_success!(o_prot.write_field_end());

        // contained struct
        // since the delta is > 0 and < 15 it gets a delta write
        let field_ident_4 = TFieldIdentifier::new("foo", TType::I08, 27);
        assert_success!(o_prot.write_field_begin(&field_ident_4));
        assert_success!(o_prot.write_field_end());

        // end contained struct
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        // end containing struct
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        copy_write_buffer_to_read_buffer!(o_prot);

        // read containing struct back
        assert_success!(i_prot.read_struct_begin());

        let read_ident_1 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_1,
            TFieldIdentifier {
                name: None,
                ..field_ident_1
            }
        );
        assert_success!(i_prot.read_field_end());

        let read_ident_2 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_2,
            TFieldIdentifier {
                name: None,
                ..field_ident_2
            }
        );
        assert_success!(i_prot.read_field_end());

        // read contained struct back
        assert_success!(i_prot.read_struct_begin());

        let read_ident_3 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_3,
            TFieldIdentifier {
                name: None,
                ..field_ident_3
            }
        );
        assert_success!(i_prot.read_field_end());

        let read_ident_4 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_4,
            TFieldIdentifier {
                name: None,
                ..field_ident_4
            }
        );
        assert_success!(i_prot.read_field_end());

        // end contained struct
        let read_ident_6 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_6,
            TFieldIdentifier {
                name: None,
                field_type: TType::Stop,
                id: None,
            }
        );
        assert_success!(i_prot.read_struct_end());

        // end containing struct
        let read_ident_7 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_7,
            TFieldIdentifier {
                name: None,
                field_type: TType::Stop,
                id: None,
            }
        );
        assert_success!(i_prot.read_struct_end());
    }

    #[test]
    fn must_write_bool_field() {
        let (_, mut o_prot) = test_objects();

        // no bytes should be written however
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));

        // write three fields with field ids that cannot be encoded as deltas

        // since the delta is > 0 and < 16 it gets a delta write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::Bool, 1)));
        assert_success!(o_prot.write_bool(true));
        assert_success!(o_prot.write_field_end());

        // since this delta > 0 and < 15 it gets a delta write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::Bool, 9)));
        assert_success!(o_prot.write_bool(false));
        assert_success!(o_prot.write_field_end());

        // since this delta > 15 it gets a full write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::Bool, 26)));
        assert_success!(o_prot.write_bool(true));
        assert_success!(o_prot.write_field_end());

        // since this delta > 15 it gets a full write
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::Bool, 45)));
        assert_success!(o_prot.write_bool(false));
        assert_success!(o_prot.write_field_end());

        // now, finish the struct off
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        let expected: [u8; 7] = [
            0x11, /* field delta (1) | true */
            0x82, /* field delta (8) | false */
            0x01, /* true */
            0x34, /* field id */
            0x02, /* false */
            0x5A, /* field id */
            0x00 /* stop field */,
        ];

        assert_eq_written_bytes!(o_prot, expected);
    }

    #[test]
    fn must_round_trip_bool_field() {
        let (mut i_prot, mut o_prot) = test_objects();

        // no bytes should be written however
        let struct_ident = TStructIdentifier::new("foo");
        assert_success!(o_prot.write_struct_begin(&struct_ident));

        // write two fields

        // since the delta is > 0 and < 16 it gets a delta write
        let field_ident_1 = TFieldIdentifier::new("foo", TType::Bool, 1);
        assert_success!(o_prot.write_field_begin(&field_ident_1));
        assert_success!(o_prot.write_bool(true));
        assert_success!(o_prot.write_field_end());

        // since this delta > 0 and < 15 it gets a delta write
        let field_ident_2 = TFieldIdentifier::new("foo", TType::Bool, 9);
        assert_success!(o_prot.write_field_begin(&field_ident_2));
        assert_success!(o_prot.write_bool(false));
        assert_success!(o_prot.write_field_end());

        // since this delta > 15 it gets a full write
        let field_ident_3 = TFieldIdentifier::new("foo", TType::Bool, 26);
        assert_success!(o_prot.write_field_begin(&field_ident_3));
        assert_success!(o_prot.write_bool(true));
        assert_success!(o_prot.write_field_end());

        // since this delta > 15 it gets a full write
        let field_ident_4 = TFieldIdentifier::new("foo", TType::Bool, 45);
        assert_success!(o_prot.write_field_begin(&field_ident_4));
        assert_success!(o_prot.write_bool(false));
        assert_success!(o_prot.write_field_end());

        // now, finish the struct off
        assert_success!(o_prot.write_field_stop());
        assert_success!(o_prot.write_struct_end());

        copy_write_buffer_to_read_buffer!(o_prot);

        // read the struct back
        assert_success!(i_prot.read_struct_begin());

        let read_ident_1 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_1,
            TFieldIdentifier {
                name: None,
                ..field_ident_1
            }
        );
        let read_value_1 = assert_success!(i_prot.read_bool());
        assert_eq!(read_value_1, true);
        assert_success!(i_prot.read_field_end());

        let read_ident_2 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_2,
            TFieldIdentifier {
                name: None,
                ..field_ident_2
            }
        );
        let read_value_2 = assert_success!(i_prot.read_bool());
        assert_eq!(read_value_2, false);
        assert_success!(i_prot.read_field_end());

        let read_ident_3 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_3,
            TFieldIdentifier {
                name: None,
                ..field_ident_3
            }
        );
        let read_value_3 = assert_success!(i_prot.read_bool());
        assert_eq!(read_value_3, true);
        assert_success!(i_prot.read_field_end());

        let read_ident_4 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_4,
            TFieldIdentifier {
                name: None,
                ..field_ident_4
            }
        );
        let read_value_4 = assert_success!(i_prot.read_bool());
        assert_eq!(read_value_4, false);
        assert_success!(i_prot.read_field_end());

        let read_ident_5 = assert_success!(i_prot.read_field_begin());
        assert_eq!(
            read_ident_5,
            TFieldIdentifier {
                name: None,
                field_type: TType::Stop,
                id: None,
            }
        );

        assert_success!(i_prot.read_struct_end());
    }

    #[test]
    #[should_panic]
    fn must_fail_if_write_field_end_without_writing_bool_value() {
        let (_, mut o_prot) = test_objects();
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::Bool, 1)));
        o_prot.write_field_end().unwrap();
    }

    #[test]
    #[should_panic]
    fn must_fail_if_write_stop_field_without_writing_bool_value() {
        let (_, mut o_prot) = test_objects();
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::Bool, 1)));
        o_prot.write_field_stop().unwrap();
    }

    #[test]
    #[should_panic]
    fn must_fail_if_write_struct_end_without_writing_bool_value() {
        let (_, mut o_prot) = test_objects();
        assert_success!(o_prot.write_struct_begin(&TStructIdentifier::new("foo")));
        assert_success!(o_prot.write_field_begin(&TFieldIdentifier::new("foo", TType::Bool, 1)));
        o_prot.write_struct_end().unwrap();
    }

    #[test]
    #[should_panic]
    fn must_fail_if_write_struct_end_without_any_fields() {
        let (_, mut o_prot) = test_objects();
        o_prot.write_struct_end().unwrap();
    }

    #[test]
    fn must_write_field_end() {
        assert_no_write(|o| o.write_field_end());
    }

    #[test]
    fn must_write_small_sized_list_begin() {
        let (_, mut o_prot) = test_objects();

        assert_success!(o_prot.write_list_begin(&TListIdentifier::new(TType::I64, 4)));

        let expected: [u8; 1] = [0x46 /* size | elem_type */];

        assert_eq_written_bytes!(o_prot, expected);
    }

    #[test]
    fn must_round_trip_small_sized_list_begin() {
        let (mut i_prot, mut o_prot) = test_objects();

        let ident = TListIdentifier::new(TType::I08, 10);

        assert_success!(o_prot.write_list_begin(&ident));

        copy_write_buffer_to_read_buffer!(o_prot);

        let res = assert_success!(i_prot.read_list_begin());
        assert_eq!(&res, &ident);
    }

    #[test]
    fn must_write_large_sized_list_begin() {
        let (_, mut o_prot) = test_objects();

        let res = o_prot.write_list_begin(&TListIdentifier::new(TType::List, 9999));
        assert!(res.is_ok());

        let expected: [u8; 3] = [
            0xF9, /* 0xF0 | elem_type */
            0x8F,
            0x4E /* size as varint */,
        ];

        assert_eq_written_bytes!(o_prot, expected);
    }

    #[test]
    fn must_round_trip_large_sized_list_begin() {
        let (mut i_prot, mut o_prot) = test_objects();

        let ident = TListIdentifier::new(TType::Set, 47381);

        assert_success!(o_prot.write_list_begin(&ident));

        copy_write_buffer_to_read_buffer!(o_prot);

        let res = assert_success!(i_prot.read_list_begin());
        assert_eq!(&res, &ident);
    }

    #[test]
    fn must_write_list_end() {
        assert_no_write(|o| o.write_list_end());
    }

    #[test]
    fn must_write_small_sized_set_begin() {
        let (_, mut o_prot) = test_objects();

        assert_success!(o_prot.write_set_begin(&TSetIdentifier::new(TType::Struct, 2)));

        let expected: [u8; 1] = [0x2C /* size | elem_type */];

        assert_eq_written_bytes!(o_prot, expected);
    }

    #[test]
    fn must_round_trip_small_sized_set_begin() {
        let (mut i_prot, mut o_prot) = test_objects();

        let ident = TSetIdentifier::new(TType::I16, 7);

        assert_success!(o_prot.write_set_begin(&ident));

        copy_write_buffer_to_read_buffer!(o_prot);

        let res = assert_success!(i_prot.read_set_begin());
        assert_eq!(&res, &ident);
    }

    #[test]
    fn must_write_large_sized_set_begin() {
        let (_, mut o_prot) = test_objects();

        assert_success!(o_prot.write_set_begin(&TSetIdentifier::new(TType::Double, 23891)));

        let expected: [u8; 4] = [
            0xF7, /* 0xF0 | elem_type */
            0xD3,
            0xBA,
            0x01 /* size as varint */,
        ];

        assert_eq_written_bytes!(o_prot, expected);
    }

    #[test]
    fn must_round_trip_large_sized_set_begin() {
        let (mut i_prot, mut o_prot) = test_objects();

        let ident = TSetIdentifier::new(TType::Map, 3928429);

        assert_success!(o_prot.write_set_begin(&ident));

        copy_write_buffer_to_read_buffer!(o_prot);

        let res = assert_success!(i_prot.read_set_begin());
        assert_eq!(&res, &ident);
    }

    #[test]
    fn must_write_set_end() {
        assert_no_write(|o| o.write_set_end());
    }

    #[test]
    fn must_write_zero_sized_map_begin() {
        let (_, mut o_prot) = test_objects();

        assert_success!(o_prot.write_map_begin(&TMapIdentifier::new(TType::String, TType::I32, 0)));

        let expected: [u8; 1] = [0x00]; // since size is zero we don't write anything

        assert_eq_written_bytes!(o_prot, expected);
    }

    #[test]
    fn must_read_zero_sized_map_begin() {
        let (mut i_prot, mut o_prot) = test_objects();

        assert_success!(o_prot.write_map_begin(&TMapIdentifier::new(TType::Double, TType::I32, 0)));

        copy_write_buffer_to_read_buffer!(o_prot);

        let res = assert_success!(i_prot.read_map_begin());
        assert_eq!(
            &res,
            &TMapIdentifier {
                 key_type: None,
                 value_type: None,
                 size: 0,
             }
        );
    }

    #[test]
    fn must_write_map_begin() {
        let (_, mut o_prot) = test_objects();

        assert_success!(o_prot.write_map_begin(&TMapIdentifier::new(TType::Double, TType::String, 238)));

        let expected: [u8; 3] = [
            0xEE,
            0x01, /* size as varint */
            0x78 /* key type | val type */,
        ];

        assert_eq_written_bytes!(o_prot, expected);
    }

    #[test]
    fn must_round_trip_map_begin() {
        let (mut i_prot, mut o_prot) = test_objects();

        let ident = TMapIdentifier::new(TType::Map, TType::List, 1928349);

        assert_success!(o_prot.write_map_begin(&ident));

        copy_write_buffer_to_read_buffer!(o_prot);

        let res = assert_success!(i_prot.read_map_begin());
        assert_eq!(&res, &ident);
    }

    #[test]
    fn must_write_map_end() {
        assert_no_write(|o| o.write_map_end());
    }

    #[test]
    fn must_write_map_with_bool_key_and_value() {
        let (_, mut o_prot) = test_objects();

        assert_success!(o_prot.write_map_begin(&TMapIdentifier::new(TType::Bool, TType::Bool, 1)));
        assert_success!(o_prot.write_bool(true));
        assert_success!(o_prot.write_bool(false));
        assert_success!(o_prot.write_map_end());

        let expected: [u8; 4] = [
            0x01, /* size as varint */
            0x11, /* key type | val type */
            0x01, /* key: true */
            0x02 /* val: false */,
        ];

        assert_eq_written_bytes!(o_prot, expected);
    }

    #[test]
    fn must_round_trip_map_with_bool_value() {
        let (mut i_prot, mut o_prot) = test_objects();

        let map_ident = TMapIdentifier::new(TType::Bool, TType::Bool, 2);
        assert_success!(o_prot.write_map_begin(&map_ident));
        assert_success!(o_prot.write_bool(true));
        assert_success!(o_prot.write_bool(false));
        assert_success!(o_prot.write_bool(false));
        assert_success!(o_prot.write_bool(true));
        assert_success!(o_prot.write_map_end());

        copy_write_buffer_to_read_buffer!(o_prot);

        // map header
        let rcvd_ident = assert_success!(i_prot.read_map_begin());
        assert_eq!(&rcvd_ident, &map_ident);
        // key 1
        let b = assert_success!(i_prot.read_bool());
        assert_eq!(b, true);
        // val 1
        let b = assert_success!(i_prot.read_bool());
        assert_eq!(b, false);
        // key 2
        let b = assert_success!(i_prot.read_bool());
        assert_eq!(b, false);
        // val 2
        let b = assert_success!(i_prot.read_bool());
        assert_eq!(b, true);
        // map end
        assert_success!(i_prot.read_map_end());
    }

    #[test]
    fn must_read_map_end() {
        let (mut i_prot, _) = test_objects();
        assert!(i_prot.read_map_end().is_ok()); // will blow up if we try to read from empty buffer
    }

    fn test_objects()
        -> (TCompactInputProtocol<ReadHalf<TBufferChannel>>,
            TCompactOutputProtocol<WriteHalf<TBufferChannel>>)
    {
        let mem = TBufferChannel::with_capacity(80, 80);

        let (r_mem, w_mem) = mem.split().unwrap();

        let i_prot = TCompactInputProtocol::new(r_mem);
        let o_prot = TCompactOutputProtocol::new(w_mem);

        (i_prot, o_prot)
    }

    fn assert_no_write<F>(mut write_fn: F)
    where
        F: FnMut(&mut TCompactOutputProtocol<WriteHalf<TBufferChannel>>) -> ::Result<()>,
    {
        let (_, mut o_prot) = test_objects();
        assert!(write_fn(&mut o_prot).is_ok());
        assert_eq!(o_prot.transport.write_bytes().len(), 0);
    }
}
