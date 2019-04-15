// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

library thrift.test.transport.t_framed_transport_test;

import 'dart:async';
import 'dart:convert';
import 'dart:typed_data' show Uint8List;

import 'package:test/test.dart';
import 'package:thrift/thrift.dart';

void main() {
  group('TFramedTransport partial reads', () {
    final flushAwaitDuration = new Duration(seconds: 10);

    FakeReadOnlySocket socket;
    TSocketTransport socketTransport;
    TFramedTransport transport;
    var messageAvailable;

    setUp(() {
      socket = new FakeReadOnlySocket();
      socketTransport = new TClientSocketTransport(socket);
      transport = new TFramedTransport(socketTransport);
      messageAvailable = false;
    });

    expectNoReadableBytes() {
      var readBuffer = new Uint8List(128);
      var readBytes = transport.read(readBuffer, 0, readBuffer.lengthInBytes);
      expect(readBytes, 0);
      expect(messageAvailable, false);
    }

    test('Test transport reads messages where header and body are sent separately', () async {
      // buffer into which we'll read
      var readBuffer = new Uint8List(10);
      var readBytes;

      // registers for readable bytes
      var flushFuture = transport.flush().timeout(flushAwaitDuration);
      flushFuture.then((_) {
        messageAvailable = true;
      });

      // write header bytes
      socket.messageController.add(new Uint8List.fromList([0x00, 0x00, 0x00, 0x06]));

      // you shouldn't be able to get any bytes from the read,
      // because the header has been consumed internally
      expectNoReadableBytes();

      // write first batch of body
      socket.messageController.add(new Uint8List.fromList(UTF8.encode("He")));

      // you shouldn't be able to get any bytes from the read,
      // because the frame has been consumed internally
      expectNoReadableBytes();

      // write second batch of body
      socket.messageController.add(new Uint8List.fromList(UTF8.encode("llo!")));

      // have to wait for the flush to complete,
      // because it's only then that the frame is available for reading
      await flushFuture;
      expect(messageAvailable, true);

      // at this point the frame is complete, so we expect the read to complete
      readBytes = transport.read(readBuffer, 0, readBuffer.lengthInBytes);
      expect(readBytes, 6);
      expect(readBuffer.sublist(0, 6), UTF8.encode("Hello!"));
    });

    test('Test transport reads messages where header is sent in pieces '
         'and body is also sent in pieces', () async {
      // buffer into which we'll read
      var readBuffer = new Uint8List(10);
      var readBytes;

      // registers for readable bytes
      var flushFuture = transport.flush().timeout(flushAwaitDuration);
      flushFuture.then((_) {
        messageAvailable = true;
      });

      // write first part of header bytes
      socket.messageController.add(new Uint8List.fromList([0x00, 0x00]));

      // you shouldn't be able to get any bytes from the read
      expectNoReadableBytes();

      // write second part of header bytes
      socket.messageController.add(new Uint8List.fromList([0x00, 0x03]));

      // you shouldn't be able to get any bytes from the read again
      // because only the header was read, and there's no frame body
      readBytes = expectNoReadableBytes();

      // write first batch of body
      socket.messageController.add(new Uint8List.fromList(UTF8.encode("H")));

      // you shouldn't be able to get any bytes from the read,
      // because the frame has been consumed internally
      expectNoReadableBytes();

      // write second batch of body
      socket.messageController.add(new Uint8List.fromList(UTF8.encode("i!")));

      // have to wait for the flush to complete,
      // because it's only then that the frame is available for reading
      await flushFuture;
      expect(messageAvailable, true);

      // at this point the frame is complete, so we expect the read to complete
      readBytes = transport.read(readBuffer, 0, readBuffer.lengthInBytes);
      expect(readBytes, 3);
      expect(readBuffer.sublist(0, 3), UTF8.encode("Hi!"));
    });
  });
}



class FakeReadOnlySocket extends TSocket {

  StreamController<Uint8List> messageController = new StreamController<Uint8List>(sync: true);
  StreamController<Object> errorController = new StreamController<Object>();
  StreamController<TSocketState> stateController = new StreamController<TSocketState>();

  @override
  Future close() {
    // noop
  }

  @override
  bool get isClosed => false;

  @override
  bool get isOpen => true;

  @override
  Stream<Object> get onError => errorController.stream;

  @override
  Stream<Uint8List> get onMessage => messageController.stream;

  @override
  Stream<TSocketState> get onState => stateController.stream;

  @override
  Future open() {
    // noop
  }

  @override
  void send(Uint8List data) {
    // noop
  }
}

