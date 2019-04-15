/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * 'License'); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//This is the server side Node test handler for the standard
//  Apache Thrift test service.

import ttypes = require("./gen-nodejs/ThriftTest_types");
import thrift = require("thrift");
import Thrift = thrift.Thrift;
import Q = require("q");


export class SyncThriftTestHandler {
  testVoid(): Q.IPromise<void> {
    //console.log('testVoid()');
    return Q.resolve<void>(undefined);
  }
  testMapMap(hello: number) {
    //console.log('testMapMap(' + hello + ')');

    var mapmap: {[key: number]: {[key: number]: number; }} = [];
    var pos: {[key: number]: number; } = [];
    var neg: {[key: number]: number; } = [];
    for (var i = 1; i < 5; i++) {
      pos[i] = i;
      neg[-i] = -i;
    }
    mapmap[4] = pos;
    mapmap[-4] = neg;

    return Q.resolve(mapmap);
  }
  testInsanity(argument: ttypes.Insanity): Q.IPromise<{ [k: number]: any; }> {
    const first_map: { [k: number]: any; } = [];
    const second_map: { [k: number]: any; } = [];
  
    first_map[ttypes.Numberz.TWO] = argument;
    first_map[ttypes.Numberz.THREE] = argument;
  
    const looney = new ttypes.Insanity();
    second_map[ttypes.Numberz.SIX] = looney;
  
    const insane: { [k: number]: any; } = [];
    insane[1] = first_map;
    insane[2] = second_map;

    return Q.resolve(insane);
  }
  testMulti(arg0: any, arg1: number, arg2: number, arg3: { [k: number]: string; }, arg4: ttypes.Numberz, arg5: number) {
    var hello = new ttypes.Xtruct();
    hello.string_thing = 'Hello2';
    hello.byte_thing = arg0;
    hello.i32_thing = arg1;
    hello.i64_thing = arg2;
    return Q.resolve(hello);
  }
  testException(arg: string): Q.IPromise<void> {
    if (arg === 'Xception') {
      var x = new ttypes.Xception();
      x.errorCode = 1001;
      x.message = arg;
      throw x;
    } else if (arg === 'TException') {
      throw new Thrift.TException(arg);
    } else {
      return Q.resolve();
    }
  }
  testMultiException(arg0: string, arg1: string) {
    if (arg0 === ('Xception')) {
      var x = new ttypes.Xception();
      x.errorCode = 1001;
      x.message = 'This is an Xception';
      throw x;
    } else if (arg0 === ('Xception2')) {
      var x2 = new ttypes.Xception2();
      x2.errorCode = 2002;
      x2.struct_thing = new ttypes.Xtruct();
      x2.struct_thing.string_thing = 'This is an Xception2';
      throw x2;
    }

    var res = new ttypes.Xtruct();
    res.string_thing = arg1;
    return Q.resolve(res);
  }
  testOneway(sleepFor: number) {
  }

  testString(thing: string) {
    return Q.resolve(thing);
  }
  testBool(thing: boolean) {
    return Q.resolve(thing);
  }
  testByte(thing: number) {
    return Q.resolve(thing);
  }
  testI32(thing: number) {
    return Q.resolve(thing);
  }
  testI64(thing: number) {
    return Q.resolve(thing);
  }
  testDouble(thing: number) {
    return Q.resolve(thing);
  }
  testBinary(thing: Buffer) {
    return Q.resolve(thing);
  }
  testStruct(thing: ttypes.Xtruct) {
    return Q.resolve(thing);
  }
  testNest(thing: ttypes.Xtruct2) {
    return Q.resolve(thing);
  }
  testMap(thing: { [k: number]: number; }) {
    return Q.resolve(thing);
  }
  testStringMap(thing: { [k: string]: string; }) {
    return Q.resolve(thing);
  }
  testSet(thing: number[]) {
    return Q.resolve(thing);
  }
  testList(thing: number[]) {
    return Q.resolve(thing);
  }
  testEnum(thing: ttypes.Numberz) {
    return Q.resolve(thing);
  }
  testTypedef(thing: number) {
    return Q.resolve(thing);
  }
}

export class AsyncThriftTestHandler {
  private syncHandler: SyncThriftTestHandler;
  constructor() {
    this.syncHandler = new SyncThriftTestHandler();
  }

  testVoid(callback: (result: void) => void): Q.IPromise<void> {
    callback(undefined);
    return Q.resolve();
  }
  testMapMap(hello: number,
    callback: (err: any, result: { [k: number]: { [k: number]: number; }; }) => void):
     Q.IPromise<{ [k: number]: { [k: number]: number; }; }> {

    var mapmap: {[key: number]: {[key: number]: number; }} = [];
    var pos: {[key: number]: number; } = [];
    var neg: {[key: number]: number; } = [];
    for (var i = 1; i < 5; i++) {
      pos[i] = i;
      neg[-i] = -i;
    }
    mapmap[4] = pos;
    mapmap[-4] = neg;

    callback(null, mapmap);
    return Q.resolve();
  }
  testInsanity(argument: ttypes.Insanity, callback?: (err: any, result: { [k: number]: any; }) => void): Q.IPromise<{ [k: number]: any; }> {
    const first_map: { [k: number]: any; } = [];
    const second_map: { [k: number]: any; } = [];
  
    first_map[ttypes.Numberz.TWO] = argument;
    first_map[ttypes.Numberz.THREE] = argument;
  
    const looney = new ttypes.Insanity();
    second_map[ttypes.Numberz.SIX] = looney;
  
    const insane: { [k: number]: any; } = [];
    insane[1] = first_map;
    insane[2] = second_map;

    if (callback !== undefined){
      callback(null, insane);
    }
    return Q.resolve();
  }
  testMulti(arg0: any, arg1: number, arg2: number, arg3: { [k: number]: string; }, arg4: ttypes.Numberz, arg5: number, result: Function): Q.IPromise<ttypes.Xtruct> {
    var hello = this.syncHandler.testMulti(arg0, arg1, arg2, arg3, arg4, arg5);
    hello.then(hello => result(null, hello));
    return Q.resolve();
  }
  testException(arg: string, result: (err: any) => void): Q.IPromise<void> {
    if (arg === 'Xception') {
      var x = new ttypes.Xception();
      x.errorCode = 1001;
      x.message = arg;
      result(x);
    } else if (arg === 'TException') {
      result(new Thrift.TException(arg));
    } else {
      result(null);
    }
    return Q.resolve();
  }
  testMultiException(arg0: string, arg1: string, result: (err: any, res?: ttypes.Xtruct) => void): Q.IPromise<ttypes.Xtruct> {
    if (arg0 === ('Xception')) {
      var x = new ttypes.Xception();
      x.errorCode = 1001;
      x.message = 'This is an Xception';
      result(x);
    } else if (arg0 === ('Xception2')) {
      var x2 = new ttypes.Xception2();
      x2.errorCode = 2002;
      x2.struct_thing = new ttypes.Xtruct();
      x2.struct_thing.string_thing = 'This is an Xception2';
      result(x2);
    } else {
      var res = new ttypes.Xtruct();
      res.string_thing = arg1;
      result(null, res);
    }
    return Q.resolve();
  }
  testOneway(sleepFor: number, result: Function) {
    this.syncHandler.testOneway(sleepFor);
  }
  testString(thing: string, callback: (err: any, result: string) => void): Q.IPromise<string> {
    callback(null, thing);
    return Q.resolve();
  }
  testByte(thing: number, callback: (err: any, result: number) => void): Q.IPromise<number> {
    callback(null, thing);
    return Q.resolve();
  }
  testBool(thing: boolean, callback: (err: any, result: boolean) => void ): Q.IPromise<boolean> {
    callback(null, thing);
    return Q.resolve();
  }
  testI32(thing: number, callback: (err: any, result: number) => void): Q.IPromise<number>  {
    callback(null, thing);
    return Q.resolve();
  }
  testI64(thing: number, callback: (err: any, result: number) => void): Q.IPromise<number>  {
    callback(null, thing);
    return Q.resolve();
  }
  testDouble(thing: number, callback: (err: any, result: number) => void): Q.IPromise<number>  {
    callback(null, thing);
    return Q.resolve();
  }
  testBinary(thing: Buffer, callback: (err: any, result: Buffer) => void): Q.IPromise<Buffer> {
    callback(null, thing);
    return Q.resolve();
  }
  testStruct(thing: ttypes.Xtruct, callback: (err: any, result: ttypes.Xtruct) => void): Q.IPromise<ttypes.Xtruct> {
    callback(null, thing);
    return Q.resolve();
  }
  testNest(thing: ttypes.Xtruct2, callback: (err: any, result: ttypes.Xtruct2) => void): Q.IPromise<ttypes.Xtruct2> {
    callback(null, thing);
    return Q.resolve();
  }
  testMap(thing: { [k: number]: number; }, callback: (err: any, result: { [k: number]: number; }) => void): Q.IPromise<{ [k: number]: number; }> {
    callback(null, thing);
    return Q.resolve();
  }
  testStringMap(thing: { [k: string]: string; }, callback: (err: any, result: { [k: string]: string; }) => void): Q.IPromise<{ [k: string]: string; }> {
    callback(null, thing);
    return Q.resolve();
  }
  testSet(thing: number[], callback: (err: any, result: number[]) => void): Q.IPromise<number[]> {
    callback(null, thing);
    return Q.resolve();
  }
  testList(thing: number[], callback: (err: any, result: number[]) => void): Q.IPromise<number[]> {
    callback(null, thing);
    return Q.resolve();
  }
  testEnum(thing: ttypes.Numberz, callback: (err: any, result: ttypes.Numberz) => void): Q.IPromise<ttypes.Numberz> {
    callback(null, thing);
    return Q.resolve();
  }
  testTypedef(thing: number, callback: (err: any, result: number) => void): Q.IPromise<number> {
    callback(null, thing);
    return Q.resolve();
  }
}
