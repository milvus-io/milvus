/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
 /* jshint -W100 */

/*
 * JavaScript test suite for ThriftTest.thrift. These tests
 * will run against Normal (-gen js) and jQuery (-gen js:jquery)
 * Apache Thrift interfaces.
 *
 * Synchronous blocking calls should be identical in both
 * Normal and jQuery interfaces. All synchronous tests belong
 * here.
 *
 * Asynchronous success callbacks passed as the last parameter
 * of an RPC call should be identical in both Normal and jQuery
 * interfaces. Async success tests belong here.
 *
 * Asynchronous exception processing is different in Normal
 * and jQuery interfaces. Such tests belong in the test-nojq.js
 * or test-jq.js files respectively. jQuery specific XHR object
 * tests also belong in test-jq.js. Do not create any jQuery
 * dependencies in this file or in test-nojq.js
 *
 * To compile client code for this test use:
 *      $ thrift -gen js ThriftTest.thrift
 *      -- or --
 *      $ thrift -gen js:jquery ThriftTest.thrift
 *
 * See also:
 * ++ test-nojq.js for "-gen js" only tests
 * ++ test-jq.js for "-gen js:jquery" only tests
 */

const transport = new Thrift.Transport('/service');
const protocol = new Thrift.Protocol(transport);
const client = new ThriftTest.ThriftTestClient(protocol);

// Work around for old API used by QUnitAdapter of jsTestDriver
if (typeof QUnit.log == 'function') {
  // When using real QUnit (fron PhantomJS) log failures to console
  QUnit.log(function(details) {
    if (!details.result) {
      console.log('======== FAIL ========');
      console.log('TestName: ' + details.name);
      if (details.message) console.log(details.message);
      console.log('Expected: ' + details.expected);
      console.log('Actual  : ' + details.actual);
      console.log('======================');
    }
  });
}

// all Languages in UTF-8
const stringTest = "Afrikaans, Alemannisch, Aragonés, العربية, مصرى, Asturianu, Aymar aru, Azərbaycan, Башҡорт, Boarisch, Žemaitėška, Беларуская, Беларуская (тарашкевіца), Български, Bamanankan, বাংলা, Brezhoneg, Bosanski, Català, Mìng-dĕ̤ng-ngṳ̄, Нохчийн, Cebuano, ᏣᎳᎩ, Česky, Словѣ́ньскъ / ⰔⰎⰑⰂⰡⰐⰠⰔⰍⰟ, Чӑвашла, Cymraeg, Dansk, Zazaki, ދިވެހިބަސް, Ελληνικά, Emiliàn e rumagnòl, English, Esperanto, Español, Eesti, Euskara, فارسی, Suomi, Võro, Føroyskt, Français, Arpetan, Furlan, Frysk, Gaeilge, 贛語, Gàidhlig, Galego, Avañe'ẽ, ગુજરાતી, Gaelg, עברית, हिन्दी, Fiji Hindi, Hrvatski, Kreyòl ayisyen, Magyar, Հայերեն, Interlingua, Bahasa Indonesia, Ilokano, Ido, Íslenska, Italiano, 日本語, Lojban, Basa Jawa, ქართული, Kongo, Kalaallisut, ಕನ್ನಡ, 한국어, Къарачай-Малкъар, Ripoarisch, Kurdî, Коми, Kernewek, Кыргызча, Latina, Ladino, Lëtzebuergesch, Limburgs, Lingála, ລາວ, Lietuvių, Latviešu, Basa Banyumasan, Malagasy, Македонски, മലയാളം, मराठी, Bahasa Melayu, مازِرونی, Nnapulitano, Nedersaksisch, नेपाल भाषा, Nederlands, ‪Norsk (nynorsk)‬, ‪Norsk (bokmål)‬, Nouormand, Diné bizaad, Occitan, Иронау, Papiamentu, Deitsch, Norfuk / Pitkern, Polski, پنجابی, پښتو, Português, Runa Simi, Rumantsch, Romani, Română, Русский, Саха тыла, Sardu, Sicilianu, Scots, Sámegiella, Simple English, Slovenčina, Slovenščina, Српски / Srpski, Seeltersk, Svenska, Kiswahili, தமிழ், తెలుగు, Тоҷикӣ, ไทย, Türkmençe, Tagalog, Türkçe, Татарча/Tatarça, Українська, اردو, Tiếng Việt, Volapük, Walon, Winaray, 吴语, isiXhosa, ייִדיש, Yorùbá, Zeêuws, 中文, Bân-lâm-gú, 粵語";

function checkRecursively(assert, map1, map2) {
  if (typeof map1 !== 'function' && typeof map2 !== 'function') {
    if (!map1 || typeof map1 !== 'object') {
        assert.equal(map1, map2);
    } else {
      for (let key in map1) {
        checkRecursively(assert, map1[key], map2[key]);
      }
    }
  }
}

QUnit.module('Base Types');

  QUnit.test('Void', function(assert) {
    assert.equal(client.testVoid(), undefined);
  });
  QUnit.test('Binary (String)', function(assert) {
    let binary = '';
    for (let v = 255; v >= 0; --v) {
      binary += String.fromCharCode(v);
    }
    assert.equal(client.testBinary(binary), binary);
  });
  QUnit.test('Binary (Uint8Array)', function(assert) {
    let binary = '';
    for (let v = 255; v >= 0; --v) {
      binary += String.fromCharCode(v);
    }
    const arr = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; ++i) {
      arr[i] = binary[i].charCodeAt();
    }
    assert.equal(client.testBinary(arr), binary);
  });
  QUnit.test('String', function(assert) {
    assert.equal(client.testString(''), '');
    assert.equal(client.testString(stringTest), stringTest);

    const specialCharacters = 'quote: \" backslash:' +
          ' forwardslash-escaped: \/ ' +
          ' backspace: \b formfeed: \f newline: \n return: \r tab: ' +
          ' now-all-of-them-together: "\\\/\b\n\r\t' +
          ' now-a-bunch-of-junk: !@#$%&()(&%$#{}{}<><><';
    assert.equal(client.testString(specialCharacters), specialCharacters);
  });
  QUnit.test('Double', function(assert) {
    assert.equal(client.testDouble(0), 0);
    assert.equal(client.testDouble(-1), -1);
    assert.equal(client.testDouble(3.14), 3.14);
    assert.equal(client.testDouble(Math.pow(2, 60)), Math.pow(2, 60));
  });
  QUnit.test('Byte', function(assert) {
    assert.equal(client.testByte(0), 0);
    assert.equal(client.testByte(0x01), 0x01);
  });
  QUnit.test('I32', function(assert) {
    assert.equal(client.testI32(0), 0);
    assert.equal(client.testI32(Math.pow(2, 30)), Math.pow(2, 30));
    assert.equal(client.testI32(-Math.pow(2, 30)), -Math.pow(2, 30));
  });
  QUnit.test('I64', function(assert) {
    assert.equal(client.testI64(0), 0);
    //This is usually 2^60 but JS cannot represent anything over 2^52 accurately
    assert.equal(client.testI64(Math.pow(2, 52)), Math.pow(2, 52));
    assert.equal(client.testI64(-Math.pow(2, 52)), -Math.pow(2, 52));
  });


QUnit.module('Structured Types');

  QUnit.test('Struct', function(assert) {
    const structTestInput = new ThriftTest.Xtruct();
    structTestInput.string_thing = 'worked';
    structTestInput.byte_thing = 0x01;
    structTestInput.i32_thing = Math.pow(2, 30);
    //This is usually 2^60 but JS cannot represent anything over 2^52 accurately
    structTestInput.i64_thing = Math.pow(2, 52);

    const structTestOutput = client.testStruct(structTestInput);

    assert.equal(structTestOutput.string_thing, structTestInput.string_thing);
    assert.equal(structTestOutput.byte_thing, structTestInput.byte_thing);
    assert.equal(structTestOutput.i32_thing, structTestInput.i32_thing);
    assert.equal(structTestOutput.i64_thing, structTestInput.i64_thing);

    assert.equal(JSON.stringify(structTestOutput), JSON.stringify(structTestInput));
  });

  QUnit.test('Nest', function(assert) {
    const xtrTestInput = new ThriftTest.Xtruct();
    xtrTestInput.string_thing = 'worked';
    xtrTestInput.byte_thing = 0x01;
    xtrTestInput.i32_thing = Math.pow(2, 30);
    //This is usually 2^60 but JS cannot represent anything over 2^52 accurately
    xtrTestInput.i64_thing = Math.pow(2, 52);

    const nestTestInput = new ThriftTest.Xtruct2();
    nestTestInput.byte_thing = 0x02;
    nestTestInput.struct_thing = xtrTestInput;
    nestTestInput.i32_thing = Math.pow(2, 15);

    const nestTestOutput = client.testNest(nestTestInput);

    assert.equal(nestTestOutput.byte_thing, nestTestInput.byte_thing);
    assert.equal(nestTestOutput.struct_thing.string_thing, nestTestInput.struct_thing.string_thing);
    assert.equal(nestTestOutput.struct_thing.byte_thing, nestTestInput.struct_thing.byte_thing);
    assert.equal(nestTestOutput.struct_thing.i32_thing, nestTestInput.struct_thing.i32_thing);
    assert.equal(nestTestOutput.struct_thing.i64_thing, nestTestInput.struct_thing.i64_thing);
    assert.equal(nestTestOutput.i32_thing, nestTestInput.i32_thing);

    assert.equal(JSON.stringify(nestTestOutput), JSON.stringify(nestTestInput));
  });

  QUnit.test('Map', function(assert) {
    const mapTestInput = {7: 77, 8: 88, 9: 99};

    const mapTestOutput = client.testMap(mapTestInput);

    for (let key in mapTestOutput) {
      assert.equal(mapTestOutput[key], mapTestInput[key]);
    }
  });

  QUnit.test('StringMap', function(assert) {
    const mapTestInput = {
      'a': '123', 'a b': 'with spaces ', 'same': 'same', '0': 'numeric key',
      'longValue': stringTest, stringTest: 'long key'
    };

    const mapTestOutput = client.testStringMap(mapTestInput);

    for (let key in mapTestOutput) {
      assert.equal(mapTestOutput[key], mapTestInput[key]);
    }
  });

  QUnit.test('Set', function(assert) {
    const setTestInput = [1, 2, 3];
    assert.ok(client.testSet(setTestInput), setTestInput);
  });

  QUnit.test('List', function(assert) {
    const listTestInput = [1, 2, 3];
    assert.ok(client.testList(listTestInput), listTestInput);
  });

  QUnit.test('Enum', function(assert) {
    assert.equal(client.testEnum(ThriftTest.Numberz.ONE), ThriftTest.Numberz.ONE);
  });

  QUnit.test('TypeDef', function(assert) {
    assert.equal(client.testTypedef(69), 69);
  });

  QUnit.test('Skip', function(assert) {
    const structTestInput = new ThriftTest.Xtruct();
    const modifiedClient = new ThriftTest.ThriftTestClient(protocol);

    modifiedClient.recv_testStruct = function() {
      const input = modifiedClient.input;
      const xtruct3 = new ThriftTest.Xtruct3();

      input.readMessageBegin();
      input.readStructBegin();

      // read Xtruct data with Xtruct3
      input.readFieldBegin();
      xtruct3.read(input);
      input.readFieldEnd();
      // read Thrift.Type.STOP message
      input.readFieldBegin();
      input.readFieldEnd();

      input.readStructEnd();
      input.readMessageEnd();

      return xtruct3;
    };

    structTestInput.string_thing = 'worked';
    structTestInput.byte_thing = 0x01;
    structTestInput.i32_thing = Math.pow(2, 30);
    structTestInput.i64_thing = Math.pow(2, 52);

    const structTestOutput = modifiedClient.testStruct(structTestInput);

    assert.equal(structTestOutput instanceof ThriftTest.Xtruct3, true);
    assert.equal(structTestOutput.string_thing, structTestInput.string_thing);
    assert.equal(structTestOutput.changed, null);
    assert.equal(structTestOutput.i32_thing, structTestInput.i32_thing);
    assert.equal(structTestOutput.i64_thing, structTestInput.i64_thing);
  });


QUnit.module('deeper!');

  QUnit.test('MapMap', function(assert) {
    const mapMapTestExpectedResult = {
      '4': {'1': 1, '2': 2, '3': 3, '4': 4},
      '-4': {'-4': -4, '-3': -3, '-2': -2, '-1': -1}
    };

    const mapMapTestOutput = client.testMapMap(1);


    for (let key in mapMapTestOutput) {
      for (let key2 in mapMapTestOutput[key]) {
        assert.equal(mapMapTestOutput[key][key2], mapMapTestExpectedResult[key][key2]);
      }
    }

    checkRecursively(assert, mapMapTestOutput, mapMapTestExpectedResult);
  });


QUnit.module('Exception');

  QUnit.test('Xception', function(assert) {
    assert.expect(2);
    const done = assert.async();
    try {
      client.testException('Xception');
      assert.ok(false);
    }catch (e) {
      assert.equal(e.errorCode, 1001);
      assert.equal(e.message, 'Xception');
      done();
    }
  });

  QUnit.test('no Exception', function(assert) {
    assert.expect(1);
    try {
      client.testException('no Exception');
      assert.ok(true);
    }catch (e) {
      assert.ok(false);
    }
  });

  QUnit.test('TException', function(assert) {
    //ThriftTest does not list TException as a legal exception so it will
    // generate an exception on the server that does not propagate back to
    // the client. This test has been modified to equate to "no exception"
    assert.expect(1);
    try {
      client.testException('TException');
    } catch (e) {
      //assert.ok(false);
    }
    assert.ok(true);
  });


QUnit.module('Insanity');

  const crazy = {
    'userMap': { '5': 5, '8': 8 },
    'xtructs': [{
      'string_thing': 'Goodbye4',
      'byte_thing': 4,
      'i32_thing': 4,
      'i64_thing': 4
    },
    {
      'string_thing': 'Hello2',
      'byte_thing': 2,
      'i32_thing': 2,
      'i64_thing': 2
    }]
  };
  QUnit.test('testInsanity', function(assert) {
    const insanity = {
      '1': {
        '2': crazy,
        '3': crazy
      },
      '2': { '6': { 'userMap': null, 'xtructs': null } }
    };
    const res = client.testInsanity(new ThriftTest.Insanity(crazy));
    assert.ok(res, JSON.stringify(res));
    assert.ok(insanity, JSON.stringify(insanity));

    checkRecursively(assert, res, insanity);
  });


//////////////////////////////////
//Run same tests asynchronously

QUnit.module('Async');

  QUnit.test('Double', function(assert) {
    assert.expect(1);

    const done = assert.async();
    client.testDouble(3.14159265, function(result) {
      assert.equal(result, 3.14159265);
      done();
    });
  });

  QUnit.test('Byte', function(assert) {
    assert.expect(1);

    const done = assert.async();
    client.testByte(0x01, function(result) {
      assert.equal(result, 0x01);
      done();
    });
  });

  QUnit.test('I32', function(assert) {
    assert.expect(2);

    const done = assert.async(2);
    client.testI32(Math.pow(2, 30), function(result) {
      assert.equal(result, Math.pow(2, 30));
      done();
    });

    client.testI32(Math.pow(-2, 31), function(result) {
      assert.equal(result, Math.pow(-2, 31));
      done();
    });
  });

  QUnit.test('I64', function(assert) {
    assert.expect(2);

    const done = assert.async(2);
    //This is usually 2^60 but JS cannot represent anything over 2^52 accurately
    client.testI64(Math.pow(2, 52), function(result) {
      assert.equal(result, Math.pow(2, 52));
      done();
    });

    //This is usually 2^60 but JS cannot represent anything over 2^52 accurately
    client.testI64(Math.pow(-2, 52), function(result) {
      assert.equal(result, Math.pow(-2, 52));
      done();
    });
  });
