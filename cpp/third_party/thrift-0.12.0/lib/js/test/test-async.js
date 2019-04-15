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
 * Fully Async JavaScript test suite for ThriftTest.thrift.
 * These tests are designed to exercise the WebSocket transport
 * (which is exclusively async).
 *
 * To compile client code for this test use:
 *      $ thrift -gen js ThriftTest.thrift
 */



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
    assert.expect(1);
    const done = assert.async();
    client.testVoid(function(result) {
      assert.equal(result, undefined);
      done();
    });
  });


  QUnit.test('String', function(assert) {
    assert.expect(3);
    const done = assert.async(3);
    client.testString('', function(result) {
       assert.equal(result, '');
       done();
    });
    client.testString(stringTest, function(result) {
       assert.equal(result, stringTest);
       done();
    });

    const specialCharacters = 'quote: \" backslash:' +
          ' forwardslash-escaped: \/ ' +
          ' backspace: \b formfeed: \f newline: \n return: \r tab: ' +
          ' now-all-of-them-together: "\\\/\b\n\r\t' +
          ' now-a-bunch-of-junk: !@#$%&()(&%$#{}{}<><><';
    client.testString(specialCharacters, function(result) {
       assert.equal(result, specialCharacters);
       done();
    });
  });
  QUnit.test('Double', function(assert) {
    assert.expect(4);
    const done = assert.async(4);
    client.testDouble(0, function(result) {
       assert.equal(result, 0);
       done();
    });
    client.testDouble(-1, function(result) {
       assert.equal(result, -1);
       done();
    });
    client.testDouble(3.14, function(result) {
       assert.equal(result, 3.14);
       done();
    });
    client.testDouble(Math.pow(2, 60), function(result) {
       assert.equal(result, Math.pow(2, 60));
       done();
    });
  });
  // TODO: add testBinary()
  QUnit.test('Byte', function(assert) {
    assert.expect(2);
    const done = assert.async(2);
    client.testByte(0, function(result) {
       assert.equal(result, 0);
       done();
    });
    client.testByte(0x01, function(result) {
       assert.equal(result, 0x01);
       done();
    });
  });
  QUnit.test('I32', function(assert) {
    assert.expect(3);
    const done = assert.async(3);
    client.testI32(0, function(result) {
       assert.equal(result, 0);
       done();
    });
    client.testI32(Math.pow(2, 30), function(result) {
       assert.equal(result, Math.pow(2, 30));
       done();
    });
    client.testI32(-Math.pow(2, 30), function(result) {
       assert.equal(result, -Math.pow(2, 30));
       done();
    });
  });
  QUnit.test('I64', function(assert) {
    assert.expect(3);
    const done = assert.async(3);
    client.testI64(0, function(result) {
       assert.equal(result, 0);
       done();
    });
    //This is usually 2^60 but JS cannot represent anything over 2^52 accurately
    client.testI64(Math.pow(2, 52), function(result) {
       assert.equal(result, Math.pow(2, 52));
       done();
    });
    client.testI64(-Math.pow(2, 52), function(result) {
       assert.equal(result, -Math.pow(2, 52));
       done();
    });
  });




QUnit.module('Structured Types');

  QUnit.test('Struct', function(assert) {
    assert.expect(5);
    const done = assert.async();
    const structTestInput = new ThriftTest.Xtruct();
    structTestInput.string_thing = 'worked';
    structTestInput.byte_thing = 0x01;
    structTestInput.i32_thing = Math.pow(2, 30);
    //This is usually 2^60 but JS cannot represent anything over 2^52 accurately
    structTestInput.i64_thing = Math.pow(2, 52);

    client.testStruct(structTestInput, function(result) {
      assert.equal(result.string_thing, structTestInput.string_thing);
      assert.equal(result.byte_thing, structTestInput.byte_thing);
      assert.equal(result.i32_thing, structTestInput.i32_thing);
      assert.equal(result.i64_thing, structTestInput.i64_thing);
      assert.equal(JSON.stringify(result), JSON.stringify(structTestInput));
      done();
    });
  });

  QUnit.test('Nest', function(assert) {
    assert.expect(7);
    const done = assert.async();
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

    client.testNest(nestTestInput, function(result) {
      assert.equal(result.byte_thing, nestTestInput.byte_thing);
      assert.equal(result.struct_thing.string_thing, nestTestInput.struct_thing.string_thing);
      assert.equal(result.struct_thing.byte_thing, nestTestInput.struct_thing.byte_thing);
      assert.equal(result.struct_thing.i32_thing, nestTestInput.struct_thing.i32_thing);
      assert.equal(result.struct_thing.i64_thing, nestTestInput.struct_thing.i64_thing);
      assert.equal(result.i32_thing, nestTestInput.i32_thing);
      assert.equal(JSON.stringify(result), JSON.stringify(nestTestInput));
      done();
    });
  });

  QUnit.test('Map', function(assert) {
    assert.expect(3);
    const done = assert.async();
    const mapTestInput = {7: 77, 8: 88, 9: 99};

    client.testMap(mapTestInput, function(result) {
      for (let key in result) {
        assert.equal(result[key], mapTestInput[key]);
      }
      done();
    });
  });

  QUnit.test('StringMap', function(assert) {
    assert.expect(6);
    const done = assert.async();
    const mapTestInput = {
      'a': '123', 'a b': 'with spaces ', 'same': 'same', '0': 'numeric key',
      'longValue': stringTest, stringTest: 'long key'
    };

    client.testStringMap(mapTestInput, function(result) {
      for (let key in result) {
        assert.equal(result[key], mapTestInput[key]);
      }
      done();
    });
  });

  QUnit.test('Set', function(assert) {
    assert.expect(1);
    const done = assert.async();
    const setTestInput = [1, 2, 3];
    client.testSet(setTestInput, function(result) {
      assert.ok(result, setTestInput);
      done();
    });
  });

  QUnit.test('List', function(assert) {
    assert.expect(1);
    const done = assert.async();
    const listTestInput = [1, 2, 3];
    client.testList(listTestInput, function(result) {
      assert.ok(result, listTestInput);
      done();
    });
  });

  QUnit.test('Enum', function(assert) {
    assert.expect(1);
    const done = assert.async();
    client.testEnum(ThriftTest.Numberz.ONE, function(result) {
      assert.equal(result, ThriftTest.Numberz.ONE);
      done();
    });
  });

  QUnit.test('TypeDef', function(assert) {
    assert.expect(1);
    const done = assert.async();
    client.testTypedef(69, function(result) {
      assert.equal(result, 69);
      done();
    });
  });


QUnit.module('deeper!');

  QUnit.test('MapMap', function(assert) {
    assert.expect(16);
    const done = assert.async();
    const mapMapTestExpectedResult = {
      '4': {'1': 1, '2': 2, '3': 3, '4': 4},
      '-4': {'-4': -4, '-3': -3, '-2': -2, '-1': -1}
    };

    client.testMapMap(1, function(result) {
      for (let key in result) {
        for (let key2 in result[key]) {
          assert.equal(result[key][key2], mapMapTestExpectedResult[key][key2]);
        }
      }
      checkRecursively(assert, result, mapMapTestExpectedResult);
      done();
    });
  });


QUnit.module('Exception');

  QUnit.test('Xception', function(assert) {
    assert.expect(2);
    const done = assert.async();
    client.testException('Xception', function(e) {
      assert.equal(e.errorCode, 1001);
      assert.equal(e.message, 'Xception');
      done();
    });
  });

  QUnit.test('no Exception', function(assert) {
    assert.expect(1);
    const done = assert.async();
    client.testException('no Exception', function(e) {
      assert.ok(!e);
      done();
    });
  });

QUnit.module('Insanity');

  QUnit.test('testInsanity', function(assert) {
    assert.expect(24);
    const done = assert.async();
    const insanity = {
      '1': {
        '2': {
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
            }
          ]
        },
        '3': {
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
            }
          ]
        }
      },
      '2': { '6': { 'userMap': null, 'xtructs': null } }
    };
    client.testInsanity(new ThriftTest.Insanity(), function(res) {
      assert.ok(res, JSON.stringify(res));
      assert.ok(insanity, JSON.stringify(insanity));
      checkRecursively(assert, res, insanity);
      done();
    });
  });

QUnit.module('Oneway');

  QUnit.test('testOneway', function(assert) {
    assert.expect(1);
    const done = assert.async();
    client.testOneway(1, function(result) {
      assert.equal(result, undefined);
      done();
    });
  });
