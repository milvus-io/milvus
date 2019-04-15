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

package tests

import (
	"reflect"
	"testing"
	"thrifttest"
)

type ThriftTestDriver struct {
	client thrifttest.ThriftTest
	t      *testing.T
}

func NewThriftTestDriver(t *testing.T, client thrifttest.ThriftTest) *ThriftTestDriver {
	return &ThriftTestDriver{client, t}
}

func (p *ThriftTestDriver) Start() {
	client := p.client
	t := p.t

	if client.TestVoid(defaultCtx) != nil {
		t.Fatal("TestVoid failed")
	}

	if r, err := client.TestString(defaultCtx, "Test"); r != "Test" || err != nil {
		t.Fatal("TestString with simple text failed")
	}

	if r, err := client.TestString(defaultCtx, ""); r != "" || err != nil {
		t.Fatal("TestString with empty text failed")
	}

	stringTest := "Afrikaans, Alemannisch, Aragonés, العربية, مصرى, " +
		"Asturianu, Aymar aru, Azərbaycan, Башҡорт, Boarisch, Žemaitėška, " +
		"Беларуская, Беларуская (тарашкевіца), Български, Bamanankan, " +
		"বাংলা, Brezhoneg, Bosanski, Català, Mìng-dĕ̤ng-ngṳ̄, Нохчийн, " +
		"Cebuano, ᏣᎳᎩ, Česky, Словѣ́ньскъ / ⰔⰎⰑⰂⰡⰐⰠⰔⰍⰟ, Чӑвашла, Cymraeg, " +
		"Dansk, Zazaki, ދިވެހިބަސް, Ελληνικά, Emiliàn e rumagnòl, English, " +
		"Esperanto, Español, Eesti, Euskara, فارسی, Suomi, Võro, Føroyskt, " +
		"Français, Arpetan, Furlan, Frysk, Gaeilge, 贛語, Gàidhlig, Galego, " +
		"Avañe'ẽ, ગુજરાતી, Gaelg, עברית, हिन्दी, Fiji Hindi, Hrvatski, " +
		"Kreyòl ayisyen, Magyar, Հայերեն, Interlingua, Bahasa Indonesia, " +
		"Ilokano, Ido, Íslenska, Italiano, 日本語, Lojban, Basa Jawa, " +
		"ქართული, Kongo, Kalaallisut, ಕನ್ನಡ, 한국어, Къарачай-Малкъар, " +
		"Ripoarisch, Kurdî, Коми, Kernewek, Кыргызча, Latina, Ladino, " +
		"Lëtzebuergesch, Limburgs, Lingála, ລາວ, Lietuvių, Latviešu, Basa " +
		"Banyumasan, Malagasy, Македонски, മലയാളം, मराठी, مازِرونی, Bahasa " +
		"Melayu, Nnapulitano, Nedersaksisch, नेपाल भाषा, Nederlands, ‪" +
		"Norsk (nynorsk)‬, ‪Norsk (bokmål)‬, Nouormand, Diné bizaad, " +
		"Occitan, Иронау, Papiamentu, Deitsch, Polski, پنجابی, پښتو, " +
		"Norfuk / Pitkern, Português, Runa Simi, Rumantsch, Romani, Română, " +
		"Русский, Саха тыла, Sardu, Sicilianu, Scots, Sámegiella, Simple " +
		"English, Slovenčina, Slovenščina, Српски / Srpski, Seeltersk, " +
		"Svenska, Kiswahili, தமிழ், తెలుగు, Тоҷикӣ, ไทย, Türkmençe, Tagalog, " +
		"Türkçe, Татарча/Tatarça, Українська, اردو, Tiếng Việt, Volapük, " +
		"Walon, Winaray, 吴语, isiXhosa, ייִדיש, Yorùbá, Zeêuws, 中文, " +
		"Bân-lâm-gú, 粵語"

	if r, err := client.TestString(defaultCtx, stringTest); r != stringTest || err != nil {
		t.Fatal("TestString with all languages failed")
	}

	specialCharacters := "quote: \" backslash:" +
		" backspace: \b formfeed: \f newline: \n return: \r tab: " +
		" now-all-of-them-together: '\\\b\n\r\t'" +
		" now-a-bunch-of-junk: !@#$%&()(&%$#{}{}<><><" +
		" char-to-test-json-parsing: ]] \"]] \\\" }}}{ [[[ "

	if r, err := client.TestString(defaultCtx, specialCharacters); r != specialCharacters || err != nil {
		t.Fatal("TestString with specialCharacters failed")
	}

	if r, err := client.TestByte(defaultCtx, 1); r != 1 || err != nil {
		t.Fatal("TestByte(1) failed")
	}
	if r, err := client.TestByte(defaultCtx, 0); r != 0 || err != nil {
		t.Fatal("TestByte(0) failed")
	}
	if r, err := client.TestByte(defaultCtx, -1); r != -1 || err != nil {
		t.Fatal("TestByte(-1) failed")
	}
	if r, err := client.TestByte(defaultCtx, -127); r != -127 || err != nil {
		t.Fatal("TestByte(-127) failed")
	}

	if r, err := client.TestI32(defaultCtx, -1); r != -1 || err != nil {
		t.Fatal("TestI32(-1) failed")
	}
	if r, err := client.TestI32(defaultCtx, 1); r != 1 || err != nil {
		t.Fatal("TestI32(1) failed")
	}

	if r, err := client.TestI64(defaultCtx, -5); r != -5 || err != nil {
		t.Fatal("TestI64(-5) failed")
	}
	if r, err := client.TestI64(defaultCtx, 5); r != 5 || err != nil {
		t.Fatal("TestI64(5) failed")
	}
	if r, err := client.TestI64(defaultCtx, -34359738368); r != -34359738368 || err != nil {
		t.Fatal("TestI64(-34359738368) failed")
	}

	if r, err := client.TestDouble(defaultCtx, -5.2098523); r != -5.2098523 || err != nil {
		t.Fatal("TestDouble(-5.2098523) failed")
	}
	if r, err := client.TestDouble(defaultCtx, -7.012052175215044); r != -7.012052175215044 || err != nil {
		t.Fatal("TestDouble(-7.012052175215044) failed")
	}

	// TODO: add testBinary() call

	out := thrifttest.NewXtruct()
	out.StringThing = "Zero"
	out.ByteThing = 1
	out.I32Thing = -3
	out.I64Thing = 1000000
	if r, err := client.TestStruct(defaultCtx, out); !reflect.DeepEqual(r, out) || err != nil {
		t.Fatal("TestStruct failed")
	}

	out2 := thrifttest.NewXtruct2()
	out2.ByteThing = 1
	out2.StructThing = out
	out2.I32Thing = 5
	if r, err := client.TestNest(defaultCtx, out2); !reflect.DeepEqual(r, out2) || err != nil {
		t.Fatal("TestNest failed")
	}

	mapout := make(map[int32]int32)
	for i := int32(0); i < 5; i++ {
		mapout[i] = i - 10
	}
	if r, err := client.TestMap(defaultCtx, mapout); !reflect.DeepEqual(r, mapout) || err != nil {
		t.Fatal("TestMap failed")
	}

	mapTestInput := map[string]string{
		"a": "123", "a b": "with spaces ", "same": "same", "0": "numeric key",
		"longValue": stringTest, stringTest: "long key",
	}
	if r, err := client.TestStringMap(defaultCtx, mapTestInput); !reflect.DeepEqual(r, mapTestInput) || err != nil {
		t.Fatal("TestStringMap failed")
	}

	setTestInput := []int32{1, 2, 3}
	if r, err := client.TestSet(defaultCtx, setTestInput); !reflect.DeepEqual(r, setTestInput) || err != nil {
		t.Fatal("TestSet failed")
	}

	listTest := []int32{1, 2, 3}
	if r, err := client.TestList(defaultCtx, listTest); !reflect.DeepEqual(r, listTest) || err != nil {
		t.Fatal("TestList failed")
	}

	if r, err := client.TestEnum(defaultCtx, thrifttest.Numberz_ONE); r != thrifttest.Numberz_ONE || err != nil {
		t.Fatal("TestEnum failed")
	}

	if r, err := client.TestTypedef(defaultCtx, 69); r != 69 || err != nil {
		t.Fatal("TestTypedef failed")
	}

	mapMapTest := map[int32]map[int32]int32{
		4:  {1: 1, 2: 2, 3: 3, 4: 4},
		-4: {-4: -4, -3: -3, -2: -2, -1: -1},
	}
	if r, err := client.TestMapMap(defaultCtx, 1); !reflect.DeepEqual(r, mapMapTest) || err != nil {
		t.Fatal("TestMapMap failed")
	}

	crazyX1 := thrifttest.NewXtruct()
	crazyX1.StringThing = "Goodbye4"
	crazyX1.ByteThing = 4
	crazyX1.I32Thing = 4
	crazyX1.I64Thing = 4

	crazyX2 := thrifttest.NewXtruct()
	crazyX2.StringThing = "Hello2"
	crazyX2.ByteThing = 2
	crazyX2.I32Thing = 2
	crazyX2.I64Thing = 2

	crazy := thrifttest.NewInsanity()
	crazy.UserMap = map[thrifttest.Numberz]thrifttest.UserId{5: 5, 8: 8}
	crazy.Xtructs = []*thrifttest.Xtruct{crazyX1, crazyX2}

	crazyEmpty := thrifttest.NewInsanity()
	crazyEmpty.UserMap = map[thrifttest.Numberz]thrifttest.UserId{}
	crazyEmpty.Xtructs = []*thrifttest.Xtruct{}

	insanity := map[thrifttest.UserId]map[thrifttest.Numberz]*thrifttest.Insanity{
		1: {thrifttest.Numberz_TWO: crazy, thrifttest.Numberz_THREE: crazy},
		2: {thrifttest.Numberz_SIX: crazyEmpty},
	}
	if r, err := client.TestInsanity(defaultCtx, crazy); !reflect.DeepEqual(r, insanity) || err != nil {
		t.Fatal("TestInsanity failed")
	}

	if err := client.TestException(defaultCtx, "TException"); err == nil {
		t.Fatal("TestException TException failed")
	}

	if err, ok := client.TestException(defaultCtx, "Xception").(*thrifttest.Xception); ok == false || err == nil {
		t.Fatal("TestException Xception failed")
	} else if err.ErrorCode != 1001 || err.Message != "Xception" {
		t.Fatal("TestException Xception failed")
	}

	if err := client.TestException(defaultCtx, "no Exception"); err != nil {
		t.Fatal("TestException no Exception failed")
	}

	if err := client.TestOneway(defaultCtx, 0); err != nil {
		t.Fatal("TestOneway failed")
	}
}
