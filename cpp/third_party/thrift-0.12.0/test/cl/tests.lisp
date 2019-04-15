(in-package #:thrift-cross)

;;;; Licensed under the Apache License, Version 2.0 (the "License");
;;;; you may not use this file except in compliance with the License.
;;;; You may obtain a copy of the License at
;;;;
;;;;     http://www.apache.org/licenses/LICENSE-2.0
;;;;
;;;; Unless required by applicable law or agreed to in writing, software
;;;; distributed under the License is distributed on an "AS IS" BASIS,
;;;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;;;; See the License for the specific language governing permissions and
;;;; limitations under the License.

;;;; The tests here only make sense in the context of a TestServer
;;;; running and the dynamic variable thrift-cross::*prot*
;;;; being set with a client connection to the TestServer. Normally,
;;;; this is handled in make-test-client.lisp.


;;; Standard Thrift cross-test error codes
(defparameter *test_basetypes* 1)
(defparameter *test_structs* 2)
(defparameter *test_containers* 4)
(defparameter *test_exceptions* 8)
(defparameter *test_unknown* 64)
(defparameter *test_timeout* 128)

(defun cross-test (&key (multiplexed nil))
  "The main cross-test runner."
  (let ((result nil))
    (handler-case
        (progn
          (unless (run-package-tests :package :base-types)
            (pushnew *test_basetypes* result))
          (unless (run-package-tests :package :structs)
            (pushnew *test_structs* result))
          (unless (run-package-tests :package :containers)
            (pushnew *test_containers* result))
          (unless (run-package-tests :package :exceptions)
            (pushnew *test_exceptions* result))
          (unless (run-package-tests :package :misc)
            (pushnew *test_unknown* result))

          ;; It doesn't seem like anyone actually uses
          ;; the second test service when testing multiplexing,
          ;; so this would fail against servers in other
          ;; languages. For now, anyway.
          #+(or)
          (when multiplexed
            (unless (run-package-tests :package :multiplex)
              (pushnew *test_unknown* result))))
      (error (e) (pushnew *test_unknown* result)))
    (apply #'+ result)))

(fiasco:define-test-package #:base-types)

(in-package #:base-types)

(defconstant *lang-string* "Afrikaans, Alemannisch, Aragonés, العربية, مصرى, Asturianu, Aymar aru, Azərbaycan, Башҡорт, Boarisch, Žemaitėška, Беларуская, Беларуская (тарашкевіца), Български, Bamanankan, বাংলা, Brezhoneg, Bosanski, Català, Mìng-dĕ̤ng-ngṳ̄, Нохчийн, Cebuano, ᏣᎳᎩ, Česky, Словѣ́ньскъ / ⰔⰎⰑⰂⰡⰐⰠⰔⰍⰟ, Чӑвашла, Cymraeg, Dansk, Zazaki, ދިވެހިބަސް, Ελληνικά, Emiliàn e rumagnòl, English, Esperanto, Español, Eesti, Euskara, فارسی, Suomi, Võro, Føroyskt, Français, Arpetan, Furlan, Frysk, Gaeilge, 贛語, Gàidhlig, Galego, Avañe'ẽ, ગુજરાતી, Gaelg, עברית, हिन्दी, Fiji Hindi, Hrvatski, Kreyòl ayisyen, Magyar, Հայերեն, Interlingua, Bahasa Indonesia, Ilokano, Ido, Íslenska, Italiano, 日本語, Lojban, Basa Jawa, ქართული, Kongo, Kalaallisut, ಕನ್ನಡ, 한국어, Къарачай-Малкъар, Ripoarisch, Kurdî, Коми, Kernewek, Кыргызча, Latina, Ladino, Lëtzebuergesch, Limburgs, Lingála, ລາວ, Lietuvių, Latviešu, Basa Banyumasan, Malagasy, Македонски, മലയാളം, मराठी, مازِرونی, Bahasa Melayu, Nnapulitano, Nedersaksisch, नेपाल भाषा, Nederlands, ‪Norsk (nynorsk)‬, ‪Norsk (bokmål)‬, Nouormand, Diné bizaad, Occitan, Иронау, Papiamentu, Deitsch, Polski, پنجابی, پښتو, Norfuk / Pitkern, Português, Runa Simi, Rumantsch, Romani, Română, Русский, Саха тыла, Sardu, Sicilianu, Scots, Sámegiella, Simple English, Slovenčina, Slovenščina, Српски / Srpski, Seeltersk, Svenska, Kiswahili, தமிழ், తెలుగు, Тоҷикӣ, ไทย, Türkmençe, Tagalog, Türkçe, Татарча/Tatarça, Українська, اردو, Tiếng Việt, Volapük, Walon, Winaray, 吴语, isiXhosa, ייִדיש, Yorùbá, Zeêuws, 中文, Bân-lâm-gú, 粵語")

(defparameter *trick-string* (format nil "quote: \" backslash: \\ newline: ~% backspace: ~C ~
                                          tab: ~T junk: !@#$%&()(&%$#{}{}<><><" #\backspace))

(defconstant *binary-sequence* #(128 129 130 131 132 133 134 135 136 137 138 139 140 141 142 143 144 145 146 147 148 149 150 151 152 153 154 155 156 157 158 159 160 161 162 163 164 165 166 167 168 169 170 171 172 173 174 175 176 177 178 179 180 181 182 183 184 185 186 187 188 189 190 191 192 193 194 195 196 197 198 199 200 201 202 203 204 205 206 207 208 209 210 211 212 213 214 215 216 217 218 219 220 221 222 223 224 225 226 227 228 229 230 231 232 233 234 235 236 237 238 239 240 241 242 243 244 245 246 247 248 249 250 251 252 253 254 255 0 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32 33 34 35 36 37 38 39 40 41 42 43 44 45 46 47 48 49 50 51 52 53 54 55 56 57 58 59 60 61 62 63 64 65 66 67 68 69 70 71 72 73 74 75 76 77 78 79 80 81 82 83 84 85 86 87 88 89 90 91 92 93 94 95 96 97 98 99 100 101 102 103 104 105 106 107 108 109 110 111 112 113 114 115 116 117 118 119 120 121 122 123 124 125 126 127))

(deftest void-test ()
  (is (null (thrift.test.thrift-test:test-void thrift-cross::*prot*))))

(deftest boolean-test ()
  (is (thrift.test.thrift-test:test-bool thrift-cross::*prot* t))
  (is (not (thrift.test.thrift-test:test-bool thrift-cross::*prot* nil))))

(deftest integer-test ()
  (is (= (thrift.test.thrift-test:test-byte thrift-cross::*prot* 127) 127))
  (is (= (thrift.test.thrift-test:test-byte thrift-cross::*prot* -128) -128))
  (is (= (thrift.test.thrift-test:test-byte thrift-cross::*prot* 42) 42))
  (is (= (thrift.test.thrift-test:test-byte thrift-cross::*prot* 0) 0))
  (is (= (thrift.test.thrift-test:test-i32 thrift-cross::*prot* 0) 0))
  (is (= (thrift.test.thrift-test:test-i32 thrift-cross::*prot* 2147483647) 2147483647))
  (is (= (thrift.test.thrift-test:test-i32 thrift-cross::*prot* -2147483648) -2147483648))
  (is (= (thrift.test.thrift-test:test-i64 thrift-cross::*prot* 0) 0))
  (is (= (thrift.test.thrift-test:test-i64 thrift-cross::*prot* 9223372036854775807) 9223372036854775807))
  (is (= (thrift.test.thrift-test:test-i64 thrift-cross::*prot* -9223372036854775808) -9223372036854775808)))

(deftest double-test ()
  (is (= (thrift.test.thrift-test:test-double thrift-cross::*prot* 0.0) 0))
  (is (= (thrift.test.thrift-test:test-double thrift-cross::*prot* 42.0) 42))
  (is (= (thrift.test.thrift-test:test-double thrift-cross::*prot* -555.0) -555))
  (is (= (thrift.test.thrift-test:test-double thrift-cross::*prot* -52.3678) -52.3678)))

(deftest string-test ()
  (is (string= (thrift.test.thrift-test:test-string thrift-cross::*prot* "") ""))
  (is (string= (thrift.test.thrift-test:test-string thrift-cross::*prot* "(defun botsbuildbots () (botsbuilsbots))")
               "(defun botsbuildbots () (botsbuilsbots))"))
  (is (string= (thrift.test.thrift-test:test-string thrift-cross::*prot* *lang-string*) *lang-string*))
  (is (string= (thrift.test.thrift-test:test-string thrift-cross::*prot* *trick-string*) *trick-string*)))

(deftest binary-test ()
  (is (equalp (thrift.test.thrift-test:test-binary thrift-cross::*prot* #()) #()))
  (is (equalp (thrift.test.thrift-test:test-binary thrift-cross::*prot* *binary-sequence*) *binary-sequence*)))

(deftest enum-test ()
  (is (= (thrift.test.thrift-test:test-enum thrift-cross::*prot* thrift.test:numberz.five) thrift.test:numberz.five))
  (is (= (thrift.test.thrift-test:test-enum thrift-cross::*prot* thrift.test:numberz.eight) thrift.test:numberz.eight))
  (is (= (thrift.test.thrift-test:test-enum thrift-cross::*prot* thrift.test:numberz.one) thrift.test:numberz.one)))

(deftest typedef-test ()
  (is (= (thrift.test.thrift-test:test-typedef thrift-cross::*prot* 309858235082523) 309858235082523)))

(fiasco:define-test-package #:structs)

(in-package #:structs)

(defparameter *test-struct* (thrift.test:make-xtruct :string-thing "Hell is empty."
                                                     :byte-thing -2
                                                     :i32-thing 42
                                                     :i64-thing 42424242))

(defparameter *test-nest* (thrift.test:make-xtruct2 :byte-thing 42
                                                    :struct-thing *test-struct*
                                                    :i32-thing -42))

(deftest struct-test ()
  (let ((rec-struct (thrift.test.thrift-test:test-struct thrift-cross::*prot* *test-struct*)))
    (is (string= (thrift.test:xtruct-string-thing *test-struct*)
                 (thrift.test:xtruct-string-thing rec-struct)))
    (is (= (thrift.test:xtruct-byte-thing *test-struct*)
           (thrift.test:xtruct-byte-thing rec-struct)))
    (is (= (thrift.test:xtruct-i32-thing *test-struct*)
           (thrift.test:xtruct-i32-thing rec-struct)))
    (is (= (thrift.test:xtruct-i64-thing *test-struct*)
           (thrift.test:xtruct-i64-thing rec-struct)))))

(deftest nest-test ()
  (let* ((rec-nest (thrift.test.thrift-test:test-nest thrift-cross::*prot* *test-nest*))
         (rec-struct (thrift.test:xtruct2-struct-thing rec-nest)))
    (is (string= (thrift.test:xtruct-string-thing *test-struct*)
                 (thrift.test:xtruct-string-thing rec-struct)))
    (is (= (thrift.test:xtruct-byte-thing *test-struct*)
           (thrift.test:xtruct-byte-thing rec-struct)))
    (is (= (thrift.test:xtruct-i32-thing *test-struct*)
           (thrift.test:xtruct-i32-thing rec-struct)))
    (is (= (thrift.test:xtruct-i64-thing *test-struct*)
           (thrift.test:xtruct-i64-thing rec-struct)))
    (is (= (thrift.test:xtruct2-byte-thing *test-nest*)
           (thrift.test:xtruct2-byte-thing rec-nest)))
    (is (= (thrift.test:xtruct2-i32-thing *test-nest*)
           (thrift.test:xtruct2-i32-thing rec-nest)))))

(fiasco:define-test-package #:containers)

(in-package #:containers)

(deftest list-test ()
  (is (null (thrift.test.thrift-test:test-list thrift-cross::*prot* nil)))
  (is (equal (thrift.test.thrift-test:test-list thrift-cross::*prot* '(42 -42 0 5)) '(42 -42 0 5))))

(deftest set-test ()
  (is (null (thrift.test.thrift-test:test-set thrift-cross::*prot* nil)))
  (is (equal (sort (thrift.test.thrift-test:test-set thrift-cross::*prot* (list 42 -42 0 5)) #'<)
             '(-42 0 5 42))))

(defun map= (map1 map2 &key (car-predicate #'equal) (cdr-predicate #'equal))
  "Compare two assoc maps according to the predicates given."
  (not (set-exclusive-or map1 map2 :test (lambda (el1 el2)
                                           (and (funcall car-predicate
                                                         (car el1)
                                                         (car el2))
                                                (funcall cdr-predicate
                                                         (cdr el1)
                                                         (cdr el2)))))))

(deftest map-test ()
  (is (null (thrift.test.thrift-test:test-map thrift-cross::*prot* nil)))
  (is (map= (thrift.test.thrift-test:test-map thrift-cross::*prot* '((0 . 1) (42 . -42) (5 . 5)))
            '((0 . 1) (42 . -42) (5 . 5))))
  (is (map= (thrift.test.thrift-test:test-map-map thrift-cross::*prot* 42)
            '((-4 . ((-4 . -4) (-3 . -3) (-2 . -2) (-1 . -1)))
              (4 . ((1 . 1) (2 . 2) (3 . 3) (4 . 4))))
            :cdr-predicate #'map=)))

(fiasco:define-test-package #:exceptions)

(in-package #:exceptions)

(defun test-xception (expected-code expected-message function &rest args)
  "A helper function to test whether xception is signalled, and whether its fields have the expected values."
  (handler-case (progn (apply function args)
                       nil)
    (thrift.test:xception (ex) (and (= (thrift.test::xception-error-code ex) expected-code)
                                    (string= (thrift.test::xception-message ex) expected-message)))))

(defun test-xception2 (expected-code expected-message function &rest args)
  "A helper function to test whether xception2 is signalled, and whether its fields have the expected values."
  (handler-case (progn (apply function args)
                       nil)
    (thrift.test:xception2 (ex) (and (= (thrift.test::xception2-error-code ex) expected-code)
                                     (string= (thrift.test::xtruct-string-thing
                                               (thrift.test::xception2-struct-thing ex))
                                              expected-message)))))

(deftest exception-test ()
  (is (test-xception 1001 "Xception" #'thrift.test.thrift-test:test-exception thrift-cross::*prot* "Xception"))
  (signals thrift:application-error (thrift.test.thrift-test:test-exception thrift-cross::*prot* "TException"))
  (finishes (thrift.test.thrift-test:test-exception thrift-cross::*prot* "success")))

(deftest multi-exception-test ()
  (is (test-xception 1001
                     "This is an Xception"
                     #'thrift.test.thrift-test:test-multi-exception
                     thrift-cross::*prot*
                     "Xception"
                     "meaningless"))
  (is (test-xception2 2002
                      "This is an Xception2"
                      #'thrift.test.thrift-test:test-multi-exception
                      thrift-cross::*prot*
                      "Xception2"
                      "meaningless too!"))
  (is (string= "foobar" (thrift.test:xtruct-string-thing
                         (thrift.test.thrift-test:test-multi-exception thrift-cross::*prot*
                                                           "success!"
                                                           "foobar")))))

(fiasco:define-test-package #:misc)

(in-package #:misc)

(deftest oneway-test ()
  (is (null (thrift.test.thrift-test:test-oneway thrift-cross::*prot* 1))))

(fiasco:define-test-package #:multiplex)

(in-package #:multiplex)

(deftest multiplex-test ()
  ;; Removed from the IDL definition.
  ;; (finishes (thrift.test.second-service:blah-blah thrift-cross::*prot*))
  (is (string= "asd" (thrift.test.second-service:secondtest-string thrift-cross::*prot* "asd"))))
