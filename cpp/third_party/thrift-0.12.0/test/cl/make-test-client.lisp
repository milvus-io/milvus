(in-package #:cl-user)

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

#+(or) (when (not (boundp 'sb-impl::default-external-format)
                 (setf sb-impl::default-external-format :UTF-8)))

(require "asdf")
(load (merge-pathnames "../../lib/cl/load-locally.lisp" *load-truename*))
(asdf:load-system :net.didierverna.clon)
(asdf:load-system :fiasco)
(asdf:load-asd (merge-pathnames "gen-cl/ThriftTest/thrift-gen-ThriftTest.asd" *load-truename*))
(asdf:load-system :thrift-gen-thrifttest)

(net.didierverna.clon:nickname-package)

(defpackage #:thrift-cross
  (:use #:common-lisp #:fiasco)
  (:export #:cross-test))

(in-package #:thrift-cross)

(defparameter *prot* nil)

(load (merge-pathnames "tests.lisp" *load-truename*) :external-format :UTF-8)

(clon:defsynopsis ()
  (text :contents "The Common Lisp client for Thrift's cross-language test suite.")
  (group (:header "Allowed options:")
    (flag :short-name "h" :long-name "help"
          :description "Print this help and exit.")
    (stropt :long-name "host"
            :description "The host to connect to."
            :default-value "localhost"
            :argument-name "ARG")
    (stropt :long-name "port"
            :description "Number of the port to listen for connections on."
            :default-value "9090"
            :argument-name "ARG"
            :argument-type :optional)
    (stropt :long-name "transport"
            :description "Transport: transport to use (\"buffered\", \"framed\")"
            :default-value "buffered"
            :argument-name "ARG")
    (stropt :long-name "protocol"
            :description "Protocol: protocol to use (\"binary\", \"multi\")"
            :default-value "binary"
            :argument-name "ARG")))

(defun main ()
  "Entry point for our standalone application."
  (clon:make-context)
  (when (clon:getopt :short-name "h")
    (clon:help)
    (clon:exit))
  (let ((port "9090")
        (host "localhost")
        (framed nil)
        (multiplexed nil))
    (clon:do-cmdline-options (option name value source)
      (print (list option name value source))
      (if (string= name "host")
          (setf host value))
      (if (string= name "port")
          (setf port value))
      (if (string= name "transport")
          (cond ((string= value "buffered") (setf framed nil))
                ((string= value "framed") (setf framed t))
                (t (error "Unsupported transport."))))
      (if (string= name "protocol")
          (cond ((string= value "binary") (setf multiplexed nil))
                ((string= value "multi") (setf multiplexed t))
                (t (error "Unsupported protocol.")))))
    (terpri)
    (setf *prot* (thrift.implementation::client (puri:parse-uri
                                                 (concatenate 'string "thrift://" host ":" port))
                                                :framed framed
                                                :multiplexed multiplexed))
    (let ((result (cross-test :multiplexed multiplexed)))
      (thrift.implementation::close *prot*)
      (clon:exit result))))

(clon:dump "TestClient" main)
