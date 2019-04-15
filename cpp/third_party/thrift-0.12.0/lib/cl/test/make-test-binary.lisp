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

;;;; This file is used to build the binary that runs all self-tests. The
;;;; binary is then meant to be hooked up to Thrift's `make check` facility,
;;;; but can easily be run on its own as well.

(in-package #:cl-user)

(require "asdf")
(load (merge-pathnames "../load-locally.lisp" *load-truename*))
(asdf:load-asd (merge-pathnames "../lib/de.setf.thrift-backport-update/test/thrift-test.asd" *load-truename*))
(asdf:load-system :thrift-test)
(asdf:load-system :net.didierverna.clon)

(net.didierverna.clon:nickname-package)

(defun main ()
  (let ((result (if (fiasco:run-tests 'thrift-test) 0 -1)))
    (clon:exit result)))

(clon:dump "run-tests" main)
