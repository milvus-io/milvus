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

(require "asdf")
(load (merge-pathnames "load-locally.lisp" *load-truename*))
(asdf:load-system :net.didierverna.clon)
(asdf:load-asd (merge-pathnames "gen-cl/shared/thrift-gen-shared.asd" *load-truename*))
(asdf:load-asd (merge-pathnames "gen-cl/tutorial/thrift-gen-tutorial.asd" *load-truename*))
(asdf:load-asd (merge-pathnames "thrift-tutorial.asd" *load-truename*))
(asdf:load-system :thrift-tutorial)

(net.didierverna.clon:nickname-package)

(defun main ()
  "Entry point for the binary."
  (thrift:with-client (prot #u"thrift://127.0.0.1:9090")
    (tutorial.calculator:ping prot)
    (format t "ping()~%")
    (format t "1 + 1 = ~a~%" (tutorial.calculator:add prot 1 1))
    (let ((work-instance (tutorial:make-work :num1 5
                                             :num2 0
                                             :op tutorial:operation.divide
                                             :comment "Booya!")))
      (handler-case (format t
                            "5 / 0 = ~a - Oh, really? An exception should have been thrown here.~%"
                            (tutorial.calculator:calculate prot 1 work-instance))
        (tutorial:invalidoperation (e)
          (format t "---~%(Expected) Invalid Operation caught: ~%~a~%---~%" e))))
    (let ((work-instance (tutorial:make-work :num1 15
                                             :num2 10
                                             :op tutorial:operation.subtract
                                             :comment "Playing nice this time.")))
      (handler-case (format t
                            "15 - 10 = ~a~%"
                            (tutorial.calculator:calculate prot 1 work-instance))
        (tutorial:invalidoperation (e)
          (format t "---~%(Unexpected) Invalid Operation caught: ~%~a~%---~%" e))))
    (format t "Check log: ~a~%" (shared.shared-service:get-struct prot 1))))

(clon:dump "TutorialClient" main)
