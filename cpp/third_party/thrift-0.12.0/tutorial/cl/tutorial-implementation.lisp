(in-package #:tutorial-implementation)

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

(defun tutorial.calculator-implementation:ping ()
  (format t "ping()~%"))

(defun tutorial.calculator-implementation:add (num1 num2)
  (format t "add(~a, ~a)~%" num1 num2)
  (+ num1 num2))

(defun tutorial.calculator-implementation:calculate (logid work)
  (format t "calculate(~a, ~a)~%" logid work)
  (handler-case
      (let* ((num1 (tutorial:work-num1 work))
             (num2 (tutorial:work-num2 work))
             (op (tutorial:work-op work))
             (result
              (cond
                ((= op tutorial:operation.add) (+ num1 num2))
                ((= op tutorial:operation.subtract) (- num1 num2))
                ((= op tutorial:operation.multiply) (* num1 num2))
                ((= op tutorial:operation.divide) (/ num1 num2)))))
        (shared-implementation::add-log logid result)
        result)
    (division-by-zero () (error 'tutorial:invalidoperation
                                :why "Division by zero."
                                :what-op (tutorial:work-op work)))))

(defun tutorial.calculator-implementation:zip ()
  (format t "zip()~%"))
