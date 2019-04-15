#!/bin/bash

set -e

if [[ ! -e quicklisp.lisp ]]; then curl -O https://beta.quicklisp.org/quicklisp.lisp; fi
sbcl --load quicklisp.lisp \
     --eval "(ignore-errors (quicklisp-quickstart:install :path \"quicklisp/\"))" \
     --eval "(load \"quicklisp/setup.lisp\")" \
     --eval "(quicklisp:bundle-systems '(#:puri #:usocket #:closer-mop #:trivial-utf-8 #:ieee-floats #:trivial-gray-streams #:alexandria #:bordeaux-threads #:cl-ppcre #:fiasco #:net.didierverna.clon) :to \"externals/\")" \
     --eval "(quit)" \
     --no-userinit
if [[ ! -e backport-update.zip ]]; then
    curl -O -L https://github.com/TurtleWarePL/de.setf.thrift/archive/backport-update.zip;
fi
mkdir -p lib
unzip -u backport-update.zip -d lib
