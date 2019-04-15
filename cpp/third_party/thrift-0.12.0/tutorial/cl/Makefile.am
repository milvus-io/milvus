#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.

setup-local-lisp-env: ensure-externals.sh
	bash ensure-externals.sh

gen-cl: $(top_srcdir)/tutorial/tutorial.thrift
	$(THRIFT) --gen cl -r $<

ALL_FILE_PREREQS = \
		   load-locally.lisp \
		   make-tutorial-server.lisp \
		   make-tutorial-client.lisp \
		   shared-implementation.lisp \
		   thrift-tutorial.asd \
		   tutorial-implementation.lisp

# NOTE: the server and client cannot be built in parallel
# because on loading the make-tutorial-* scripts SBCL will
# attempt to compile their dependencies. Unfortunately,
# because their dependencies are shared, parallel jobs can
# end up overwriting or corrupting the compiled files
all-local: gen-cl setup-local-lisp-env $(ALL_FILE_PREREQS)
	$(SBCL) --script make-tutorial-server.lisp
	$(SBCL) --script make-tutorial-client.lisp

tutorialserver: all
	./TutorialServer

tutorialclient: all
	./TutorialClient

clean-local:
	-$(RM) -r gen-*
	-$(RM) -r externals
	-$(RM) -r quicklisp
	-$(RM) -r lib
	-$(RM) quicklisp.lisp
	-$(RM) backport-update.zip
	-$(RM) shared-implementation.fasl
	-$(RM) tutorial-implementation.fasl
	-$(RM) TutorialServer
	-$(RM) TutorialClient

EXTRA_DIST = \
	tutorial-implementation.lisp \
	shared-implementation.lisp \
	thrift-tutorial.asd \
	make-tutorial-server.lisp \
	make-tutorial-client.lisp
