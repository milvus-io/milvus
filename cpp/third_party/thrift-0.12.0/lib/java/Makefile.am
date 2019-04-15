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
#

export CLASSPATH

all-local:
	./gradlew $(GRADLE_OPTS) assemble \
		-Prelease=true \
		-Pthrift.version=$(PACKAGE_VERSION) \
		--console=plain

install-exec-hook:
	./gradlew $(GRADLE_OPTS) install \
		-Prelease=true \
		-Pinstall.path=$(DESTDIR)$(JAVA_PREFIX) \
		-Pinstall.javadoc.path=$(DESTDIR)$(docdir)/java \
		-Pthrift.version=$(PACKAGE_VERSION) \
		--console=plain

clean-local:
	./gradlew $(GRADLE_OPTS) clean --console=plain

precross: $(THRIFT)
	./gradlew $(GRADLE_OPTS) shadowJar \
		-Prelease=true \
		-Pthrift.version=$(PACKAGE_VERSION) \
		-Pthrift.compiler=$(THRIFT) \
		--console=plain

check-local: $(THRIFT)
	./gradlew $(GRADLE_OPTS) test \
		-Prelease=true \
		-Pthrift.version=$(PACKAGE_VERSION) \
		-Pthrift.compiler=$(THRIFT) \
		--console=plain

maven-publish:
	./gradlew $(GRADLE_OPTS) uploadArchives \
		-Prelease=true \
		-Pthrift.version=$(PACKAGE_VERSION) \
		--console=plain

EXTRA_DIST = \
	build.gradle \
	gradle.properties \
	settings.gradle \
	gradle \
	gradlew \
	gradlew.bat \
	CMakeLists.txt \
	coding_standards.md \
	android \
	src \
	test \
	code_quality_tools \
	README.md
