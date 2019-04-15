Thrift Java Software Library

License
=======

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.

Building and installing from source
===================================

When using a CMake build from the source distribution on Linux the
easiest way to build and install is this simple command line:

    make all && sudo make install/fast

It is important to use the install/fast option to eliminate
the automatic rebuild by dependency that causes issues because
the build tooling is designed to work with cached files in the
user home directory during the build process. Instead this builds
the code in the expected local build tree and then uses CMake
install code to copy to the target destination.

Building Thrift with Gradle without CMake/Autoconf
==================================================

The Thrift Java source is not build using the GNU tools, but rather uses
the Gradle build system, which tends to be predominant amongst Java
developers.

To compile the Java Thrift libraries, simply do the following:

    ./gradlew

Yep, that's easy. Look for libthrift-<version>.jar in the build/libs directory.

The default build will run the unit tests which expect a usable
Thrift compiler to exist on the system. You have two choices for
that.

* Build the Thrift executable from source at the default
  location in the source tree. The project is configured
  to look for it there.
* Install the published binary distribution to have Thrift
  executable in a known location and add the path to the
  ~/.gradle/gradle.properties file using the property name
  "thrift.compiler". For example this would set the path in
  a Windows box if Thrift was installed under C:\Thrift

    thrift.compiler=C:/Thrift/thrift.exe

To just build the library without running unit tests you simply do this.

    ./gradlew assemble

To install the library in the local Maven repository location
where other Maven or Gradle builds can reference it simply do this.

    ./gradlew install

The library will be placed in your home directory under .m2/repository

To include Thrift in your applications simply add libthrift.jar to your
classpath, or install if in your default system classpath of choice.


Build Thrift behind a proxy:

    ./gradlew -Dhttp.proxyHost=myproxyhost -Dhttp.proxyPort=8080 -Dhttp.proxyUser=thriftuser -Dhttp.proxyPassword=topsecret

or via

    ./configure --with-java GRADLE_OPTS='-Dhttp.proxyHost=myproxyhost -Dhttp.proxyPort=8080 -Dhttp.proxyUser=thriftuser -Dhttp.proxyPassword=topsecret'


Unit Test HTML Reports
======================

The build will automatically generate an HTML Unit Test report. This can be found
under build/reports/tests/test/index.html. It can be viewed with a browser
directly from that location.


Clover Code Coverage for Thrift
===============================

The build will optionally generate Clover Code coverage if the Gradle property
`cloverEnabled=true` is set in ~/.gradle/gradle.properties or on the command line
via `-PcloverEnabled=true`. The generated report can be found under the location
build/reports/clover/html/index.html. It can be viewed with a browser
directly from that location. Additionally, a PDF report is generated and is found
under the location build/reports/clover/clover.pdf.

The following command will build, unit test, and generate Clover reports:

    ./gradlew -PcloverEnabled=true


Publishing Maven Artifacts to Maven Central
===========================================

The Automake build generates a Makefile that provides the correct parameters
when you run the build provided the configure.ac has been set with the correct
version number. The Gradle build will receive the correct value for the build.
The same applies to the CMake build, the value from the configure.ac file will
be used if you execute these commands:

    make maven-publish   -- This is for an Automake Linux build
    make MavenPublish    -- This is for a CMake generated build

The uploadArchives task in Gradle is preconfigured with all necessary details
to sign and publish the artifacts from the build to the Apache Maven staging
repository. The task requires the following externally provided properties to
authenticate to the repository and sign the artifacts. The preferred approach
is to create or edit the ~/.gradle/gradle.properties file and add the following
properties to it.

    # Signing key information for artifacts PGP signature (values are examples)
    signing.keyId=24875D73
    signing.password=secret
    signing.secretKeyRingFile=/Users/me/.gnupg/secring.gpg

    # Apache Maven staging repository user credentials
    mavenUser=meMyselfAndI
    mavenPassword=MySuperAwesomeSecretPassword

It is also possible to manually publish using the Gradle build directly.
With the key information and credentials in place the following will generate
if needed the build artifacts and proceed to publish the results.

    ./gradlew -Prelease=true -Pthrift.version=0.11.0 uploadArchives

It is also possible to override the target repository for the Maven Publication
by using a Gradle property, for example you can publish signed JAR files to your
company internal server if you add this to the command line or in the
~/.gradle/gradle.properties file. The URL below assumes a Nexus Repository.

    maven-repository-url=https://my.company.com/service/local/staging/deploy/maven2

Or the same on the command line:

    ./gradlew -Pmaven-repository-url=https://my.company.com/service/local/staging/deploy/maven2 -Prelease=true -Pthrift.version=0.11.0 uploadArchives


Dependencies
============

Gradle
http://gradle.org/
