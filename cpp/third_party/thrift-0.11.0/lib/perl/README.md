Thrift Perl Software Library

# Summary

Apache Thrift is a software framework for scalable cross-language services development.
It combines a software stack with a code generation engine to build services that work
efficiently and seamlessly between many programming languages.  A language-neutral IDL
is used to generate functioning client libraries and server-side handling frameworks.

# License

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

# For More Information

See the [Apache Thrift Web Site](http://thrift.apache.org/) for more information.

# Using Thrift with Perl

Thrift requires Perl >= 5.10.0

Unexpected exceptions in a service handler are converted to
TApplicationException with type INTERNAL ERROR and the string
of the exception is delivered as the message.

On the client side, exceptions are thrown with die, so be sure
to wrap eval{} statments around any code that contains exceptions.

Please see tutoral and test dirs for examples.

The Perl ForkingServer ignores SIGCHLD allowing the forks to be
reaped by the operating system naturally when they exit.  This means
one cannot use a custom SIGCHLD handler in the consuming perl
implementation that calls serve().  It is acceptable to use
a custom SIGCHLD handler within a thrift handler implementation
as the ForkingServer resets the forked child process to use
default signal handling.

# Dependencies

The following modules are not provided by Perl 5.10.0 but are required
to use Thrift.

## Runtime

  * Bit::Vector
  * Class::Accessor

### HttpClient Transport

These are only required if using Thrift::HttpClient:

  * HTTP::Request
  * IO::String
  * LWP::UserAgent

### SSL/TLS

These are only required if using Thrift::SSLSocket or Thrift::SSLServerSocket:

  * IO::Socket::SSL

# Breaking Changes

## 0.10.0

The socket classes were refactored in 0.10.0 so that there is one package per
file.  This means `use Socket;` no longer defines SSLSocket.  You can use this
technique to make your application run against 0.10.0 as well as earlier versions:

`eval { require Thrift::SSLSocket; } or do { require Thrift::Socket; }`

## 0.11.0

  * Namespaces of packages that were not scoped within Thrift have been fixed.
  ** TApplicationException is now Thrift::TApplicationException
  ** TException is now Thrift::TException
  ** TMessageType is now Thrift::TMessageType
  ** TProtocolException is now Thrift::TProtocolException
  ** TProtocolFactory is now Thrift::TProtocolFactory
  ** TTransportException is now Thrift::TTransportException
  ** TType is now Thrift::TType

If you need a single version of your code to work with both older and newer thrift
namespace changes, you can make the new, correct namespaces behave like the old ones
in your files with this technique to create an alias, which will allow you code to
run against either version of the perl runtime for thrift:

`BEGIN {*TType:: = *Thrift::TType::}`

  * Packages found in Thrift.pm were moved into the Thrift/ directory in separate files:
  ** Thrift::TApplicationException is now in Thrift/Exception.pm
  ** Thrift::TException is now in Thrift/Exception.pm
  ** Thrift::TMessageType is now in Thrift/MessageType.pm
  ** Thrift::TType is now in Thrift/Type.pm

If you need to modify your code to work against both older or newer thrift versions,
you can deal with these changes in a backwards compatible way in your projects using eval:

`eval  { require Thrift::Exception; require Thrift::MessageType; require Thrift::Type; }
 or do { require Thrift; }`

# Deprecations

## 0.11.0

Thrift::HttpClient setRecvTimeout() and setSendTimeout() are deprecated. 
Use setTimeout instead.

