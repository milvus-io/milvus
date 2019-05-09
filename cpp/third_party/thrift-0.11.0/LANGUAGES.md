# Apache Thrift Language Support #

Last Modified: 2017-10-05<br>
Version: 0.11.0+

Thrift supports many programming languages and has an impressive test suite that exercises most of the languages, protocols, and transports that represents a matrix of thousands of possible combinations.  Each language typically has a minimum required version as well as support libraries - some mandatory and some optional.  All of this information is provided below to help you assess whether you can use Apache Thrift with your project.  Obviously this is a complex matrix to maintain and may not be correct in all cases - if you spot an error please inform the developers using the mailing list.

Apache Thrift has a choice of two build systems.  The `autoconf` build system is the most complete build and is used to build all supported languages.  The `cmake` build system has been designated by the project to replace `autoconf` however this transition will take quite some time to complete. 

The Language/Library Levels indicate the minimum and maximum versions that are used in the [continuous integration environments](build/docker/README.md) (Appveyor, Travis) for Apache Thrift.  Note that while a language may contain support for protocols, transports, and servers, the extent to which each is tested as part of the overall build process varies.  The definitive integration test for the project is called the "cross" test which executes a test matrix with clients and servers communicating across languages.

<table style="font-size: 65%; padding: 1px;">
<thead>
<tr>
<th rowspan=2>Language</th>
<th colspan=2 align=center>Build Systems</th>
<th colspan=2 align=center>Lang/Lib Levels</th>
<th colspan=6 align=center>Low-Level Transports</th>
<th colspan=3 align=center>Transport Wrappers</th>
<th colspan=4 align=center>Protocols</th>
<th colspan=5 align=center>Servers</th>
<th rowspan=2>Open Issues</th>
</tr>
<tr>
<!-- Build Systems ---------><th>autoconf</th><th>cmake</th>
<!-- Lang/Lib Levels -------><th>Min</th><th>Max</th>
<!-- Low-Level Transports --><th><a href="https://en.wikipedia.org/wiki/Unix_domain_socket">Domain</a></th><th>&nbsp;File&nbsp;</th><th>Memory</th><th>&nbsp;Pipe&nbsp;</th><th>Socket</th><th>&nbsp;TLS&nbsp;</th>
<!-- Transport Wrappers ----><th>Framed</th><th>&nbsp;http&nbsp;</th><th>&nbsp;zlib&nbsp;</th>
<!-- Protocols -------------><th><a href="doc/specs/thrift-binary-protocol.md">Binary</a></th><th><a href="doc/specs/thrift-compact-protocol.md">Compact</a></th><th>&nbsp;JSON&nbsp;</th><th>Multiplex</th>
<!-- Servers ---------------><th>Forking</th><th>Nonblocking</th><th>Simple</th><th>Threaded</th><th>ThreadPool</th>
</tr>
</thead>
<tbody>
<tr align=center>
<td align=left><a href="lib/as3/README.md">ActionScript</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Language Levels -------><td colspan=2>ActionScript 3</td>
<!-- Low-Level Transports --><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Servers ---------------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12313722">ActionScript</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/c_glib/README.md">C (glib)</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Language Levels -------><td>2.40.2</td><td>2.54.0</td>
<!-- Low-Level Transports --><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Servers ---------------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12313854">C (glib)</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/cpp/README.md">C++</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Language Levels -------><td colspan=2>C++98, gcc </td>
<!-- Low-Level Transports --><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Servers ---------------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12312313">C++</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/csharp/README.md">C#</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Language Levels -------><td>.NET&nbsp;3.5 / mono&nbsp;3.2.8.0</td><td>.NET&nbsp;4.6.1 / mono&nbsp;4.6.2.7</td>
<!-- Low-Level Transports --><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Servers ---------------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12312362">C# (.NET)</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/cocoa/README.md">Cocoa</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Language Levels -------><td colspan=2>unknown</td>
<!-- Low-Level Transports --><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Servers ---------------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12312398">Cocoa</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/d/README.md">D</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Language Levels -------><td>2.070.2</td><td>2.076.0</td>
<!-- Low-Level Transports --><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Servers ---------------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12317904">D</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/dart/README.md">Dart</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Language Levels -------><td>1.20.1</td><td>1.24.2</td>
<!-- Low-Level Transports --><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Servers ---------------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12328006">Dart</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/delphi/README.md">Delphi</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Language Levels -------><td>2010</td><td>unknown</td>
<!-- Low-Level Transports --><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Servers ---------------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12316601">Delphi</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/netcore/README.md">.NET Core</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Language Levels -------><td>2.0.0</td><td>2.0.3</td>
<!-- Low-Level Transports --><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Servers ---------------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12331176">.NET Core</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/erl/README.md">Erlang</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Language Levels -------><td>R16B03</td><td>20.0.4</td>
<!-- Low-Level Transports --><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Servers ---------------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12312390">Erlang</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/go/README.md">Go</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Language Levels -------><td>1.2.1</td><td>1.8.3</td>
<!-- Low-Level Transports --><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Servers ---------------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12314307">Go</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/hs/README.md">Haskell</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Language Levels -------><td>7.6.3</td><td>8.0.2</td>
<!-- Low-Level Transports --><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Servers ---------------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12312704">Haskell</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/haxe/README.md">Haxe</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Language Levels -------><td colspan=2>3.2.1</td>
<!-- Low-Level Transports --><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Servers ---------------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12324347">Haxe</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/java/README.md">Java (SE)</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Language Levels -------><td>1.7.0_151</td><td>1.8.0_144</td>
<!-- Low-Level Transports --><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Servers ---------------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12312314">Java SE</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/javame/README.md">Java (ME)</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Language Levels -------><td colspan=2>unknown</td>
<!-- Low-Level Transports --><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Servers ---------------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12313759">Java ME</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/js/README.md">Javascript</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Language Levels -------><td colspan=2>unknown</td>
<!-- Low-Level Transports --><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Protocols -------------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Servers ---------------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12313418">Javascript</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/lua/README.md">Lua</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Language Levels -------><td>5.1.5</td><td>5.2.4</td>
<!-- Low-Level Transports --><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Servers ---------------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12322659">Lua</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/nodejs/README.md">node.js</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Language Levels -------><td>4.2.6</td><td>8.9.1</td>
<!-- Low-Level Transports --><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Servers ---------------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12314320">node.js</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/ocaml/README.md">OCaml</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Language Levels -------><td>4.02.3</td><td>4.04.0</td>
<!-- Low-Level Transports --><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Servers ---------------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12313660">OCaml</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/perl/README.md">Perl</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Language Levels -------><td>5.18.2</td><td>5.26.0</td>
<!-- Low-Level Transports --><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Servers ---------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12312312">Perl</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/php/README.md">PHP</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Language Levels -------><td>5.5.9</td><td>7.1.8</td>
<!-- Low-Level Transports --><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Servers ---------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12312431">PHP</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/py/README.md">Python</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Language Levels -------><td>2.7.6, 3.4.3</td><td>2.7.14, 3.6.3</td>
<!-- Low-Level Transports --><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Servers ---------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12312315">Python</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/rb/README.md">Ruby</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Language Levels -------><td>1.9.3p484</td><td>2.3.3p222</td>
<!-- Low-Level Transports --><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Servers ---------------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12312316">Ruby</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/rs/README.md">Rust</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Language Levels -------><td>1.15.1</td><td>1.18.0</td>
<!-- Low-Level Transports --><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td>
<!-- Servers ---------------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12331420">Rust</a></td>
</tr>
<tr align=center>
<td align=left><a href="lib/st/README.md">Smalltalk</a></td>
<!-- Build Systems ---------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Language Levels -------><td colspan=2>unknown</td>
<!-- Low-Level Transports --><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Transport Wrappers ----><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Protocols -------------><td><img src="doc/images/cgrn.png" alt="Yes"/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<!-- Servers ---------------><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td><td><img src="doc/images/cred.png" alt=""/></td>
<td align=left><a href="https://issues.apache.org/jira/browse/THRIFT/component/12313861">Smalltalk</a></td>
</tr>
</tbody>
<tfoot>
<tr>
<th rowspan=2>Language</th>
<!-- Build Systems ---------><th>autoconf</th><th>cmake</th>
<!-- Lang/Lib Levels -------><th>Min</th><th>Max</th>
<!-- Low-Level Transports --><th><a href="https://en.wikipedia.org/wiki/Unix_domain_socket">Domain</a></th></th><th>&nbsp;File&nbsp;</th><th>Memory</th><th>&nbsp;Pipe&nbsp;</th><th>Socket</th><th>&nbsp;TLS&nbsp;</th>
<!-- Transport Wrappers ----><th>Framed</th><th>&nbsp;http&nbsp;</th><th>&nbsp;zlib&nbsp;</th>
<!-- Protocols -------------><th><a href="doc/specs/thrift-binary-protocol.md">Binary</a></th><th><a href="doc/specs/thrift-compact-protocol.md">Compact</a></th><th>&nbsp;JSON&nbsp;</th><th>Multiplex</th>
<!-- Servers ---------------><th>Forking</th><th>Nonblocking</th><th>Simple</th><th>Threaded</th><th>ThreadPool</th>
<th rowspan=2>Open Issues</th>
</tr>
<tr>
<th colspan=2 align=center>Build Systems</th>
<th colspan=2 align=center>Lang/Lib Levels</th>
<th colspan=6 align=center>Low-Level Transports</th>
<th colspan=3 align=center>Transport Wrappers</th>
<th colspan=4 align=center>Protocols</th>
<th colspan=5 align=center>Servers</th>
</tr>
</tfoot>
</table>
