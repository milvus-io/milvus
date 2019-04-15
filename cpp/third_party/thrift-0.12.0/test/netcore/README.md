# Apache Thrift net-core-lib tests

Tests for Thrift client library ported to Microsoft .Net Core 

# Content
- ThriftTest - tests for Thrift library 

# Reused components 
- NET Core Standard 1.6 (SDK 2.0.0)

# How to build on Windows
- Get Thrift IDL compiler executable, add to some folder and add path to this folder into PATH variable
- Open ThriftTest.sln in Visual Studio and build
or 
- Build with scripts

# How to build on Unix
- Ensure you have .NET Core 2.0.0 SDK installed or use the Ubuntu Xenial docker image
- Follow common build practice for Thrift: bootstrap, configure, and make precross

