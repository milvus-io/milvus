# Apache Thrift netcore

Thrift client library ported to Microsoft .Net Core 

# Content
- Tests/Thrift.PublicInterfaces.Compile.Tests - project for checking public interfaces during adding changes to Thrift library
- Thrift - Thrift library 

# Reused components 
- .NET Standard 1.6 (SDK 2.0.0)

# How to build on Windows
- Get Thrift IDL compiler executable, add to some folder and add path to this folder into PATH variable
- Open the Thrift.sln project with Visual Studio and build.
or 
- Build with scripts

# How to build on Unix
- Ensure you have .NET Core 2.0.0 SDK installed or use the Ubuntu Xenial docker image
- Follow common build practice for Thrift: bootstrap, configure, and make

# Known issues
- In trace logging mode you can see some not important internal exceptions

