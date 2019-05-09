# Building of samples for different platforms 

# Reused components 
- NET Core Standard 1.6 (SDK 1.0.0-preview2-003121)
- NET Core App 1.1

# How to build
- Download and install .NET Core SDK for your platform https://www.microsoft.com/net/core#windowsvs2015 (archive for SDK 1.0.0-preview2-003121 located by: https://github.com/dotnet/core/blob/master/release-notes/download-archive.md)
- Ensure that you have thrift.exe which supports netcore lib and it added to PATH 
- Go to current folder 
- Run **build.sh** or **build.cmd** from the root of cloned repository
- Check tests in **src/Tests** folder
- Continue with /tutorials/netcore 

# How to run 

Notes: dotnet run supports passing arguments to app after -- symbols (https://docs.microsoft.com/en-us/dotnet/articles/core/tools/dotnet-run) - example: **dotnet run -- -h** will show help for app

- build 
- go to folder (Client/Server) 
- run with specifying of correct parameters **dotnet run -t:tcp -p:multiplexed**, **dotnet run -help** (later, after migration to csproj and latest SDK will be possibility to use more usable form **dotnet run -- arguments**)

#Notes
- Migration to .NET Standard 2.0 planned for later (Q1 2017) according to  https://blogs.msdn.microsoft.com/dotnet/2016/09/26/introducing-net-standard/
- Possible adding additional platforms after stabilization of .NET Core (runtimes, platforms (Red Hat Linux, OpenSuse, etc.) 

#Known issues
- In trace logging mode you can see some not important internal exceptions
- Ubuntu 16.10 still not supported fully 
- There is some problems with .NET Core CLI and usage specific -r|--runtime for building and publishing projects with different target frameworks (netstandard1.6 and netcoreapp1.1) 

# Running of samples 
Please install Thrift C# .NET Core library or copy sources and build them to correcly build and run samples 

# NetCore Server

Usage: 

    Server.exe -h
        will diplay help information 

    Server.exe -t:<transport> -p:<protocol> 
        will run server with specified arguments (tcp transport and binary protocol by default)

Options:

    -t (transport): 
        tcp - (default) tcp transport will be used (host - ""localhost"", port - 9090)
        tcpbuffered - tcp buffered transport will be used (host - ""localhost"", port - 9090)
        namedpipe - namedpipe transport will be used (pipe address - "".test"")
        http - http transport will be used (http address - ""localhost:9090"")
        tcptls - tcp transport with tls will be used (host - ""localhost"", port - 9090)
        framed - tcp framed transport will be used (host - ""localhost"", port - 9090)

    -p (protocol): 
        binary - (default) binary protocol will be used
        compact - compact protocol will be used
        json - json protocol will be used
		
Sample:

    Server.exe -t:tcp

**Remarks**:

    For TcpTls mode certificate's file ThriftTest.pfx should be in directory with binaries in case of command line usage (or at project level in case of debugging from IDE).
    Password for certificate - "ThriftTest".



# NetCore Client

Usage: 

    Client.exe -h
        will diplay help information 

    Client.exe -t:<transport> -p:<protocol> -mc:<numClients>
        will run client with specified arguments (tcp transport and binary protocol by default)

Options:

    -t (transport): 
        tcp - (default) tcp transport will be used (host - ""localhost"", port - 9090)
        tcpbuffered - buffered transport over tcp will be used (host - ""localhost"", port - 9090)
        namedpipe - namedpipe transport will be used (pipe address - "".test"")
        http - http transport will be used (address - ""http://localhost:9090"")        
        tcptls - tcp tls transport will be used (host - ""localhost"", port - 9090)
        framed - tcp framed transport will be used (host - ""localhost"", port - 9090)

    -p (protocol): 
        binary - (default) binary protocol will be used
        compact - compact protocol will be used
        json - json protocol will be used
        
    -mc (multiple clients):
        <numClients> - number of multiple clients to connect to server (max 100, default 1)

Sample:

    Client.exe -t:tcp -p:binary -mc:10

Remarks:

    For TcpTls mode certificate's file ThriftTest.pfx should be in directory 
	with binaries in case of command line usage (or at project level in case of debugging from IDE).
    Password for certificate - "ThriftTest".

# How to test communication between NetCore and Python

* Generate code with the latest **thrift.exe** util
* Ensure that **thrift.exe** util generated folder **gen-py** with generated code for Python
* Create **client.py** and **server.py** from the code examples below and save them to the folder with previosly generated folder **gen-py**
* Run netcore samples (client and server) and python samples (client and server)

Remarks:

Samples of client and server code below use correct methods (operations) 
and fields (properties) according to generated contracts from *.thrift files

At Windows 10 add record **127.0.0.1 testserver** to **C:\Windows\System32\drivers\etc\hosts** file
for correct work of python server


**Python Client:**
	
```python
import sys
import glob
sys.path.append('gen-py')

from tutorial import Calculator
from tutorial.ttypes import InvalidOperation, Operation, Work

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


def main():
    # Make socket
    transport = TSocket.TSocket('127.0.0.1', 9090)

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = Calculator.Client(protocol)

    # Connect!
    transport.open()

    client.Ping()
    print('ping()')

    sum = client.Add(1, 1)
    print(('1+1=%d' % (sum)))

    work = Work()

    work.Op = Operation.Divide
    work.Num1 = 1
    work.Num2 = 0

    try:
        quotient = client.Calculate(1, work)
        print('Whoa? You know how to divide by zero?')
        print('FYI the answer is %d' % quotient)
    except InvalidOperation as e:
        print(('InvalidOperation: %r' % e))

    work.Op = Operation.Substract
    work.Num1 = 15
    work.Num2 = 10

    diff = client.Calculate(1, work)
    print(('15-10=%d' % (diff)))

    log = client.GetStruct(1)
    print(('Check log: %s' % (log.Value)))

    client.Zip()
    print('zip()')

    # Close!
    transport.close()

if __name__ == '__main__':
  try:
    main()
  except Thrift.TException as tx:
    print('%s' % tx.message)
```


**Python Server:**


```python
import glob
import sys
sys.path.append('gen-py')

from tutorial import Calculator
from tutorial.ttypes import InvalidOperation, Operation

from shared.ttypes import SharedStruct

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer


class CalculatorHandler:
    def __init__(self):
        self.log = {}

    def Ping(self):
        print('ping()')

    def Add(self, n1, n2):
        print('add(%d,%d)' % (n1, n2))
        return n1 + n2

    def Calculate(self, logid, work):
        print('calculate(%d, %r)' % (logid, work))

        if work.Op == Operation.Add:
            val = work.Num1 + work.Num2
        elif work.Op == Operation.Substract:
            val = work.Num1 - work.Num2
        elif work.Op == Operation.Multiply:
            val = work.Num1 * work.Num2
        elif work.Op == Operation.Divide:
            if work.Num2 == 0:
                x = InvalidOperation()
                x.WhatOp = work.Op
                x.Why = 'Cannot divide by 0'
                raise x
            val = work.Num1 / work.Num2
        else:
            x = InvalidOperation()
            x.WhatOp = work.Op
            x.Why = 'Invalid operation'
            raise x

        log = SharedStruct()
        log.Key = logid
        log.Value = '%d' % (val)
        self.log[logid] = log

        return val

    def GetStruct(self, key):
        print('getStruct(%d)' % (key))
        return self.log[key]

    def Zip(self):
        print('zip()')

if __name__ == '__main__':
    handler = CalculatorHandler()
    processor = Calculator.Processor(handler)
    transport = TSocket.TServerSocket(host="testserver", port=9090)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    print('Starting the server...')
    server.serve()
    print('done.')

    # You could do one of these for a multithreaded server
    # server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    # server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)
```
