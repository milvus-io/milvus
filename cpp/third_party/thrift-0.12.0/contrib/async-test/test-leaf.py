#!/usr/bin/env python
import sys
import time
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.server import THttpServer
from aggr import Aggr


class AggrHandler(Aggr.Iface):
    def __init__(self):
        self.values = []

    def addValue(self, value):
        self.values.append(value)

    def getValues(self, ):
        time.sleep(1)
        return self.values

processor = Aggr.Processor(AggrHandler())
pfactory = TBinaryProtocol.TBinaryProtocolFactory()
THttpServer.THttpServer(processor, ('', int(sys.argv[1])), pfactory).serve()
