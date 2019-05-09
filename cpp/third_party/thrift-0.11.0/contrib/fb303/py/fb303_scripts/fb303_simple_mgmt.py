#!/usr/bin/env python 

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

from __future__ import print_function
import sys
import os
from optparse import OptionParser

from thrift.Thrift import *

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from fb303 import *
from fb303.ttypes import *


def service_ctrl(
        command,
        port,
        trans_factory=None,
        prot_factory=None):
    """
    service_ctrl is a generic function to execute standard fb303 functions

    @param command: one of stop, start, reload, status, counters, name, alive
    @param port: service's port
    @param trans_factory: TTransportFactory to use for obtaining a TTransport. Default is
                          TBufferedTransportFactory
    @param prot_factory: TProtocolFactory to use for obtaining a TProtocol. Default is
                         TBinaryProtocolFactory
    """

    if command in ["status"]:
        try:
            status = fb303_wrapper('status', port, trans_factory, prot_factory)
            status_details = fb303_wrapper('get_status_details', port, trans_factory, prot_factory)

            msg = fb_status_string(status)
            if (len(status_details)):
                msg += " - %s" % status_details
            print(msg)
            return 2 if status == fb_status.ALIVE else 3
        except:
            print("Failed to get status")
            return 3

    # scalar commands
    if command in ["version", "alive", "name"]:
        try:
            result = fb303_wrapper(command, port, trans_factory, prot_factory)
            print(result)
            return 0
        except:
            print("failed to get ", command)
            return 3

    # counters
    if command in ["counters"]:
        try:
            counters = fb303_wrapper('counters', port, trans_factory, prot_factory)
            for counter in counters:
                print("%s: %d" % (counter.encode('utf-8'), counters[counter]))
            return 0
        except:
            print("failed to get counters")
            return 3

    # Only root should be able to run the following commands
    if os.getuid() == 0:
        # async commands
        if command in ["stop", "reload"]:
            try:
                fb303_wrapper(command, port, trans_factory, prot_factory)
                return 0
            except:
                print("failed to tell the service to ", command)
                return 3
    else:
        if command in ["stop", "reload"]:
            print("root privileges are required to stop or reload the service.")
            return 4

    print("The following commands are available:")
    for command in ["counters", "name", "version", "alive", "status"]:
        print("\t%s" % command)
    print("The following commands are available for users with root privileges:")
    for command in ["stop", "reload"]:
        print("\t%s" % command)

    return 0


def fb303_wrapper(command, port, trans_factory=None, prot_factory=None):
    sock = TSocket.TSocket('localhost', port)

    # use input transport factory if provided
    if (trans_factory is None):
        trans = TTransport.TBufferedTransport(sock)
    else:
        trans = trans_factory.getTransport(sock)

    # use input protocol factory if provided
    if (prot_factory is None):
        prot = TBinaryProtocol.TBinaryProtocol(trans)
    else:
        prot = prot_factory.getProtocol(trans)

    # initialize client and open transport
    fb303_client = FacebookService.Client(prot, prot)
    trans.open()

    if (command == 'reload'):
        fb303_client.reinitialize()

    elif (command == 'stop'):
        fb303_client.shutdown()

    elif (command == 'status'):
        return fb303_client.getStatus()

    elif (command == 'version'):
        return fb303_client.getVersion()

    elif (command == 'get_status_details'):
        return fb303_client.getStatusDetails()

    elif (command == 'counters'):
        return fb303_client.getCounters()

    elif (command == 'name'):
        return fb303_client.getName()

    elif (command == 'alive'):
        return fb303_client.aliveSince()

    trans.close()


def fb_status_string(status_enum):
    if (status_enum == fb_status.DEAD):
        return "DEAD"
    if (status_enum == fb_status.STARTING):
        return "STARTING"
    if (status_enum == fb_status.ALIVE):
        return "ALIVE"
    if (status_enum == fb_status.STOPPING):
        return "STOPPING"
    if (status_enum == fb_status.STOPPED):
        return "STOPPED"
    if (status_enum == fb_status.WARNING):
        return "WARNING"


def main():

    # parse command line options
    parser = OptionParser()
    commands = ["stop", "counters", "status", "reload", "version", "name", "alive"]

    parser.add_option("-c", "--command", dest="command", help="execute this API",
                      choices=commands, default="status")
    parser.add_option("-p", "--port", dest="port", help="the service's port",
                      default=9082)

    (options, args) = parser.parse_args()
    status = service_ctrl(options.command, options.port)
    sys.exit(status)


if __name__ == '__main__':
    main()
