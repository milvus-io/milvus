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

import time
import FacebookService
import thrift.reflection.limited
from ttypes import fb_status


class FacebookBase(FacebookService.Iface):

    def __init__(self, name):
        self.name = name
        self.alive = int(time.time())
        self.counters = {}

    def getName(self, ):
        return self.name

    def getVersion(self, ):
        return ''

    def getStatus(self, ):
        return fb_status.ALIVE

    def getCounters(self):
        return self.counters

    def resetCounter(self, key):
        self.counters[key] = 0

    def getCounter(self, key):
        if self.counters.has_key(key):
            return self.counters[key]
        return 0

    def incrementCounter(self, key):
        self.counters[key] = self.getCounter(key) + 1

    def setOption(self, key, value):
        pass

    def getOption(self, key):
        return ""

    def getOptions(self):
        return {}

    def getOptions(self):
        return {}

    def aliveSince(self):
        return self.alive

    def getCpuProfile(self, duration):
        return ""

    def getLimitedReflection(self):
        return thrift.reflection.limited.Service()

    def reinitialize(self):
        pass

    def shutdown(self):
        pass
